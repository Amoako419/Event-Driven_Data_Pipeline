import logging
import os # To read environment variables
import json
from decimal import Decimal
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, sum as _sum, countDistinct, count, when, round
)

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# --- DynamoDB Helper Function ---
def write_spark_df_to_dynamodb(spark_df, table_name, region_name):
    """
    Helper function to write Spark DataFrame rows to a DynamoDB table.
    Assumes DataFrame columns match DynamoDB table attributes.
    Converts floats/doubles to Decimals. Adjust primary key/data type handling as needed.
    """
    dynamodb = boto3.resource('dynamodb', region_name=region_name)
    table = dynamodb.Table(table_name)

    logger.info(f"Starting write to DynamoDB table: {table_name} in region {region_name}")

    def process_partition(iterator):
        # Create boto3 resource per partition for efficiency
        dynamodb_partition = boto3.resource('dynamodb', region_name=region_name)
        table_partition = dynamodb_partition.Table(table_name)
        with table_partition.batch_writer() as batch:
            count = 0
            for row in iterator:
                try:
                    # Convert Spark Row to Python dict
                    # Use built-in .asDict() and then handle type conversions
                    item_dict_raw = row.asDict(recursive=True)

                    # Convert floats/doubles to Decimals for DynamoDB compatibility
                    # Use json dumps/loads trick or manual iteration if structure is complex
                    item_dict = json.loads(json.dumps(item_dict_raw), parse_float=Decimal)

                    # ** Important: Add specific data type conversions if needed **
                    # e.g., Ensure date/timestamp columns are strings in ISO format
                    # Ensure primary key(s) are correctly formatted and named

                    if item_dict: # Ensure dict is not empty
                      batch.put_item(Item=item_dict)
                      count += 1
                    else:
                      logger.warning("Skipping empty item_dict conversion.")

                except Exception as part_err:
                    logger.error(f"Error processing row for {table_name}: {row}. Error: {part_err}", exc_info=True)
                    # Decide if you want to skip the row or fail the partition/job

            logger.info(f"Wrote {count} items from partition to {table_name}")

    try:
        spark_df.rdd.foreachPartition(process_partition)
        logger.info(f"Finished writing to DynamoDB table: {table_name}")
    except Exception as write_err:
        logger.error(f"Error during foreachPartition write to {table_name}", exc_info=True)
        raise write_err # Re-raise the exception to signal failure

# --- Main Function ---
def main():
    spark = None # Initialize spark variable
    try:
        # Initialize Spark session with S3A configuration for IAM role auth
        spark = SparkSession.builder \
            .appName("KPIComputationECS") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider") \
            .getOrCreate()
        logger.info("Spark session started with S3A IAM role configuration.")
    except Exception as e:
        logger.exception("Error starting Spark session: %s", e)
        return

    try:
        # --- Read input data from Cleaned S3 Bucket ---
        orders_path = "s3a://ecs-output-bucket/cleaned_folder/output/clean_orders.parquet" 
        order_items_path = "s3a://ecs-output-bucket/cleaned_folder/output/clean_order_items.parquet" 
        products_path = "s3a://ecs-output-bucket/cleaned_folder/output/clean_products.parquet" 

        logger.info(f"Reading orders data from S3 directory: {orders_path}")
        orders_df = spark.read.parquet(orders_path)

        logger.info(f"Reading order items data from S3 directory: {order_items_path}")
        order_items_df = spark.read.parquet(order_items_path)

        logger.info(f"Reading products data from S3 directory: {products_path}")
        products_df = spark.read.parquet(products_path)

        logger.info("Cleaned Parquet files loaded successfully from S3.")

    except Exception as e:
        logger.exception(f"Error loading cleaned Parquet files from S3 : {e}", exc_info=True)
        if spark: spark.stop()
        return

    try:
        # Preprocess orders and order_items
        orders_df = orders_df.withColumn("order_date", to_date(col("created_at")))
        # Cast might still be needed depending on Parquet source types
        order_items_df = order_items_df.withColumn("sale_price", col("sale_price").cast("float"))
        logger.info("Preprocessing completed.")
    except Exception as e:
        logger.exception("Error during preprocessing: %s", e)
        if spark: spark.stop()
        return

    # --- Compute KPIs (Logic remains similar) ---
    category_kpis_df = None
    order_kpis_df = None
    try:
        # Join data for Category-Level KPIs
        joined_category_df = (
            order_items_df
            .join(orders_df.select("order_id", "order_date"), on="order_id")
            .join(products_df.select(col("id").alias("product_id"), "category"), on="product_id")
            .withColumn("is_returned", when(col("status") == "returned", 1).otherwise(0))
         )
        logger.info("Data joined for category-level KPIs.")

        # Compute Category-Level KPIs
        category_kpis_df = (
            joined_category_df.groupBy("category", "order_date")
            .agg(
                round(_sum("sale_price"), 2).alias("daily_revenue"),
                round(_sum("sale_price") / countDistinct("order_id"), 2).alias("avg_order_value"),
                round(_sum("is_returned") / countDistinct("order_id"), 4).alias("avg_return_rate")
            )
            # Define schema explicitly if needed for DynamoDB mapping later
            # .select(col("category").cast("string"), col("order_date").cast("string"), ...)
            .cache()
        )
        logger.info("Category-level KPIs computed successfully.")
        category_kpis_df.show(5, truncate=False) # Display sample

    except Exception as e:
        logger.exception("Error computing category-level KPIs: %s", e)
        # Decide if you want to stop or try computing the other KPI set
        # if spark: spark.stop()
        # return

    try:
        # Join data for Order-Level KPIs (Adjust join keys/columns as needed)
        # This join seems potentially incorrect if user_id/status are only in orders_df
        # Assuming orders_df has user_id, status and created_at (for order_date)
        # Assuming order_items_df has id (item primary key), order_id, sale_price
        joined_order_df = (
             order_items_df.alias("oi")
             .join(orders_df.alias("o"), col("oi.order_id") == col("o.order_id"))
             .select(
                 col("o.order_date"),
                 col("o.order_id"),
                 col("o.user_id"),
                 col("oi.id"), # item id
                 col("oi.sale_price"),
                 when(col("o.status") == "returned", 1).otherwise(0).alias("is_returned")
             )
         )
        logger.info("Data joined for order-level KPIs.")


        # Compute Order-Level KPIs
        order_kpis_df = (
            joined_order_df.groupBy("order_date")
            .agg(
                countDistinct("order_id").alias("total_orders"),
                round(_sum("sale_price"), 2).alias("total_revenue"),
                count("id").alias("total_items_sold"),
                round(_sum("is_returned") / countDistinct("order_id"), 4).alias("return_rate"),
                countDistinct("user_id").alias("unique_customers")
            )
            # Define schema explicitly if needed for DynamoDB mapping later
            # .select(col("order_date").cast("string"), col("total_orders").cast("int"), ...)
            .cache()
        )
        logger.info("Order-level KPIs computed successfully.")
        order_kpis_df.show(5, truncate=False) # Display sample

    except Exception as e:
        logger.exception("Error computing order-level KPIs: %s", e)
        # Decide if you want to stop here
        # if spark: spark.stop()
        # return


    # --- Write KPI Results to DynamoDB ---
    try:
        if category_kpis_df is not None:
            write_spark_df_to_dynamodb(category_kpis_df, category_kpi_table_name = "category_kpi_table", aws_region = "eu-west-1")
        else:
            logger.warning("Category KPIs DataFrame is None, skipping DynamoDB write.")

        if order_kpis_df is not None:
            write_spark_df_to_dynamodb(order_kpis_df, order_kpi_table_name = "order_kpi_table", aws_region = "eu-west-1")
        else:
            logger.warning("Order KPIs DataFrame is None, skipping DynamoDB write.")

    except Exception as e:
        logger.exception("Error writing KPIs to DynamoDB: %s", e)
        # Optional: Decide if failure here should stop the Spark session

    # Stop Spark session
    if spark:
        spark.stop()
        logger.info("Spark session stopped.")

if __name__ == "__main__":
    main()