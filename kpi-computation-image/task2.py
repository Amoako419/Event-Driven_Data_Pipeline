import logging
import os # To read environment variables
import json
from decimal import Decimal
import boto3
from pyspark.sql import SparkSession
from dotenv import load_dotenv
import datetime
from pyspark.sql.functions import (
    col, to_date, sum as _sum, countDistinct, count, when, round
)
load_dotenv()
ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")
REGION = os.getenv("REGION") 
session = boto3.Session(
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name=REGION
    )

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


def write_spark_df_to_dynamodb(spark_df, table_name):
    dynamodb = session.resource('dynamodb')
    table = dynamodb.Table(table_name)
    logger.info(f"Starting write to DynamoDB table: {table_name} in region ")
    
    def process_partition(iterator):
        # Create boto3 resource per partition for efficiency
        dynamodb_partition = session.resource('dynamodb')
        table_partition = dynamodb_partition.Table(table_name)
        
        # Custom JSON encoder to handle date objects
        class DateEncoder(json.JSONEncoder):
            def default(self, obj):
                if isinstance(obj, (datetime.date, datetime.datetime)):
                    return obj.isoformat()
                return super().default(obj)
        
        with table_partition.batch_writer() as batch:
            count = 0
            for row in iterator:
                try:
                    item_dict_raw = row.asDict(recursive=True)
                    # Use the custom encoder to handle date objects
                    item_json = json.dumps(item_dict_raw, cls=DateEncoder)
                    item_dict = json.loads(item_json, parse_float=Decimal)
                    
                    if item_dict:  # Ensure dict is not empty
                        batch.put_item(Item=item_dict)
                        count += 1
                    else:
                        logger.warning("Skipping empty item_dict conversion.")

                except Exception as part_err:
                    logger.error(f"Error processing row for {table_name}: {row}. Error: {part_err}", exc_info=True)

            logger.info(f"Wrote {count} items from partition to {table_name}")

    try:
        spark_df.rdd.foreachPartition(process_partition)
        logger.info(f"Finished writing to DynamoDB table: {table_name}")
    except Exception as write_err:
        logger.error(f"Error during foreachPartition write to {table_name}", exc_info=True)
        raise write_err  # Re-raise the exception to signal failure

# --- Main Function ---
def main():
    spark = None # Initialize spark variable
    try:
        # Initialize Spark session with S3A configuration for IAM role auth
        spark = SparkSession.builder \
            .appName("DataCleaningECS") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) \
            .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) \
            .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
            .config("spark.hadoop.fs.s3a.region", REGION) \
            .getOrCreate()
        logger.info("Spark session started with S3A IAM role configuration.")
    except Exception as e:
        logger.exception("Error starting Spark session: %s", e)
        return

    try:
        OUTPUT_BUCKET = os.getenv('OUTPUT_BUCKET')
        OUTPUT_PREFIX = os.getenv('OUTPUT_PREFIX')
        # --- Read input data from Cleaned S3 Bucket ---
        orders_path = f"s3a://{OUTPUT_BUCKET}/{OUTPUT_PREFIX}clean_orders/" 
        order_items_path = f"s3a://{OUTPUT_BUCKET}/{OUTPUT_PREFIX}clean_order_items/" 
        products_path = f"s3a://{OUTPUT_BUCKET}/{OUTPUT_PREFIX}clean_products/" 
        
        logger.info(f"Reading orders data from S3 directory: {orders_path}")
        orders_df = spark.read.parquet(orders_path).cache()

        logger.info(f"Reading order items data from S3 directory: {order_items_path}")
        order_items_df = spark.read.parquet(order_items_path).cache()

        logger.info(f"Reading products data from S3 directory: {products_path}")
        products_df = spark.read.parquet(products_path).cache()

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
            ).cache()
        )
        logger.info("Category-level KPIs computed successfully.")
        category_kpis_df.show(5, truncate=False) # Display sample

    except Exception as e:
        logger.exception("Error computing category-level KPIs: %s", e)
    try:
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
            ).cache()
        )
        logger.info("Order-level KPIs computed successfully.")
        order_kpis_df.show(5, truncate=False) 

    except Exception as e:
        logger.exception("Error computing order-level KPIs: %s", e)
    

    # --- Write KPI Results to DynamoDB ---
    try:
        if category_kpis_df is not None:
            write_spark_df_to_dynamodb(category_kpis_df, table_name = "category_kpi_table")
        else:
            logger.warning("Category KPIs DataFrame is None, skipping DynamoDB write.")

        if order_kpis_df is not None:
            write_spark_df_to_dynamodb(order_kpis_df, table_name = "order_kpi_table")
        else:
            logger.warning("Order KPIs DataFrame is None, skipping DynamoDB write.")

    except Exception as e:
        logger.exception("Error writing KPIs to DynamoDB: %s", e)

    # Clear DataFrames from cache to free up memory
    if order_kpis_df is not None:
        order_kpis_df.unpersist()
    if category_kpis_df is not None:    
        category_kpis_df.unpersist()

    # Stop Spark session
    if spark:
        spark.stop()
        logger.info("Spark session stopped.")

if __name__ == "__main__":
    main()