import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def clean_orders(orders_df):
    """
    Clean and validate orders data.
    Mandatory fields: order_id, user_id, created_at, status.
    Reject rows with null or malformed values.
    """
    logger.info("Starting cleaning for orders data.")

    # Check mandatory fields
    mandatory_fields = ["order_id", "user_id", "created_at", "status"]
    for field in mandatory_fields:
        orders_df = orders_df.filter(col(field).isNotNull())

    # Validate and convert created_at to date
    orders_df = orders_df.withColumn("order_date", to_date(col("created_at")))
    orders_df = orders_df.filter(col("order_date").isNotNull())

    # Drop duplicate orders (if any)
    orders_df = orders_df.dropDuplicates(["order_id"])

    logger.info(f"Orders cleaned. Remaining records: {orders_df.count()}")
    return orders_df

def clean_order_items(order_items_df):
    """
    Clean and validate order_items data.
    Mandatory fields: id, order_id, product_id, sale_price.
    Reject rows with null or malformed values.
    """
    logger.info("Starting cleaning for order_items data.")

    mandatory_fields = ["id", "order_id", "product_id", "sale_price"]
    for field in mandatory_fields:
        order_items_df = order_items_df.filter(col(field).isNotNull())

    # Ensure sale_price is a valid float
    order_items_df = order_items_df.withColumn("sale_price", col("sale_price").cast("float"))
    order_items_df = order_items_df.filter(col("sale_price").isNotNull())

    # Drop duplicate order items if necessary
    order_items_df = order_items_df.dropDuplicates(["id"])

    logger.info(f"Order items cleaned. Remaining records: {order_items_df.count()}")
    return order_items_df

def clean_products(products_df):
    """
    Clean and validate products data.
    Mandatory fields: id, sku, cost, category, retail_price.
    Reject rows with null or malformed values.
    """
    logger.info("Starting cleaning for products data.")

    mandatory_fields = ["id", "sku", "cost", "category", "retail_price"]
    for field in mandatory_fields:
        products_df = products_df.filter(col(field).isNotNull())

    # Convert cost and retail_price to float
    products_df = products_df.withColumn("cost", col("cost").cast("float"))
    products_df = products_df.withColumn("retail_price", col("retail_price").cast("float"))
    products_df = products_df.filter(col("cost").isNotNull() & col("retail_price").isNotNull())

    # Drop duplicates
    products_df = products_df.dropDuplicates(["id"])

    logger.info(f"Products cleaned. Remaining records: {products_df.count()}")
    return products_df

def main():
    try:
        spark = SparkSession.builder \
            .appName("DataCleaningECS") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider") \
            .getOrCreate()

        logger.info("Spark session created with S3A configuration.")
    except Exception as e:
        logger.exception("Error starting Spark session: %s", e)
        return

    try:
        # Read data files (adjust paths as necessary; using wildcards to concatenate files)
        orders_df = spark.read.option("header", True).csv("s3://pipeline-land-bucket-125/land-folder/e-commerce-data/orders/*.csv", inferSchema=True)
        order_items_df = spark.read.option("header", True).csv("s3://pipeline-land-bucket-125/land-folder/e-commerce-data/order_items/*.csv", inferSchema=True)
        products_df = spark.read.option("header", True).csv("s3://pipeline-land-bucket-125/land-folder/e-commerce-data/products.csv", inferSchema=True)
        logger.info("Data files loaded successfully.")
    except Exception as e:
        logger.exception("Error loading CSV files: %s", e)
        spark.stop()
        return

    try:
        # Clean each dataset
        orders_clean = clean_orders(orders_df)
        order_items_clean = clean_order_items(order_items_df)
        products_clean = clean_products(products_df)
    except Exception as e:
        logger.exception("Error during cleaning and validation: %s", e)
        spark.stop()
        return

    try:
        # Make sure the path starts with "cleaned_data/"
        output_orders_path = "s3://ecs-output-bucket/cleaned_folder/output/clean_orders.parquet"
        output_order_items_path = "s3://ecs-output-bucket/cleaned_folder/output/clean_order_items.parquet"
        output_products_path = "s3://ecs-output-bucket/cleaned_folder/output/clean_products.parquet"

        logger.info(f"Writing cleaned orders to: {output_orders_path}")
        orders_clean.write.mode("overwrite").parquet(output_orders_path) 

        logger.info(f"Writing cleaned order items to: {output_order_items_path}")
        order_items_clean.write.mode("overwrite").parquet(output_order_items_path)

        logger.info(f"Writing cleaned products to: {output_products_path}")
        products_clean.write.mode("overwrite").parquet(output_products_path) 
    except Exception as e:
            logger.exception("Error writing cleaned data: %s", e)

    # Stop Spark session
    spark.stop()
    logger.info("Spark session stopped.")

if __name__ == "__main__":
    main()
