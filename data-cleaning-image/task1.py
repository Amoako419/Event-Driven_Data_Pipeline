import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, lit
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# Initialize S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv("ACCESS_KEY"),
    aws_secret_access_key=os.getenv("SECRET_KEY"),
    region_name=os.getenv("REGION")
)


def validate_output_files(bucket_name, prefix):
    """
    Validate that output files exist in the specified S3 prefix.
    """
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if 'Contents' in response:
            logger.info(f"Output files validated successfully in s3://{bucket_name}/{prefix}")
            return True
        else:
            logger.warning(f"No output files found in s3://{bucket_name}/{prefix}")
            return False
    except NoCredentialsError:
        logger.error("AWS credentials not found.")
        return False
    except ClientError as e:
        logger.error(f"An error occurred while validating output files: {e}")
        return False

def clean_orders(orders_df):
    """
    Clean and validate orders data.
    Mandatory fields: order_id, user_id, created_at, status.
    Impute missing values for mandatory fields.
    """
    logger.info("Starting cleaning for orders data.")

    # Define imputation values for mandatory fields
    impute_values = {
        "order_id": "UNKNOWN_ORDER",
        "user_id": "UNKNOWN_USER",
        "created_at": "1970-01-01",  # Default date
        "status": "UNKNOWN_STATUS"
    }

    # Impute missing values for mandatory fields
    orders_df = orders_df.fillna(impute_values)

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
    Impute missing values for mandatory fields.
    """
    logger.info("Starting cleaning for order_items data.")

    # Define imputation values for mandatory fields
    impute_values = {
        "id": "UNKNOWN_ID",
        "order_id": "UNKNOWN_ORDER",
        "product_id": "UNKNOWN_PRODUCT",
        "sale_price": 0.0  # Default price
    }

    # Impute missing values for mandatory fields
    order_items_df = order_items_df.fillna(impute_values)

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
    Impute missing values for mandatory fields.
    """
    logger.info("Starting cleaning for products data.")

    # Define imputation values for mandatory fields
    impute_values = {
        "id": "UNKNOWN_ID",
        "sku": "UNKNOWN_SKU",
        "cost": 0.0,  # Default cost
        "category": "UNKNOWN_CATEGORY",
        "retail_price": 0.0  # Default price
    }

    # Impute missing values for mandatory fields
    products_df = products_df.fillna(impute_values)

    # Convert cost and retail_price to float
    products_df = products_df.withColumn("cost", col("cost").cast("float"))
    products_df = products_df.withColumn("retail_price", col("retail_price").cast("float"))
    products_df = products_df.filter(col("cost").isNotNull() & col("retail_price").isNotNull())

    # Drop duplicates
    products_df = products_df.dropDuplicates(["id"])

    logger.info(f"Products cleaned. Remaining records: {products_df.count()}")
    return products_df



def main():
    spark = None  # Initialize spark variable
    try:
        # Initialize Spark session with S3A configuration for IAM role auth
        spark = SparkSession.builder \
            .appName("DataCleaningECS") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("ACCESS_KEY")) \
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("SECRET_KEY")) \
            .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
            .config("spark.hadoop.fs.s3a.region", os.getenv("REGION")) \
            .getOrCreate()
        logger.info("Spark session started with S3A IAM role configuration.")
    except Exception as e:
        logger.exception("Error starting Spark session: %s", e)
        return

    # Define S3 bucket and prefixes
    INPUT_BUCKET = os.getenv('INPUT_BUCKET')
    INPUT_PREFIX_ORDERS = os.getenv('INPUT_PREFIX_ORDERS')
    INPUT_PREFIX_ORDER_ITEMS = os.getenv('INPUT_PREFIX_ORDER_ITEMS')
    INPUT_PREFIX_PRODUCTS = os.getenv('INPUT_PREFIX_PRODUCTS')
    OUTPUT_BUCKET = os.getenv('OUTPUT_BUCKET')
    OUTPUT_PREFIX = os.getenv('OUTPUT_PREFIX')


    try:
        # Read data files
        orders_df = spark.read.option("header", True).csv(f"s3a://{INPUT_BUCKET}/{INPUT_PREFIX_ORDERS}*.csv", inferSchema=True)
        order_items_df = spark.read.option("header", True).csv(f"s3a://{INPUT_BUCKET}/{INPUT_PREFIX_ORDER_ITEMS}*.csv", inferSchema=True)
        products_df = spark.read.option("header", True).csv(f"s3a://{INPUT_BUCKET}/{INPUT_PREFIX_PRODUCTS}", inferSchema=True)
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
        # Write cleaned data to S3
        output_orders_path = f"s3a://{OUTPUT_BUCKET}/{OUTPUT_PREFIX}clean_orders"
        output_order_items_path = f"s3a://{OUTPUT_BUCKET}/{OUTPUT_PREFIX}clean_order_items"
        output_products_path = f"s3a://{OUTPUT_BUCKET}/{OUTPUT_PREFIX}clean_products"

        logger.info(f"Writing cleaned orders to: {output_orders_path}")
        orders_clean.write.mode("overwrite").parquet(output_orders_path)

        logger.info(f"Writing cleaned order items to: {output_order_items_path}")
        order_items_clean.write.mode("overwrite").parquet(output_order_items_path)

        logger.info(f"Writing cleaned products to: {output_products_path}")
        products_clean.write.mode("overwrite").parquet(output_products_path)

        # Validate output files using boto3
        validate_output_files(OUTPUT_BUCKET, f"{OUTPUT_PREFIX}clean_orders/")
        validate_output_files(OUTPUT_BUCKET, f"{OUTPUT_PREFIX}clean_order_items/")
        validate_output_files(OUTPUT_BUCKET, f"{OUTPUT_PREFIX}clean_products/")
    except Exception as e:
        logger.exception("Error writing cleaned data: %s", e)

    # Stop Spark session
    spark.stop()
    logger.info("Spark session stopped.")

if __name__ == "__main__":
    main()