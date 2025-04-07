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
        # Initialize Spark session
        spark = SparkSession.builder.appName("DataCleaningAndValidation").getOrCreate()
        logger.info("Spark session started.")
    except Exception as e:
        logger.exception("Error starting Spark session: %s", e)
        return

    try:
        # Read data files (adjust paths as necessary; using wildcards to concatenate files)
        orders_df = spark.read.option("header", True).csv("path/to/orders/*.csv", inferSchema=True)
        order_items_df = spark.read.option("header", True).csv("path/to/order_items/*.csv", inferSchema=True)
        products_df = spark.read.option("header", True).csv("path/to/products.csv", inferSchema=True)
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
        # Write the cleaned data to Parquet files (or another output format)
        orders_clean.write.mode("overwrite").parquet("path/to/output/clean_orders.parquet")
        order_items_clean.write.mode("overwrite").parquet("path/to/output/clean_order_items.parquet")
        products_clean.write.mode("overwrite").parquet("path/to/output/clean_products.parquet")
        logger.info("Cleaned data written successfully.")
    except Exception as e:
        logger.exception("Error writing cleaned data: %s", e)

    # Stop Spark session
    spark.stop()
    logger.info("Spark session stopped.")

if __name__ == "__main__":
    main()
