import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, sum as _sum, countDistinct, count, when, round
)

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def main():
    try:
        # Initialize Spark session
        spark = SparkSession.builder.appName("KPIsWithLogging").getOrCreate()
        logger.info("Spark session started.")
    except Exception as e:
        logger.exception("Error starting Spark session: %s", e)
        return

    try:
        # Read order_items and orders from directories (concatenation using wildcards)
        order_items_df = spark.read.option("header", True) \
            .csv("../Data/order_items/*.csv", inferSchema=True)
        orders_df = spark.read.option("header", True) \
            .csv("../Data/orders/*.csv", inferSchema=True)
        products_df = spark.read.option("header", True) \
            .csv("../Data/products.csv", inferSchema=True)
        logger.info("CSV files loaded successfully.")
    except Exception as e:
        logger.exception("Error loading CSV files: %s", e)
        return

    try:
        # Preprocess orders and order_items
        orders_df = orders_df.withColumn("order_date", to_date(col("created_at")))
        order_items_df = order_items_df.withColumn("sale_price", col("sale_price").cast("float"))
        logger.info("Preprocessing completed.")
    except Exception as e:
        logger.exception("Error during preprocessing: %s", e)
        return

    try:
        # Join data for Category-Level KPIs
        joined_category_df = (
            order_items_df
            .join(orders_df.select("order_id", "order_date"), on="order_id")
            .join(products_df.select(col("id").alias("product_id"), "category"), on="product_id")
            .withColumn("is_returned", when(col("status") == "returned", 1).otherwise(0))
        )
        logger.info("Data joined for category-level KPIs.")
    except Exception as e:
        logger.exception("Error joining data for category-level KPIs: %s", e)
        return

    try:
        # Compute Category-Level KPIs
        category_kpis_df = (
            joined_category_df.groupBy("category", "order_date")
            .agg(
                round(_sum("sale_price"), 2).alias("daily_revenue"),
                round(_sum("sale_price") / countDistinct("order_id"), 2).alias("avg_order_value"),
                round(_sum("is_returned") / countDistinct("order_id"), 4).alias("avg_return_rate")
            )
            .cache()
        )
        logger.info("Category-level KPIs computed successfully.")
    except Exception as e:
        logger.exception("Error computing category-level KPIs: %s", e)
        return

    try:
        # Join data for Order-Level KPIs
        joined_order_df = (
            order_items_df
            .join(orders_df.select("order_id", "user_id", "status", "order_date"), on="order_id")
            .withColumn("is_returned", when(col("status") == "returned", 1).otherwise(0))
        )
        logger.info("Data joined for order-level KPIs.")
    except Exception as e:
        logger.exception("Error joining data for order-level KPIs: %s", e)
        return

    try:
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
            .cache()
        )
        logger.info("Order-level KPIs computed successfully.")
    except Exception as e:
        logger.exception("Error computing order-level KPIs: %s", e)
        return

    try:
        # Optionally display results
        logger.info("Displaying Category-Level KPIs:")
        category_kpis_df.show(truncate=False)
        logger.info("Displaying Order-Level KPIs:")
        order_kpis_df.show(truncate=False)
    except Exception as e:
        logger.exception("Error displaying results: %s", e)
    
    # Stop Spark session
    spark.stop()
    logger.info("Spark session stopped.")

if __name__ == "__main__":
    main()
