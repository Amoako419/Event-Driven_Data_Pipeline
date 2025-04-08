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
        spark = SparkSession.builder.appName("KPIsWithCleanedParquet").getOrCreate() # Updated App Name
        logger.info("Spark session started.")
    except Exception as e:
        logger.exception("Error starting Spark session: %s", e)
        return

    try:
        # --- Read input data from specific cleaned Parquet files ---
        logger.info("Attempting to load cleaned Parquet files from ../Output/ directory...")

        # --- In task2.py ---

        # Paths should point to the 'cleaned_data' directory mapped in docker-compose
        orders_path = "cleaned_data/clean_orders.parquet"
        order_items_path = "cleaned_data/clean_order_items.parquet"
        products_path = "cleaned_data/clean_products.parquet"

        logger.info(f"Reading orders data from directory: {orders_path}") # Updated log message
        orders_df = spark.read.parquet(orders_path) # Keep this line as is (reads the directory)

        logger.info(f"Reading order items data from directory: {order_items_path}")
        order_items_df = spark.read.parquet(order_items_path)

        logger.info(f"Reading products data from directory: {products_path}")
        products_df = spark.read.parquet(products_path)

        logger.info(f"Reading order items data from: {order_items_path}")
        order_items_df = spark.read.parquet(order_items_path)

        logger.info(f"Reading products data from: {products_path}")
        products_df = spark.read.parquet(products_path)

        logger.info("Cleaned Parquet files loaded successfully.")
    except Exception as e:
        logger.exception("Error loading cleaned Parquet files: %s", e)
        spark.stop() # Stop Spark session on error
        return

    try:
        # Preprocess orders and order_items
        # Assuming 'created_at' and 'sale_price' columns exist in the cleaned data
        # Adjust or remove casting if cleaning process already handled data types
        orders_df = orders_df.withColumn("order_date", to_date(col("created_at")))
        order_items_df = order_items_df.withColumn("sale_price", col("sale_price").cast("float"))
        logger.info("Preprocessing completed.")
    except Exception as e:
        logger.exception("Error during preprocessing: %s", e)
        spark.stop()
        return

    try:
        # Join data for Category-Level KPIs
        # Ensure required columns ('order_id', 'product_id', 'id', 'category', 'status')
        # are present in the cleaned Parquet files
        joined_category_df = (
            order_items_df
            .join(orders_df.select("order_id", "order_date"), on="order_id") # Assumes 'order_date' was added in preprocessing
            .join(products_df.select(col("id").alias("product_id"), "category"), on="product_id") # Assumes 'id' in products is the product_id
            .withColumn("is_returned", when(col("status") == "returned", 1).otherwise(0)) # Assumes 'status' is in order_items_df or orders_df after join
        )
        logger.info("Data joined for category-level KPIs.")
    except Exception as e:
        logger.exception("Error joining data for category-level KPIs: %s", e)
        spark.stop()
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
        spark.stop()
        return

    try:
        # Join data for Order-Level KPIs
        # Ensure required columns ('order_id', 'user_id', 'status', 'id' from order_items)
        # are present in the cleaned Parquet files
        joined_order_df = (
            order_items_df
            .join(orders_df.select("order_id", "order_date"), on="order_id") # Assumes 'order_date' was added
            .withColumn("is_returned", when(col("status") == "returned", 1).otherwise(0)) # Assumes 'status' is available after join
        )
        logger.info("Data joined for order-level KPIs.")
    except Exception as e:
        logger.exception("Error joining data for order-level KPIs: %s", e)
        spark.stop()
        return

    try:
        # Compute Order-Level KPIs
        order_kpis_df = (
            joined_order_df.groupBy("order_date")
            .agg(
                countDistinct("order_id").alias("total_orders"),
                round(_sum("sale_price"), 2).alias("total_revenue"),
                count("id").alias("total_items_sold"), # Make sure 'id' here refers to order_items primary key if intended
                round(_sum("is_returned") / countDistinct("order_id"), 4).alias("return_rate"),
                countDistinct("user_id").alias("unique_customers")
            )
            .cache()
        )
        logger.info("Order-level KPIs computed successfully.")
    except Exception as e:
        logger.exception("Error computing order-level KPIs: %s", e)
        spark.stop()
        return

    try:
        # --- Write results to Parquet files ---
        # Using a different output directory to avoid overwriting input, adjust as needed
        output_path_category = "kpi_results/category_kpis_parquet"
        output_path_order = "kpi_results/order_kpis_parquet"

        logger.info(f"Writing Category-Level KPIs to Parquet: {output_path_category}")
        # Ensure the ../KPI_Output directory exists
        category_kpis_df.write.mode("overwrite").parquet(output_path_category)
        logger.info("Category-Level KPIs written successfully.")

        logger.info(f"Writing Order-Level KPIs to Parquet: {output_path_order}")
        # Ensure the ../KPI_Output directory exists
        order_kpis_df.write.mode("overwrite").parquet(output_path_order)
        logger.info("Order-Level KPIs written successfully.")

    except Exception as e:
        logger.exception("Error writing KPIs to Parquet: %s", e)
        # spark.stop() # Optional: Stop on write error

    try:
        # Optionally display results
        logger.info("Displaying Category-Level KPIs (sample):")
        category_kpis_df.show(5, truncate=False)
        logger.info("Displaying Order-Level KPIs (sample):")
        order_kpis_df.show(5, truncate=False)
    except Exception as e:
        logger.exception("Error displaying results: %s", e)

    # Stop Spark session
    spark.stop()
    logger.info("Spark session stopped.")

if __name__ == "__main__":
    main()