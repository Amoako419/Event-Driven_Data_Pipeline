from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, sum as _sum, countDistinct, count, when, round
)
import boto3
from boto3.dynamodb.conditions import Key

# Start Spark session
spark = SparkSession.builder.appName("KPIsToDynamoDB").getOrCreate()

# Load datasets
order_items_df = spark.read.option("header", True).csv("path/to/order_items.csv", inferSchema=True)
orders_df = spark.read.option("header", True).csv("path/to/orders.csv", inferSchema=True)
products_df = spark.read.option("header", True).csv("path/to/products.csv", inferSchema=True)

# Preprocess orders_df and order_items_df
orders_df = orders_df.withColumn("order_date", to_date(col("created_at")))
order_items_df = order_items_df.withColumn("sale_price", col("sale_price").cast("float"))

# --- Category-Level KPIs ---
category_kpis_df = (
    order_items_df
    .join(orders_df.select("order_id", "order_date", "status"), on="order_id")
    .join(products_df.select(col("id").alias("product_id"), "category"), on="product_id")
    .withColumn("is_returned", when(col("status") == "returned", 1).otherwise(0))
    .groupBy("category", "order_date")
    .agg(
        round(_sum("sale_price"), 2).alias("daily_revenue"),
        round(_sum("sale_price") / countDistinct("order_id"), 2).alias("avg_order_value"),
        round(_sum("is_returned") / countDistinct("order_id"), 4).alias("avg_return_rate")
    )
    .cache()  # Cache the category KPIs
)

# --- Order-Level KPIs ---
order_kpis_df = (
    order_items_df
    .join(orders_df.select("order_id", "user_id", "status", "order_date"), on="order_id")
    .withColumn("is_returned", when(col("status") == "returned", 1).otherwise(0))
    .groupBy("order_date")
    .agg(
        countDistinct("order_id").alias("total_orders"),
        round(_sum("sale_price"), 2).alias("total_revenue"),
        count("id").alias("total_items_sold"),
        round(_sum("is_returned") / countDistinct("order_id"), 4).alias("return_rate"),
        countDistinct("user_id").alias("unique_customers")
    )
    .cache()  # Cache the order KPIs
)

# --- Convert DataFrames to Lists of Dictionaries ---
category_kpis_data = category_kpis_df.collect()
order_kpis_data = order_kpis_df.collect()

# --- Initialize DynamoDB Client ---
dynamodb = boto3.resource('dynamodb')

# --- Define Tables ---
category_table = dynamodb.Table('CategoryLevelKPIs')  # Change to your category KPIs table name
order_table = dynamodb.Table('OrderLevelKPIs')  # Change to your order KPIs table name

# --- Function to Write Data to DynamoDB ---
def write_to_dynamodb(data, table):
    with table.batch_writer() as batch:
        for row in data:
            item = {key: row[key] for key in row.asDict().keys()}
            batch.put_item(Item=item)

# --- Insert Data into Category-Level KPIs Table ---
write_to_dynamodb(category_kpis_data, category_table)

# --- Insert Data into Order-Level KPIs Table ---
write_to_dynamodb(order_kpis_data, order_table)

# Optionally: Save the final DataFrame to Parquet as well
# final_kpis_df.write.mode("overwrite").parquet("path/to/output/final_kpis.parquet")
