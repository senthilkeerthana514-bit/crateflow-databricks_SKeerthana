# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver Layer: Cleaning & Standardization
# MAGIC
# MAGIC Reads the Bronze table **as a stream**, cleans and validates it, then
# MAGIC writes trusted records to `orders_silver` and bad records to
# MAGIC `orders_silver_quarantine`.
# MAGIC
# MAGIC Data quality rules applied:
# MAGIC - `order_id` must not be null -> hard requirement
# MAGIC - `customer_name` must not be null
# MAGIC - `quantity` must be > 0
# MAGIC - Deduplicate by `order_id` (handles files that get re-delivered)
# MAGIC - Cast `order_timestamp` to a proper `timestamp` type

# COMMAND ----------

dbutils.widgets.text("bronze_table", "main.retail.raw_orders_bronze")
dbutils.widgets.text("silver_table", "main.retail.orders_silver")
dbutils.widgets.text("quarantine_table", "main.retail.orders_silver_quarantine")
dbutils.widgets.text("checkpoint_path", "/Volumes/main/retail/checkpoints/silver")
dbutils.widgets.text("quarantine_checkpoint_path", "/Volumes/main/retail/checkpoints/silver_quarantine")

BRONZE_TABLE = dbutils.widgets.get("bronze_table")
SILVER_TABLE = dbutils.widgets.get("silver_table")
QUARANTINE_TABLE = dbutils.widgets.get("quarantine_table")
CHECKPOINT_PATH = dbutils.widgets.get("checkpoint_path")
QUARANTINE_CHECKPOINT_PATH = dbutils.widgets.get("quarantine_checkpoint_path")

# COMMAND ----------

from pyspark.sql import functions as F

bronze_stream = spark.readStream.table(BRONZE_TABLE)

typed_df = (
    bronze_stream
    .withColumn("order_timestamp", F.to_timestamp("order_timestamp"))
    .withColumn("quantity", F.col("quantity").cast("int"))
    .withColumn("unit_price", F.col("unit_price").cast("double"))
    .withColumn("total_amount", F.col("total_amount").cast("double"))
)

is_valid = (
    F.col("order_id").isNotNull()
    & F.col("customer_name").isNotNull()
    & (F.col("quantity") > 0)
    & F.col("order_timestamp").isNotNull()
)

valid_df = typed_df.filter(is_valid).dropDuplicates(["order_id"])
invalid_df = typed_df.filter(~is_valid)

# COMMAND ----------

# MAGIC %md ### Write valid records to Silver

# COMMAND ----------

silver_query = (
    valid_df.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .outputMode("append")
    .trigger(processingTime="10 seconds")
    .toTable(SILVER_TABLE)
)
print(f"Silver streaming query started: {silver_query.id}")

# COMMAND ----------

# MAGIC %md ### Write invalid records to a quarantine table for later review

# COMMAND ----------

quarantine_query = (
    invalid_df.writeStream
    .format("delta")
    .option("checkpointLocation", QUARANTINE_CHECKPOINT_PATH)
    .outputMode("append")
    .trigger(processingTime="10 seconds")
    .toTable(QUARANTINE_TABLE)
)
print(f"Quarantine streaming query started: {quarantine_query.id}")

# COMMAND ----------

# MAGIC %md Leave both cells running alongside the Bronze notebook. Query
# MAGIC `orders_silver` and `orders_silver_quarantine` any time to check progress.
