# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Delta Live Tables (DLT) Version of the Same Pipeline
# MAGIC
# MAGIC This is a **declarative** alternative to notebooks 01-03. Instead of
# MAGIC manually managing `readStream`/`writeStream`/checkpoints, DLT figures out
# MAGIC the dependency graph (Bronze -> Silver -> Gold) automatically, retries
# MAGIC failed runs, and enforces data quality with `@dlt.expect` rules.
# MAGIC
# MAGIC ### How to run this one
# MAGIC 1. Go to **Workflows -> Delta Live Tables -> Create Pipeline**
# MAGIC 2. Source notebook: this file
# MAGIC 3. Target catalog/schema: e.g. `main.retail_dlt`
# MAGIC 4. Pipeline mode: "Triggered" (batch-like runs on schedule) or
# MAGIC    "Continuous" (true streaming, like notebooks 01-03)
# MAGIC 5. Click **Start**
# MAGIC
# MAGIC Do NOT run this notebook interactively with "Run All" — DLT notebooks are
# MAGIC only executed by the DLT pipeline engine, not standard clusters.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

LANDING_PATH = "/Volumes/main/retail/landing/streaming_orders"

# COMMAND ----------

# MAGIC %md ## Bronze — raw ingestion with Auto Loader

# COMMAND ----------

@dlt.table(
    name="raw_orders_bronze",
    comment="Raw order events ingested as-is from the landing zone via Auto Loader.",
)
def raw_orders_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(LANDING_PATH)
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )

# COMMAND ----------

# MAGIC %md ## Silver — cleaned & quality-checked
# MAGIC `@dlt.expect_or_drop` automatically drops rows that fail the rule and
# MAGIC records the failure count in the pipeline's data-quality metrics UI.

# COMMAND ----------

@dlt.table(
    name="orders_silver",
    comment="Cleaned, validated, de-duplicated order records.",
)
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_customer", "customer_name IS NOT NULL")
@dlt.expect_or_drop("valid_quantity", "quantity > 0")
def orders_silver():
    return (
        dlt.read_stream("raw_orders_bronze")
        .withColumn("order_timestamp", F.to_timestamp("order_timestamp"))
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("unit_price", F.col("unit_price").cast("double"))
        .withColumn("total_amount", F.col("total_amount").cast("double"))
        .dropDuplicates(["order_id"])
    )

# COMMAND ----------

# MAGIC %md ## Gold — business aggregates

# COMMAND ----------

@dlt.table(
    name="revenue_by_category_5min",
    comment="Revenue and order count per category in 5-minute tumbling windows.",
)
def revenue_by_category_5min():
    return (
        dlt.read_stream("orders_silver")
        .withWatermark("order_timestamp", "10 minutes")
        .groupBy(F.window("order_timestamp", "5 minutes").alias("window"), "category")
        .agg(
            F.sum("total_amount").alias("revenue"),
            F.count("order_id").alias("order_count"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "category",
            "revenue",
            "order_count",
        )
    )

# COMMAND ----------

@dlt.table(
    name="orders_by_region",
    comment="Running order counts and revenue per region.",
)
def orders_by_region():
    return (
        dlt.read_stream("orders_silver")
        .withWatermark("order_timestamp", "10 minutes")
        .groupBy("region")
        .agg(
            F.count("order_id").alias("order_count"),
            F.sum("total_amount").alias("total_revenue"),
        )
    )

# COMMAND ----------

@dlt.table(
    name="top_products",
    comment="Running revenue and units sold per product.",
)
def top_products():
    return (
        dlt.read_stream("orders_silver")
        .withWatermark("order_timestamp", "10 minutes")
        .groupBy("product_name", "category")
        .agg(
            F.sum("total_amount").alias("total_revenue"),
            F.sum("quantity").alias("total_units_sold"),
        )
    )
