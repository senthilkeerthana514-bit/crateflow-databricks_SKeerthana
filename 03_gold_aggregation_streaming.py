# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold Layer: Real-Time Business Aggregates
# MAGIC
# MAGIC Reads Silver **as a stream** and produces the business-ready tables that
# MAGIC power the dashboard:
# MAGIC 1. `revenue_by_category_5min` — 5-minute tumbling-window revenue per category
# MAGIC 2. `orders_by_region` — running order counts per region
# MAGIC 3. `top_products` — running revenue per product
# MAGIC
# MAGIC Uses **watermarking** (handles late data up to 10 minutes late) and
# MAGIC **`foreachBatch` + Delta `MERGE`** so the tables are continuously
# MAGIC *upserted* instead of endlessly appended — exactly what a live
# MAGIC dashboard needs.

# COMMAND ----------

dbutils.widgets.text("silver_table", "main.retail.orders_silver")
dbutils.widgets.text("revenue_table", "main.retail.revenue_by_category_5min")
dbutils.widgets.text("region_table", "main.retail.orders_by_region")
dbutils.widgets.text("top_products_table", "main.retail.top_products")
dbutils.widgets.text("checkpoint_root", "/Volumes/main/retail/checkpoints/gold")

SILVER_TABLE = dbutils.widgets.get("silver_table")
REVENUE_TABLE = dbutils.widgets.get("revenue_table")
REGION_TABLE = dbutils.widgets.get("region_table")
TOP_PRODUCTS_TABLE = dbutils.widgets.get("top_products_table")
CHECKPOINT_ROOT = dbutils.widgets.get("checkpoint_root")

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

silver_stream = spark.readStream.table(SILVER_TABLE)

# COMMAND ----------

# MAGIC %md ## 3.1 Revenue by category — 5-minute tumbling window

# COMMAND ----------

revenue_agg = (
    silver_stream
    .withWatermark("order_timestamp", "10 minutes")
    .groupBy(
        F.window("order_timestamp", "5 minutes").alias("window"),
        "category",
    )
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


def upsert_revenue(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    if not spark.catalog.tableExists(REVENUE_TABLE):
        batch_df.write.format("delta").saveAsTable(REVENUE_TABLE)
        return
    target = DeltaTable.forName(spark, REVENUE_TABLE)
    (
        target.alias("t")
        .merge(
            batch_df.alias("s"),
            "t.window_start = s.window_start AND t.category = s.category",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


revenue_query = (
    revenue_agg.writeStream
    .foreachBatch(upsert_revenue)
    .option("checkpointLocation", f"{CHECKPOINT_ROOT}/revenue_by_category")
    .outputMode("update")
    .trigger(processingTime="15 seconds")
    .start()
)
print(f"Revenue aggregation query started: {revenue_query.id}")

# COMMAND ----------

# MAGIC %md ## 3.2 Orders by region — running totals

# COMMAND ----------

region_agg = (
    silver_stream
    .withWatermark("order_timestamp", "10 minutes")
    .groupBy("region")
    .agg(
        F.count("order_id").alias("order_count"),
        F.sum("total_amount").alias("total_revenue"),
    )
)


def upsert_region(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    if not spark.catalog.tableExists(REGION_TABLE):
        batch_df.write.format("delta").saveAsTable(REGION_TABLE)
        return
    target = DeltaTable.forName(spark, REGION_TABLE)
    (
        target.alias("t")
        .merge(batch_df.alias("s"), "t.region = s.region")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


region_query = (
    region_agg.writeStream
    .foreachBatch(upsert_region)
    .option("checkpointLocation", f"{CHECKPOINT_ROOT}/orders_by_region")
    .outputMode("update")
    .trigger(processingTime="15 seconds")
    .start()
)
print(f"Region aggregation query started: {region_query.id}")

# COMMAND ----------

# MAGIC %md ## 3.3 Top products — running revenue per product

# COMMAND ----------

product_agg = (
    silver_stream
    .withWatermark("order_timestamp", "10 minutes")
    .groupBy("product_name", "category")
    .agg(
        F.sum("total_amount").alias("total_revenue"),
        F.sum("quantity").alias("total_units_sold"),
    )
)


def upsert_products(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    if not spark.catalog.tableExists(TOP_PRODUCTS_TABLE):
        batch_df.write.format("delta").saveAsTable(TOP_PRODUCTS_TABLE)
        return
    target = DeltaTable.forName(spark, TOP_PRODUCTS_TABLE)
    (
        target.alias("t")
        .merge(batch_df.alias("s"), "t.product_name = s.product_name")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


products_query = (
    product_agg.writeStream
    .foreachBatch(upsert_products)
    .option("checkpointLocation", f"{CHECKPOINT_ROOT}/top_products")
    .outputMode("update")
    .trigger(processingTime="15 seconds")
    .start()
)
print(f"Top products aggregation query started: {products_query.id}")

# COMMAND ----------

# MAGIC %md Leave all three cells running alongside Bronze and Silver. Query the
# MAGIC three Gold tables any time — they update every ~15 seconds.
