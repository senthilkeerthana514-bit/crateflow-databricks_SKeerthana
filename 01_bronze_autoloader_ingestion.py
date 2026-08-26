# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze Layer: Auto Loader Ingestion
# MAGIC
# MAGIC Continuously ingests raw order JSON files from the landing zone into a
# MAGIC Bronze Delta table using **Databricks Auto Loader** (`cloudFiles`).
# MAGIC
# MAGIC - Schema inference + schema evolution enabled (new fields won't break the pipeline)
# MAGIC - Adds ingestion metadata (`_ingest_ts`, `_source_file`) for lineage/auditing
# MAGIC - Exactly-once, checkpointed, restart-safe
# MAGIC
# MAGIC **Run this notebook and leave the stream running** — that is what makes
# MAGIC ingestion "real time."

# COMMAND ----------

# MAGIC %md ### Widgets / configuration
# MAGIC Edit these paths to match your workspace (Unity Catalog Volume or DBFS).

# COMMAND ----------

dbutils.widgets.text("landing_path", "/Volumes/main/retail/landing/streaming_orders")
dbutils.widgets.text("schema_location", "/Volumes/main/retail/checkpoints/bronze_schema")
dbutils.widgets.text("checkpoint_path", "/Volumes/main/retail/checkpoints/bronze")
dbutils.widgets.text("bronze_table", "main.retail.raw_orders_bronze")

LANDING_PATH = dbutils.widgets.get("landing_path")
SCHEMA_LOCATION = dbutils.widgets.get("schema_location")
CHECKPOINT_PATH = dbutils.widgets.get("checkpoint_path")
BRONZE_TABLE = dbutils.widgets.get("bronze_table")

print(f"landing_path     = {LANDING_PATH}")
print(f"schema_location  = {SCHEMA_LOCATION}")
print(f"checkpoint_path  = {CHECKPOINT_PATH}")
print(f"bronze_table     = {BRONZE_TABLE}")

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md ### Read stream with Auto Loader (cloudFiles)

# COMMAND ----------

raw_stream_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load(LANDING_PATH)
)

bronze_df = (
    raw_stream_df
    .withColumn("_ingest_ts", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
)

# COMMAND ----------

# MAGIC %md ### Write stream to the Bronze Delta table
# MAGIC Trigger: micro-batch every 10 seconds (`processingTime`). For production
# MAGIC you could instead use `availableNow=True` on a schedule via a Databricks
# MAGIC Job — see `workflow/job_config.json`.

# COMMAND ----------

bronze_query = (
    bronze_df.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .option("mergeSchema", "true")
    .outputMode("append")
    .trigger(processingTime="10 seconds")
    .toTable(BRONZE_TABLE)
)

print(f"Bronze streaming query started: {bronze_query.id}")
print("Leave this cell running. Check progress with bronze_query.status / bronze_query.lastProgress")

# COMMAND ----------

# MAGIC %md ### (Optional) Inspect the stream while it runs
# MAGIC Run this in a *new* cell periodically — it queries the table as a normal
# MAGIC batch read, which is safe to do while the stream above keeps writing.

# COMMAND ----------

# display(spark.sql(f"SELECT * FROM {BRONZE_TABLE} ORDER BY _ingest_ts DESC LIMIT 20"))
