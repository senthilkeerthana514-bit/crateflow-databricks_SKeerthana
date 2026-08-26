# Real-Time Retail Order Analytics Pipeline (Databricks)

A complete, hands-on real-time data engineering project you can build in a free
Databricks Community Edition or a full Databricks workspace. It simulates a
retail company that wants **live dashboards** of orders, revenue, and top
products, built the way real production pipelines are built on Databricks:
**Auto Loader -> Structured Streaming -> Delta Lake -> Medallion Architecture
(Bronze / Silver / Gold) -> Delta Live Tables -> Databricks Workflow (job
schedule) -> SQL Dashboard.**

---

## 1. Business Use Case

An e-commerce company's order system pushes one JSON file per order (or a
batch of orders) into cloud storage every few seconds. Data Engineering must:

1. Ingest the raw files continuously and reliably (no data loss, no
   duplicates) -> **Bronze**
2. Clean, validate, and standardize the data (types, nulls, bad records
   quarantined) -> **Silver**
3. Aggregate business metrics in near real time (revenue per category per
   5-minute window, top products, order counts per region) -> **Gold**
4. Serve the Gold tables to a BI dashboard that refreshes automatically.

This is the exact pattern used in real Databricks data engineering jobs, and
it is the project most commonly asked about in interviews for this role.

---

## 2. Architecture

```
 [Order Producer] --> writes JSON files every 2-5 sec
        |
        v
 landing zone (DBFS / cloud storage: /FileStore/streaming_orders/)
        |
        v  (Databricks Auto Loader - cloudFiles, schema inference + evolution)
 +----------------+
 |  BRONZE TABLE  |  raw_orders_bronze   (raw JSON as-is + ingestion metadata)
 +----------------+
        |  (Structured Streaming readStream -> transform -> writeStream)
        v
 +----------------+
 |  SILVER TABLE  |  orders_silver  (cleaned, typed, deduplicated, quality-checked)
 +----------------+
        |  (Structured Streaming windowed aggregation, foreachBatch MERGE)
        v
 +----------------+
 |  GOLD TABLES   |  revenue_by_category_5min, top_products, orders_by_region
 +----------------+
        |
        v
 Databricks SQL Dashboard  (auto-refresh every 1 min)
```

**Why this design (talking points for interviews):**
- Auto Loader (`cloudFiles`) incrementally and efficiently discovers new
  files without listing the whole directory every time — scales to millions
  of files.
- Structured Streaming with checkpoints gives **exactly-once** processing
  and automatic recovery after failure.
- Medallion architecture separates concerns: Bronze = raw/replayable,
  Silver = trusted/queryable, Gold = business-ready/aggregated.
- Delta Lake gives ACID transactions, schema enforcement/evolution, time
  travel, and `MERGE` for upserts — all needed for reliable streaming
  pipelines.
- Delta Live Tables (DLT) version shows the declarative alternative with
  built-in data quality **expectations** and automatic orchestration.

---

## 3. Folder Contents

```
databricks_realtime_project/
├── README.md                                 <- this file
├── requirements.txt                           <- Python deps for the local data generator
├── data_generator/
│   └── generate_streaming_orders.py           <- simulates the real-time order source
├── notebooks/
│   ├── 01_bronze_autoloader_ingestion.py      <- Bronze layer (Auto Loader)
│   ├── 02_silver_transformation_streaming.py  <- Silver layer (cleaning)
│   ├── 03_gold_aggregation_streaming.py       <- Gold layer (windowed aggregates)
│   └── 04_dlt_pipeline_alternative.py         <- same pipeline as Delta Live Tables
├── sql/
│   └── create_tables.sql                      <- catalog/schema/table setup
├── dashboard/
│   └── gold_layer_queries.sql                 <- ready-to-use dashboard queries
└── workflow/
    └── job_config.json                        <- Databricks Jobs API definition (orchestration)
```

---

## 4. Prerequisites

- A Databricks workspace (Community Edition is fine — free at
  https://community.cloud.databricks.com, or any trial workspace).
- A running cluster / SQL warehouse (Databricks Runtime 13+ recommended,
  Unity Catalog optional but shown in the SQL).
- Python 3.9+ locally only if you want to run the data generator from your
  own machine instead of inside a Databricks notebook.

---

## 5. Step-by-Step Setup & Execution

### Step 1 — Upload this project to your Databricks workspace
1. In Databricks, go to **Workspace -> Import**.
2. Import each file under `notebooks/` as a notebook (they are plain
   `.py` files formatted with Databricks notebook `# COMMAND ----------`
   markers, so Databricks recognizes them as notebooks automatically).
3. Alternatively, use **Repos**: push this whole folder to a Git repo and
   use "Repos -> Add Repo" in Databricks to clone it directly.

### Step 2 — Create the landing zone and catalog objects
1. Open a SQL editor / notebook and run `sql/create_tables.sql`. This
   creates the catalog, schema, and the landing directory path used by
   Auto Loader.
2. This project uses these default paths (edit at the top of each
   notebook if you use different ones):
   - Landing zone: `/Volumes/main/retail/landing/streaming_orders/`
     (or `dbfs:/FileStore/streaming_orders/` on Community Edition)
   - Checkpoints: `/Volumes/main/retail/checkpoints/...`
   - Tables: `main.retail.raw_orders_bronze`, `main.retail.orders_silver`,
     `main.retail.revenue_by_category_5min`, etc.

### Step 3 — Start the real-time order generator
Run **either**:
- Inside Databricks: open `data_generator/generate_streaming_orders.py`
  as a notebook and run it (it will write files directly to the landing
  volume/DBFS path).
- Locally: `pip install -r requirements.txt` then
  `python data_generator/generate_streaming_orders.py` — it writes JSON
  files to a local `./streaming_orders/` folder, which you can then sync
  or manually upload to DBFS via the Databricks CLI
  (`databricks fs cp -r ./streaming_orders dbfs:/FileStore/streaming_orders`).

This script runs in an infinite loop, producing 1 order JSON file every
2–5 seconds, forever — simulating a live production order feed. Stop it
with `Ctrl+C` any time.

### Step 4 — Run the Bronze notebook (Auto Loader ingestion)
Open and **Run All** on `notebooks/01_bronze_autoloader_ingestion.py`.
- It starts a `writeStream` using `cloudFiles` (Auto Loader) that watches
  the landing zone and appends every new file into
  `raw_orders_bronze` as soon as it lands (default trigger:
  `availableNow=False`, i.e. continuous micro-batches every 10 seconds).
- Leave this notebook's stream **running** (don't cancel the cell) — that
  is what makes it "real time."

### Step 5 — Run the Silver notebook (cleaning)
Open and **Run All** on `notebooks/02_silver_transformation_streaming.py`
in a **separate notebook / cluster tab** while Bronze keeps streaming.
- It reads the Bronze table as a stream, casts types, drops bad/null
  records into a `orders_silver_quarantine` table, deduplicates by
  `order_id`, and writes clean records to `orders_silver`.

### Step 6 — Run the Gold notebook (aggregation)
Open and **Run All** on `notebooks/03_gold_aggregation_streaming.py`.
- It reads Silver as a stream, computes a 5-minute tumbling-window revenue
  aggregate with watermarking (handles late-arriving data), and uses
  `foreachBatch` + Delta `MERGE` to upsert results into the Gold tables so
  the dashboard always shows current totals instead of duplicating rows.

### Step 7 — Watch it work end to end
With the generator (Step 3) and all three streams (Steps 4–6) running:
```sql
SELECT * FROM main.retail.revenue_by_category_5min ORDER BY window_start DESC;
```
Re-run this query every 30–60 seconds — the numbers will keep climbing as
new simulated orders flow through Bronze -> Silver -> Gold automatically.
You can also open the **Spark UI / Structured Streaming tab** on the
cluster to see live throughput, batch duration, and input rate graphs.

### Step 8 (optional) — Delta Live Tables version
`notebooks/04_dlt_pipeline_alternative.py` reproduces the exact same
Bronze -> Silver -> Gold logic declaratively using `@dlt.table` and
`@dlt.expect` data-quality rules. To run it:
1. Go to **Workflows -> Delta Live Tables -> Create Pipeline**.
2. Point it at this notebook, set the target catalog/schema, choose
   "Triggered" or "Continuous" mode, and click **Start**.
3. DLT handles checkpointing, retries, and the dependency graph between
   the three tables automatically — no manual `writeStream` needed.

### Step 9 — Orchestrate with a Databricks Job (production pattern)
`workflow/job_config.json` defines a multi-task Databricks Workflow:
`bronze_ingestion -> silver_transformation -> gold_aggregation`, scheduled
every 5 minutes, with retry-on-failure enabled. Import it with:
```bash
databricks jobs create --json @workflow/job_config.json
```
(This is how the pipeline would actually be scheduled and monitored in
production, instead of you manually running notebooks.)

### Step 10 — Build the dashboard
Use the queries in `dashboard/gold_layer_queries.sql` in **Databricks SQL
-> Dashboards** to build 3 visuals: revenue by category over time (line
chart), top 10 products (bar chart), and orders by region (map/bar). Set
the dashboard refresh to 1 minute to see it update live.

### Step 11 — Clean up (avoid ongoing cost)
- Cancel the three running streaming notebooks (Bronze/Silver/Gold) or
  stop the DLT pipeline.
- Stop the data generator.
- Optionally run: `DROP SCHEMA main.retail CASCADE;` to remove all tables.

---

## 6. How the "Real-Time" Part Actually Works

- **Auto Loader (`cloudFiles`)**: instead of re-listing an entire cloud
  storage directory (slow, expensive at scale), it keeps track of which
  files it has already seen using a scalable RocksDB-backed state store,
  so it only processes *new* files each micro-batch.
- **Structured Streaming micro-batches**: each stream wakes up on a
  trigger interval (default here: every 10–30 seconds), pulls only the
  new/changed data since last run, processes it, and commits a
  checkpoint. If the cluster crashes, restarting the notebook resumes
  exactly where it left off — no data loss, no duplicates.
- **Watermarking**: the Gold layer uses
  `withWatermark("event_time", "10 minutes")` so it can correctly handle
  orders that arrive a bit late (e.g., due to network delay) while still
  being able to close out and finalize old aggregation windows.
- **`foreachBatch` + `MERGE`**: lets a streaming query do an *upsert*
  into a Delta table (update existing aggregate rows instead of just
  appending), which is what real dashboards need.

---

## 7. Resume / Interview Talking Points

- Built a medallion (Bronze/Silver/Gold) streaming pipeline on Databricks
  using Auto Loader and Structured Streaming with exactly-once
  guarantees via Delta Lake checkpointing.
- Implemented data quality checks and a quarantine table for bad records
  in the Silver layer.
- Used watermarking and tumbling windows to compute near-real-time
  revenue aggregates, upserted via `foreachBatch` + `MERGE`.
- Delivered an equivalent Delta Live Tables pipeline with declarative
  `@dlt.expect` data-quality constraints for comparison.
- Orchestrated and scheduled the pipeline with Databricks Workflows and
  exposed results through a Databricks SQL dashboard.
