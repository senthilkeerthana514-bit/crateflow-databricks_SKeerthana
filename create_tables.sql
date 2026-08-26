-- create_tables.sql
-- Run this first (Step 2 in README.md) to set up the catalog, schema and
-- storage locations used by the streaming pipeline.
--
-- If your workspace does NOT have Unity Catalog (e.g. Community Edition),
-- skip the CATALOG/VOLUME statements below and just replace every
-- "main.retail.xxx" table reference in the notebooks with a plain
-- database name, e.g. "retail.xxx", and use DBFS paths
-- (dbfs:/FileStore/streaming_orders) instead of Volumes for the landing
-- zone and checkpoints.

-- 1. Catalog + schema (Unity Catalog workspaces)
CREATE CATALOG IF NOT EXISTS main;
CREATE SCHEMA IF NOT EXISTS main.retail
  COMMENT 'Real-time retail order analytics pipeline';

-- 2. Volumes for landing zone data and streaming checkpoints
CREATE VOLUME IF NOT EXISTS main.retail.landing;
CREATE VOLUME IF NOT EXISTS main.retail.checkpoints;

-- After creating the volumes, the effective paths used in the notebooks are:
--   Landing zone : /Volumes/main/retail/landing/streaming_orders
--   Checkpoints  : /Volumes/main/retail/checkpoints/<bronze|silver|gold>/...

-- 3. (Optional) Pre-create empty Bronze table so permissions/grants can be
--    set up before the stream starts. Auto Loader will create it
--    automatically on first run if it does not exist, so this is optional.
-- CREATE TABLE IF NOT EXISTS main.retail.raw_orders_bronze (
--   order_id STRING,
--   customer_name STRING,
--   product_name STRING,
--   category STRING,
--   quantity INT,
--   unit_price DOUBLE,
--   total_amount DOUBLE,
--   region STRING,
--   payment_method STRING,
--   order_status STRING,
--   order_timestamp STRING,
--   _ingest_ts TIMESTAMP,
--   _source_file STRING
-- ) USING DELTA;

-- 4. Grants (adjust principal names to your workspace users/groups)
-- GRANT USE CATALOG ON CATALOG main TO `account users`;
-- GRANT USE SCHEMA ON SCHEMA main.retail TO `account users`;
-- GRANT SELECT ON SCHEMA main.retail TO `account users`;
