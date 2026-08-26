-- gold_layer_queries.sql
-- Paste these into Databricks SQL -> Queries, then add each as a
-- visualization to a Dashboard (Step 10 in README.md). Set the dashboard
-- refresh schedule to 1 minute to see it update live while the pipeline runs.

-- ============================================================
-- 1) Revenue by category over time (line chart)
--    X axis: window_start, Y axis: revenue, Group/Series: category
-- ============================================================
SELECT
  window_start,
  window_end,
  category,
  revenue,
  order_count
FROM main.retail.revenue_by_category_5min
ORDER BY window_start DESC;

-- ============================================================
-- 2) Top 10 products by revenue (bar chart)
--    X axis: product_name, Y axis: total_revenue
-- ============================================================
SELECT
  product_name,
  category,
  total_revenue,
  total_units_sold
FROM main.retail.top_products
ORDER BY total_revenue DESC
LIMIT 10;

-- ============================================================
-- 3) Orders and revenue by region (bar chart / map)
-- ============================================================
SELECT
  region,
  order_count,
  total_revenue
FROM main.retail.orders_by_region
ORDER BY total_revenue DESC;

-- ============================================================
-- 4) Live pipeline health check — rows landed in the last 5 minutes
--    Useful as a "counter" widget to prove the pipeline is actually live
-- ============================================================
SELECT
  COUNT(*) AS orders_last_5_min
FROM main.retail.orders_silver
WHERE order_timestamp >= current_timestamp() - INTERVAL 5 MINUTES;

-- ============================================================
-- 5) Data quality — quarantined (rejected) records count
-- ============================================================
SELECT
  COUNT(*) AS quarantined_records
FROM main.retail.orders_silver_quarantine;
