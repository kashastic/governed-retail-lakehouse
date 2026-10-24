-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 03 · Proof that the pipeline is incremental
-- MAGIC
-- MAGIC Run after at least three pipeline updates:
-- MAGIC 1. backfill (batch 0),
-- MAGIC 2. a rerun **without** landing anything new,
-- MAGIC 3. after landing one or more daily batches.
-- MAGIC
-- MAGIC What each query should show:
-- MAGIC
-- MAGIC | Query | Expected |
-- MAGIC |---|---|
-- MAGIC | A. Files and load time per batch | Each batch loaded once, at a different time; earlier batches never reloaded |
-- MAGIC | B. Rows written per flow per update | Update 2 writes 0 rows to bronze; update 3 writes only the new batch's rows; `gold.fact_order_line` grows in the same update as `silver.order_lines` |
-- MAGIC | C. Materialized view refresh technique | Whether each gold view was refreshed incrementally or fully recomputed |
-- MAGIC | D. Expectation results | Dropped rows per rule, matching the quarantine tables and the generator manifest |
-- MAGIC
-- MAGIC Screenshot B and C for the README.

-- COMMAND ----------

-- The event log of the whole pipeline, reached through any table it manages.
CREATE OR REPLACE TEMPORARY VIEW pipeline_events AS
SELECT * FROM event_log(TABLE(retail.silver.order_lines))

-- COMMAND ----------

-- A. Each landing batch is read once. Re-running the pipeline does not reload old files.
SELECT regexp_extract(_source_file, '/(batch_[0-9]{3})/', 1) AS batch_id,
       count(DISTINCT _source_file)                           AS files,
       count(*)                                               AS order_records,
       min(_ingested_at)                                      AS loaded_at
FROM retail.bronze.orders_raw
GROUP BY ALL
ORDER BY batch_id

-- COMMAND ----------

-- B. Rows written per flow in each pipeline update.
-- Streaming flows report rows per micro-batch on RUNNING events; materialized views report
-- them once, on the COMPLETED event; AUTO CDC flows report upserts. 0 means nothing new was written.
WITH progress AS (
  SELECT origin.update_id,
         timestamp,
         origin.flow_name,
         details:flow_progress:status AS status,
         try_cast(details:flow_progress:metrics:num_output_rows AS BIGINT)   AS output_rows,
         try_cast(details:flow_progress:metrics:num_upserted_rows AS BIGINT) AS upserted_rows
  FROM pipeline_events
  WHERE event_type = 'flow_progress'
)
SELECT update_id,
       min(timestamp) AS update_time,
       flow_name,
       coalesce(max(CASE WHEN status = 'COMPLETED' THEN output_rows END),
                sum(CASE WHEN status = 'RUNNING' THEN output_rows END),
                sum(upserted_rows),
                0) AS rows_written
FROM progress
GROUP BY ALL
ORDER BY update_time, flow_name

-- COMMAND ----------

-- C. How each materialized view was refreshed (incremental vs complete recompute).
SELECT timestamp, origin.flow_name, message, details:planning_information AS planning_details
FROM pipeline_events
WHERE event_type = 'planning_information'
ORDER BY timestamp DESC

-- COMMAND ----------

-- D. Expectation results across all updates.
SELECT e.dataset,
       e.name                AS expectation,
       sum(e.passed_records) AS passed_records,
       sum(e.failed_records) AS failed_records
FROM (
  SELECT explode(from_json(details:flow_progress:data_quality:expectations,
                 'array<struct<name: string, dataset: string, passed_records: bigint, failed_records: bigint>>')) AS e
  FROM pipeline_events
  WHERE event_type = 'flow_progress'
)
GROUP BY ALL
ORDER BY dataset, expectation

-- COMMAND ----------

-- Table history of the bronze orders stream: one STREAMING UPDATE per update that brought new files,
-- none for an update with nothing new. operationMetrics.numOutputRows shows the rows each one added.
DESCRIBE HISTORY retail.bronze.orders_raw
