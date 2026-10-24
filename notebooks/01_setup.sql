-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 01 · Setup
-- MAGIC
-- MAGIC Creates the catalog, one schema per layer and the landing volume. Safe to re-run.
-- MAGIC Run on the SQL warehouse or serverless compute.
-- MAGIC
-- MAGIC | Schema | Holds | Who reads it |
-- MAGIC |---|---|---|
-- MAGIC | `raw` | Landing volume with the generator's files and manifests | Engineers |
-- MAGIC | `bronze` | Raw records as received (Auto Loader) | Engineers |
-- MAGIC | `silver` | Typed, validated, de-duplicated records; quarantine tables; customer PII | Engineers |
-- MAGIC | `gold` | Star schema (dims + facts), no direct identifiers | Analysts, dashboard, Genie |
-- MAGIC | `semantic` | Metric views: the governed KPI definitions | Analysts, dashboard, Genie |
-- MAGIC | `dq` | Key checks and the data-issue summary | Engineers, analysts |
-- MAGIC | `governance` | Masking function | Engineers |

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS retail COMMENT 'Governed Retail AI/BI Lakehouse: a fictional online retailer.';

CREATE SCHEMA IF NOT EXISTS retail.raw        COMMENT 'Landing zone: files exactly as delivered, plus the generator manifests.';
CREATE SCHEMA IF NOT EXISTS retail.bronze     COMMENT 'Raw records loaded incrementally with Auto Loader. Nothing filtered.';
CREATE SCHEMA IF NOT EXISTS retail.silver     COMMENT 'Typed, validated, de-duplicated records and quarantine tables. Contains personal data.';
CREATE SCHEMA IF NOT EXISTS retail.gold       COMMENT 'Star schema for analytics. No direct identifiers.';
CREATE SCHEMA IF NOT EXISTS retail.semantic   COMMENT 'Metric views: governed KPI definitions for dashboards and Genie.';
CREATE SCHEMA IF NOT EXISTS retail.dq         COMMENT 'Data-quality checks and the data-issue summary.';
CREATE SCHEMA IF NOT EXISTS retail.governance COMMENT 'Masking functions and other governance objects.';

CREATE VOLUME IF NOT EXISTS retail.raw.landing COMMENT 'Generator output: <entity>/<batch_id>/ files and _manifest/<batch_id>.json.';

-- COMMAND ----------

SHOW SCHEMAS IN retail
