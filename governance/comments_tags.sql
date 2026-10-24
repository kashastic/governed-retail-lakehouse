-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Documentation and tags
-- MAGIC
-- MAGIC Table and column descriptions for gold are set by the pipeline itself (`pipelines/utilities/catalog_docs.py`),
-- MAGIC so they can't drift from the code. Metric views carry their own descriptions and synonyms in YAML.
-- MAGIC This notebook adds what the pipeline can't: tags for classifying data.
-- MAGIC
-- MAGIC Catalog and schema tags always work. Tags on pipeline-owned tables may be refused from SQL. If a cell fails,
-- MAGIC add the same tag in **Catalog Explorer → table → Tags**, and note which way worked.

-- COMMAND ----------

ALTER CATALOG retail SET TAGS ('project' = 'governed-retail-lakehouse', 'data' = 'synthetic');
ALTER SCHEMA retail.silver SET TAGS ('contains_pii' = 'true', 'layer' = 'silver');
ALTER SCHEMA retail.gold   SET TAGS ('contains_pii' = 'false', 'layer' = 'gold');
ALTER SCHEMA retail.bronze SET TAGS ('contains_pii' = 'true', 'layer' = 'bronze');

-- COMMAND ----------

ALTER STREAMING TABLE retail.silver.customers ALTER COLUMN email      SET TAGS ('pii' = 'email');
ALTER STREAMING TABLE retail.silver.customers ALTER COLUMN first_name SET TAGS ('pii' = 'name');
ALTER STREAMING TABLE retail.silver.customers ALTER COLUMN last_name  SET TAGS ('pii' = 'name');

-- COMMAND ----------

-- Where is personal data? (information_schema reads the tags.)
SELECT catalog_name, schema_name, table_name, column_name, tag_name, tag_value
FROM retail.information_schema.column_tags
ORDER BY schema_name, table_name, column_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Lineage evidence
-- MAGIC
-- MAGIC Catalog Explorer → `retail.semantic.sales_metrics` → **Lineage** → **See lineage graph**. Expand upstream until
-- MAGIC you see metric view → gold facts and dims → silver → bronze. Screenshot it for the README.
