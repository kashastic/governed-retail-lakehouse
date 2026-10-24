-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Grants
-- MAGIC
-- MAGIC Least privilege by layer. Analysts (and the dashboard and Genie, which run as the viewer) read only the
-- MAGIC curated layers: `gold`, `semantic` and the `dq` summary. Raw, bronze and silver, including the quarantine tables
-- MAGIC and the customer PII in `silver.customers`, stay with engineers.
-- MAGIC
-- MAGIC Prerequisite: groups `retail_engineers` and `retail_analysts` exist (Settings → Identity and access → Groups).
-- MAGIC Fallback if groups aren't available on Free Edition: replace `` `retail_analysts` `` with the second user's email.

-- COMMAND ----------

-- Engineers own the whole catalog.
GRANT ALL PRIVILEGES ON CATALOG retail TO `retail_engineers`;

-- Analysts: browse the catalog, read curated layers only.
GRANT USE CATALOG ON CATALOG retail TO `retail_analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA retail.gold     TO `retail_analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA retail.semantic TO `retail_analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA retail.dq       TO `retail_analysts`;
-- Needed to call the masking function inside gold.customer_contact.
GRANT USE SCHEMA, EXECUTE ON SCHEMA retail.governance TO `retail_analysts`;

-- COMMAND ----------

-- Evidence for the README: who can read what.
SHOW GRANTS ON CATALOG retail;

-- COMMAND ----------

SHOW GRANTS ON SCHEMA retail.gold;

-- COMMAND ----------

SHOW GRANTS ON SCHEMA retail.silver;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Test as the analyst
-- MAGIC
-- MAGIC Signed in as a member of `retail_analysts` (the second user), run:
-- MAGIC
-- MAGIC ```sql
-- MAGIC SELECT * FROM retail.gold.dim_customer LIMIT 5;           -- works
-- MAGIC SELECT * FROM retail.gold.customer_contact LIMIT 5;       -- works, emails masked
-- MAGIC SELECT * FROM retail.silver.customers LIMIT 5;            -- PERMISSION_DENIED
-- MAGIC SELECT * FROM retail.silver.order_lines_quarantine;       -- PERMISSION_DENIED
-- MAGIC ```
-- MAGIC
-- MAGIC Screenshot the denied query. Also give the group **Can use** on the SQL warehouse and **Can view** on the dashboard
-- MAGIC and the Genie agent, or they can't run anything.
