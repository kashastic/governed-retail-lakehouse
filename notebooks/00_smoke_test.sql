-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 00 · Day-1 smoke test
-- MAGIC
-- MAGIC Run this **before building anything**. It checks the four Free Edition features the project depends on.
-- MAGIC Attach it to the **Serverless Starter Warehouse** (SQL warehouse) and run the cells one by one.
-- MAGIC Write the outcome of each check into the table at the bottom and into `docs/RUNBOOK.md`.
-- MAGIC
-- MAGIC | # | Check | If it fails |
-- MAGIC |---|---|---|
-- MAGIC | 1 | `CREATE CATALOG retail` | Use the built-in `workspace` catalog: replace `retail.` with `workspace.` everywhere |
-- MAGIC | 2 | Metric view with a window measure | **Stop.** The CV line can't claim Metric Views. Reword it before going further. |
-- MAGIC | 3 | Groups + `GRANT` | Invite a second email as the "analyst" user and grant to that user instead |
-- MAGIC | 4 | Genie is available | Without Genie the evaluation can't run |

-- COMMAND ----------

-- Which runtime is the warehouse on? Metric views need 17.3 or newer for everything used here.
SELECT current_version() AS version

-- COMMAND ----------

-- MAGIC %md ### 1 · Create the project catalog

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS retail COMMENT 'Governed Retail AI/BI Lakehouse';
CREATE SCHEMA IF NOT EXISTS retail.smoke COMMENT 'Temporary: day-1 smoke test. Dropped at the end of this notebook.';

-- COMMAND ----------

-- MAGIC %md ### 2 · Metric view with joins, synonyms and a window measure

-- COMMAND ----------

CREATE OR REPLACE TABLE retail.smoke.orders AS
SELECT * FROM VALUES
  ('O1', 'C1', DATE'2026-08-01', 'Clothing', 40.00),
  ('O2', 'C2', DATE'2026-08-02', 'Grocery',  12.50),
  ('O3', 'C1', DATE'2026-08-20', 'Shoes',    90.00),
  ('O4', 'C3', DATE'2026-09-05', 'Clothing', 25.00),
  ('O5', 'C2', DATE'2026-09-06', 'Grocery',   8.00)
AS t(order_id, customer_id, order_date, category, amount);

CREATE OR REPLACE TABLE retail.smoke.customers AS
SELECT * FROM VALUES ('C1', 'Plus'), ('C2', 'Basic'), ('C3', 'Premium') AS t(customer_id, membership);

-- COMMAND ----------

CREATE OR REPLACE VIEW retail.smoke.order_metrics
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: "Smoke test metric view."
source: retail.smoke.orders
joins:
  - name: customer
    source: retail.smoke.customers
    on: source.customer_id = customer.customer_id
fields:
  - name: order_date
    expr: source.order_date
  - name: order_month
    expr: DATE_TRUNC('MONTH', source.order_date)
    display_name: "Order month"
  - name: membership
    expr: customer.membership
    synonyms: ["tier"]
measures:
  - name: revenue
    expr: SUM(source.amount)
    synonyms: ["sales"]
  - name: orders
    expr: COUNT(DISTINCT source.order_id)
  - name: aov
    expr: SUM(source.amount) / COUNT(DISTINCT source.order_id)
  - name: active_customers_30d
    expr: COUNT(DISTINCT source.customer_id)
    window:
      - order: order_date
        range: trailing 30 day
        semiadditive: last
$$

-- COMMAND ----------

-- Expect: 2026-08 -> revenue 142.50, orders 3; 2026-09 -> revenue 33.00, orders 2.
SELECT order_month, MEASURE(revenue) AS revenue, MEASURE(orders) AS orders, MEASURE(aov) AS aov
FROM retail.smoke.order_metrics
GROUP BY order_month
ORDER BY order_month

-- COMMAND ----------

-- Window measure. If this cell fails but the one above works, Metric Views are fine:
-- drop active_customers_30d from semantic/sales_metrics.sql and keep plain active_customers.
SELECT order_date, MEASURE(active_customers_30d) AS active_customers_30d
FROM retail.smoke.order_metrics
GROUP BY order_date
ORDER BY order_date

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3 · Groups and grants
-- MAGIC
-- MAGIC 1. Open **Settings → Identity and access → Groups** (workspace settings, top-right user menu).
-- MAGIC 2. Create two groups: `retail_engineers` and `retail_analysts`. Add yourself to `retail_engineers`.
-- MAGIC 3. Run the cells below. If the Groups page is missing or `GRANT` fails with "principal does not exist",
-- MAGIC    note it down: the fallback is to invite a second email address as the analyst user.

-- COMMAND ----------

SELECT is_account_group_member('retail_engineers') AS in_engineers,
       is_account_group_member('retail_analysts')  AS in_analysts

-- COMMAND ----------

GRANT USE CATALOG ON CATALOG retail TO `retail_analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA retail.smoke TO `retail_analysts`;
SHOW GRANTS ON SCHEMA retail.smoke;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4 · Genie
-- MAGIC
-- MAGIC In the left sidebar, open **Genie** and click **New**. Add `retail.smoke.order_metrics` as a data source and ask
-- MAGIC *"What was revenue by month?"*. If you get an answer, Genie works. Delete the test agent afterwards.

-- COMMAND ----------

-- MAGIC %md ### Clean up

-- COMMAND ----------

DROP SCHEMA IF EXISTS retail.smoke CASCADE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Results (copy into docs/RUNBOOK.md)
-- MAGIC
-- MAGIC | Check | Result | Notes |
-- MAGIC |---|---|---|
-- MAGIC | Runtime version | | |
-- MAGIC | CREATE CATALOG | | |
-- MAGIC | Metric view (basic) | | |
-- MAGIC | Metric view (window measure) | | |
-- MAGIC | Groups + GRANT | | |
-- MAGIC | Genie | | |
