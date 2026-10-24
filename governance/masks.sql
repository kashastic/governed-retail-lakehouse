-- Databricks notebook source
-- MAGIC %md
-- MAGIC # PII: keep identifiers out of gold, mask them where they are needed
-- MAGIC
-- MAGIC - `gold.dim_customer` has no name or email at all (data minimisation, done in the pipeline).
-- MAGIC - Engineers still have the raw values in `silver.customers`, which analysts can't read.
-- MAGIC - For the rare analyst need (e.g. a contact list for a campaign), `gold.customer_contact` exposes the current
-- MAGIC   customer with the email passed through a masking function: engineers see it, everyone else sees `***@domain`.
-- MAGIC
-- MAGIC The view runs with its owner's rights, so analysts don't need access to silver. This is a masking *view*.
-- MAGIC Unity Catalog column masks (`ALTER TABLE … SET MASK`) can't be added to pipeline-owned streaming tables from
-- MAGIC outside the pipeline.

-- COMMAND ----------

CREATE OR REPLACE FUNCTION retail.governance.mask_email(email STRING)
RETURNS STRING
COMMENT 'Shows the email to retail_engineers; everyone else sees ***@domain.'
RETURN CASE
  WHEN is_account_group_member('retail_engineers') THEN email
  ELSE regexp_replace(email, '^[^@]+', '***')
END;

-- COMMAND ----------

CREATE OR REPLACE VIEW retail.gold.customer_contact
COMMENT 'Current customers with masked email. Use only for approved contact purposes.'
AS
SELECT customer_id,
       retail.governance.mask_email(email) AS email,
       city,
       country,
       membership
FROM retail.silver.customers
WHERE __END_AT IS NULL;

-- COMMAND ----------

-- As an engineer you see real emails; as an analyst you see ***@example.com.
SELECT * FROM retail.gold.customer_contact LIMIT 5
