-- Databricks notebook source
-- MAGIC %md
-- MAGIC # semantic.returns_metrics
-- MAGIC
-- MAGIC Returns by the date they were **received**: refunds paid out in a period, return reasons, time to return.
-- MAGIC Return *rate* is not here: it needs units sold, so it lives in `sales_metrics` (by order date).

-- COMMAND ----------

CREATE OR REPLACE VIEW retail.semantic.returns_metrics
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: >-
  Returns at return grain, in EUR, dated by when the return was received. Use for refunds paid
  in a period and for return reasons. Includes returns that could not be matched to an order line
  (is_orphan_return), because the money was still refunded. Return rate is in sales_metrics.
source: retail.gold.fact_return
joins:
  - name: product
    source: retail.gold.dim_product
    on: source.product_key = product.product_key
  - name: customer
    source: retail.gold.dim_customer
    on: source.customer_key = customer.customer_key
  - name: return_calendar
    source: retail.gold.dim_date
    on: source.return_date_key = return_calendar.date_key
fields:
  - name: return_date
    expr: source.return_date
    display_name: "Return date"
    comment: "Date the return was received."
  - name: return_month
    expr: return_calendar.month_start
    display_name: "Return month"
  - name: return_year
    expr: return_calendar.year
    display_name: "Return year"
  - name: reason
    expr: source.reason
    display_name: "Return reason"
    comment: "size_or_fit, defective, damaged_in_transit, not_as_described, changed_mind or wrong_item."
    synonyms: ["return reason", "why returned"]
  - name: category
    expr: product.category
    display_name: "Product category"
  - name: brand
    expr: product.brand
    display_name: "Brand"
  - name: membership_at_order
    expr: customer.membership
    display_name: "Membership tier at order time"
  - name: country
    expr: customer.country
    display_name: "Customer country"
  - name: is_orphan_return
    expr: source.is_orphan_return
    display_name: "Unmatched return"
    comment: "True when the return points to an order line that does not exist."
measures:
  - name: returns
    expr: COUNT(DISTINCT source.return_id)
    display_name: "Returns"
    synonyms: ["number of returns", "return count"]
  - name: returned_units
    expr: SUM(source.quantity)
    display_name: "Returned units"
  - name: refunds_paid
    expr: SUM(source.refund_amount)
    display_name: "Refunds paid"
    comment: "Amount refunded for returns received in the period."
    synonyms: ["refunds", "refunded amount", "refund amount", "money refunded"]
  - name: avg_days_to_return
    expr: AVG(source.days_to_return)
    display_name: "Average days to return"
    comment: "Days between order and return, for matched returns."
$$

-- COMMAND ----------

SELECT return_month, reason, MEASURE(returns) AS returns, MEASURE(refunds_paid) AS refunds_paid
FROM retail.semantic.returns_metrics
GROUP BY ALL
ORDER BY return_month, returns DESC
