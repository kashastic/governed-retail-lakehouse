-- Databricks notebook source
-- MAGIC %md
-- MAGIC # semantic.sales_metrics
-- MAGIC
-- MAGIC The governed definitions of the sales KPIs. The dashboard and Genie query these measures instead of
-- MAGIC re-deriving them, so "revenue" means the same thing everywhere. Formulas and edge cases:
-- MAGIC `docs/metric_definitions.md`. Run on the SQL warehouse after the pipeline has built gold.

-- COMMAND ----------

CREATE OR REPLACE VIEW retail.semantic.sales_metrics
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: >-
  Sales KPIs at order-line grain, in EUR. Every period filter is by ORDER date.
  Returns and refunds are attributed to the order they belong to, so the latest ~30 days of
  orders show fewer returns than they will end up with. For refunds by the date the return was
  received, use retail.semantic.returns_metrics.
source: retail.gold.fact_order_line
joins:
  - name: customer
    source: retail.gold.dim_customer
    on: source.customer_key = customer.customer_key
  - name: product
    source: retail.gold.dim_product
    on: source.product_key = product.product_key
  - name: order_calendar
    source: retail.gold.dim_date
    on: source.order_date_key = order_calendar.date_key
fields:
  - name: order_date
    expr: source.order_date
    display_name: "Order date"
    comment: "Date the order was placed."
  - name: order_month
    expr: order_calendar.month_start
    display_name: "Order month"
    comment: "First day of the month the order was placed in."
  - name: order_quarter
    expr: CONCAT(CAST(order_calendar.year AS STRING), '-Q', CAST(order_calendar.quarter AS STRING))
    display_name: "Order quarter"
    comment: "Quarter the order was placed in, e.g. 2026-Q2."
  - name: order_year
    expr: order_calendar.year
    display_name: "Order year"
  - name: order_weekday
    expr: order_calendar.day_name
    display_name: "Order weekday"
  - name: category
    expr: product.category
    display_name: "Product category"
    synonyms: ["product category", "department"]
  - name: brand
    expr: product.brand
    display_name: "Brand"
  - name: product_name
    expr: product.product_name
    display_name: "Product"
  - name: channel
    expr: source.channel
    display_name: "Sales channel"
    comment: "web or app."
    synonyms: ["sales channel", "platform"]
  - name: membership_at_order
    expr: customer.membership
    display_name: "Membership tier at order time"
    comment: "The customer's membership tier when the order was placed, not today's tier."
    synonyms: ["membership", "membership tier", "tier", "customer tier", "loyalty tier"]
  - name: country
    expr: customer.country
    display_name: "Customer country"
    comment: "ISO code of the customer's country when the order was placed: DE, FR, NL, AT or CH."
    synonyms: ["market"]
  - name: customer_id
    expr: source.customer_id
    display_name: "Customer ID"
  - name: is_unknown_product
    expr: source.is_orphan_product
    display_name: "Unknown product"
    comment: "True for order lines whose product is missing from the catalogue (category 'Unknown')."
measures:
  - name: gross_sales
    expr: SUM(source.gross_amount)
    display_name: "Gross sales"
    comment: "Quantity x unit price, before discounts and returns."
    synonyms: ["gross revenue", "GMV", "gross merchandise value"]
  - name: discounts
    expr: SUM(source.discount_amount)
    display_name: "Discounts"
  - name: net_sales
    expr: SUM(source.net_amount)
    display_name: "Net sales"
    comment: "What customers paid: gross sales minus discounts, before returns. Use for 'sales' and 'how much did we sell'."
    synonyms: ["sales", "amount sold", "turnover"]
  - name: refunds_on_orders
    expr: SUM(source.refund_amount)
    display_name: "Refunds on orders"
    comment: "Refunds for orders placed in the period (by order date). Refunds paid in a period are in returns_metrics."
  - name: net_revenue
    expr: SUM(source.net_amount) - SUM(source.refund_amount)
    display_name: "Net revenue"
    comment: "Net sales minus refunds on those orders. This is the company's definition of revenue."
    synonyms: ["revenue", "income"]
  - name: orders
    expr: COUNT(DISTINCT source.order_id)
    display_name: "Orders"
    synonyms: ["number of orders", "order count"]
  - name: aov
    expr: SUM(source.net_amount) / COUNT(DISTINCT source.order_id)
    display_name: "Average order value"
    comment: "Net sales per order, before returns."
    synonyms: ["average order value", "AOV", "average basket value"]
  - name: units_sold
    expr: SUM(source.quantity)
    display_name: "Units sold"
    synonyms: ["units", "items sold", "quantity sold"]
  - name: units_per_order
    expr: SUM(source.quantity) / COUNT(DISTINCT source.order_id)
    display_name: "Units per order"
    comment: "Average number of units in an order. This is what 'basket size' means here."
    synonyms: ["basket size", "average basket size", "items per order"]
  - name: returned_units
    expr: SUM(source.returned_quantity)
    display_name: "Returned units"
  - name: return_rate
    expr: SUM(source.returned_quantity) / SUM(source.quantity)
    display_name: "Return rate"
    comment: "Returned units / units sold, for orders placed in the period. The company's definition of return rate."
    synonyms: ["return rate", "returns rate", "unit return rate"]
  - name: return_rate_value
    expr: SUM(source.refund_amount) / SUM(source.net_amount)
    display_name: "Return rate by value"
    comment: "Refunds / net sales, for orders placed in the period. Only when value is asked for explicitly."
  - name: active_customers
    expr: COUNT(DISTINCT source.customer_id)
    display_name: "Active customers"
    comment: "Distinct customers who placed at least one order in the period."
    synonyms: ["customers", "buyers", "unique customers"]
  - name: active_customers_30d
    expr: COUNT(DISTINCT source.customer_id)
    display_name: "Active customers (trailing 30 days)"
    comment: "Distinct customers with an order in the 30 days up to and including each date."
    window:
      - order: order_date
        range: trailing 30 day inclusive
        semiadditive: last
$$

-- COMMAND ----------

-- Quick look: monthly KPIs.
SELECT order_month,
       MEASURE(net_sales)   AS net_sales,
       MEASURE(net_revenue) AS net_revenue,
       MEASURE(orders)      AS orders,
       MEASURE(aov)         AS aov,
       MEASURE(return_rate) AS return_rate
FROM retail.semantic.sales_metrics
GROUP BY order_month
ORDER BY order_month

-- COMMAND ----------

-- Which days does active_customers_30d cover? Compare it on 2026-08-31 with plain SQL over the candidate
-- windows. Without `inclusive` the window leaves out the day itself (seen in the smoke test).
WITH by_day AS (
  SELECT order_date, MEASURE(active_customers_30d) AS active_customers_30d
  FROM retail.semantic.sales_metrics
  GROUP BY order_date
)
SELECT d.active_customers_30d,
       (SELECT COUNT(DISTINCT customer_id) FROM retail.gold.fact_order_line
         WHERE order_date BETWEEN DATE'2026-08-02' AND DATE'2026-08-31') AS days_minus_29_to_day,
       (SELECT COUNT(DISTINCT customer_id) FROM retail.gold.fact_order_line
         WHERE order_date BETWEEN DATE'2026-08-01' AND DATE'2026-08-31') AS days_minus_30_to_day,
       (SELECT COUNT(DISTINCT customer_id) FROM retail.gold.fact_order_line
         WHERE order_date BETWEEN DATE'2026-08-01' AND DATE'2026-08-30') AS days_minus_30_to_minus_1
FROM by_day d
WHERE d.order_date = DATE'2026-08-31'
