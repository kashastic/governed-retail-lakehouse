"""The fixed 25-question benchmark for the Genie agent.

- split: 'dev' questions may be used to tune instructions; 'holdout' questions
  are never looked at while tuning, so they measure whether fixes generalise.
- sql: ground truth written against the gold tables, NOT the metric views, so
  a correct Genie answer through the metric views also cross-checks them.
- trap_sql: the answer a plausible wrong reading gives. tests/test_benchmarks.py
  checks that it differs from the ground truth, so the question really tests
  the definition or join it is meant to test.

Absolute dates only: the data ends on 2026-09-14, so "last month" would drift.
Run `python -m genie.export` to regenerate benchmarks.csv and expected_answers.csv.
"""

F = "retail.gold.fact_order_line"
R = "retail.gold.fact_return"
C = "retail.gold.dim_customer"
P = "retail.gold.dim_product"


def between(col: str, start: str, end: str) -> str:
    return f"{col} BETWEEN DATE'{start}' AND DATE'{end}'"


BENCHMARKS = [
    # ------------------------------------------------------------------ simple KPIs
    dict(id="B01", split="dev", category="kpi",
         question="What was net revenue in July 2026?",
         sql=f"SELECT SUM(net_amount) - SUM(refund_amount) AS net_revenue FROM {F} "
             f"WHERE {between('order_date', '2026-07-01', '2026-07-31')}",
         hard="Net revenue subtracts refunds on those orders, not just discounts."),
    dict(id="B02", split="dev", category="kpi",
         question="How many orders were placed in Q2 2026?",
         sql=f"SELECT COUNT(DISTINCT order_id) AS orders FROM {F} "
             f"WHERE {between('order_date', '2026-04-01', '2026-06-30')}",
         hard="The fact table is at order-line grain: counting rows counts lines, not orders.",
         trap_sql=f"SELECT COUNT(*) FROM {F} WHERE {between('order_date', '2026-04-01', '2026-06-30')}"),
    dict(id="B03", split="holdout", category="kpi",
         question="What was the average order value in August 2026?",
         sql=f"SELECT SUM(net_amount) / COUNT(DISTINCT order_id) AS aov FROM {F} "
             f"WHERE {between('order_date', '2026-08-01', '2026-08-31')}",
         hard="AOV is net sales per distinct order; averaging net_amount averages lines.",
         trap_sql=f"SELECT AVG(net_amount) FROM {F} WHERE {between('order_date', '2026-08-01', '2026-08-31')}"),
    dict(id="B04", split="dev", category="kpi",
         question="How many units did we sell in March 2026?",
         sql=f"SELECT SUM(quantity) AS units_sold FROM {F} "
             f"WHERE {between('order_date', '2026-03-01', '2026-03-31')}",
         hard="Units are summed quantity, not a count of lines."),
    dict(id="B05", split="holdout", category="kpi",
         question="What were gross sales from September 2025 through August 2026?",
         sql=f"SELECT SUM(gross_amount) AS gross_sales FROM {F} "
             f"WHERE {between('order_date', '2025-09-01', '2026-08-31')}",
         hard="Gross is before discounts; a twelve-month range that crosses a year boundary."),
    dict(id="B06", split="dev", category="kpi",
         question="How many customers placed at least one order in June 2026?",
         sql=f"SELECT COUNT(DISTINCT customer_id) AS active_customers FROM {F} "
             f"WHERE {between('order_date', '2026-06-01', '2026-06-30')}",
         hard="Distinct customers who ordered, not all customers in the dimension.",
         trap_sql=f"SELECT COUNT(DISTINCT customer_id) FROM {C} WHERE customer_key <> -1"),

    # ------------------------------------------------------------------ breakdowns and time
    dict(id="B07", split="dev", category="breakdown",
         question="Show net sales by product category for Q1 2026, highest first.",
         sql=f"SELECT p.category, SUM(f.net_amount) AS net_sales FROM {F} f JOIN {P} p ON f.product_key = p.product_key "
             f"WHERE {between('f.order_date', '2026-01-01', '2026-03-31')} GROUP BY p.category ORDER BY net_sales DESC",
         hard="Includes the 'Unknown' category: joining on the raw product id would silently drop those sales.",
         trap_sql=f"SELECT p.category, SUM(f.net_amount) AS net_sales FROM {F} f JOIN {P} p ON f.source_product_id = p.product_id "
                  f"WHERE {between('f.order_date', '2026-01-01', '2026-03-31')} GROUP BY p.category ORDER BY net_sales DESC"),
    dict(id="B08", split="holdout", category="breakdown",
         question="How many orders were placed in each month from September 2025 to August 2026?",
         sql=f"SELECT trunc(order_date, 'MM') AS order_month, COUNT(DISTINCT order_id) AS orders FROM {F} "
             f"WHERE {between('order_date', '2025-09-01', '2026-08-31')} GROUP BY 1 ORDER BY 1",
         hard="Twelve rows; order count per month must be distinct orders."),
    dict(id="B09", split="dev", category="breakdown",
         question="Which 5 brands had the highest net sales in 2026?",
         sql=f"SELECT p.brand, SUM(f.net_amount) AS net_sales FROM {F} f JOIN {P} p ON f.product_key = p.product_key "
             f"WHERE {between('f.order_date', '2026-01-01', '2026-12-31')} GROUP BY p.brand ORDER BY net_sales DESC LIMIT 5",
         hard="Some brands sell in two categories; grouping by brand and category splits them."),
    dict(id="B10", split="holdout", category="breakdown",
         question="What were net sales by sales channel in May 2026?",
         sql=f"SELECT channel, SUM(net_amount) AS net_sales FROM {F} "
             f"WHERE {between('order_date', '2026-05-01', '2026-05-31')} GROUP BY channel ORDER BY net_sales DESC",
         hard="Straight breakdown on a fact attribute."),
    dict(id="B11", split="dev", category="breakdown",
         question="Which country had the highest average order value in Q2 2026?",
         sql=f"SELECT c.country, SUM(f.net_amount) / COUNT(DISTINCT f.order_id) AS aov FROM {F} f "
             f"JOIN {C} c ON f.customer_key = c.customer_key "
             f"WHERE {between('f.order_date', '2026-04-01', '2026-06-30')} GROUP BY c.country ORDER BY aov DESC LIMIT 1",
         hard="Country comes from the customer version at order time; AOV per distinct order."),

    # ------------------------------------------------------------------ returns
    dict(id="B12", split="dev", category="returns",
         question="What was the return rate for Clothing for orders placed in May 2026?",
         sql=f"SELECT SUM(f.returned_quantity) / SUM(f.quantity) AS return_rate FROM {F} f "
             f"JOIN {P} p ON f.product_key = p.product_key "
             f"WHERE p.category = 'Clothing' AND {between('f.order_date', '2026-05-01', '2026-05-31')}",
         hard="Unit-based return rate on the May order cohort."),
    dict(id="B13", split="holdout", category="returns",
         question="Show the return rate by product category for orders placed in Q1 2026.",
         sql=f"SELECT p.category, SUM(f.returned_quantity) / SUM(f.quantity) AS return_rate FROM {F} f "
             f"JOIN {P} p ON f.product_key = p.product_key "
             f"WHERE {between('f.order_date', '2026-01-01', '2026-03-31')} GROUP BY p.category ORDER BY return_rate DESC",
         hard="Ratio of sums per category, not an average of per-line ratios."),
    dict(id="B14", split="dev", category="returns",
         question="What were the top 3 return reasons by returned units for returns received in 2026?",
         sql=f"SELECT reason, SUM(quantity) AS returned_units FROM {R} "
             f"WHERE {between('return_date', '2026-01-01', '2026-12-31')} GROUP BY reason ORDER BY returned_units DESC LIMIT 3",
         hard="Dated by when the return was received (fact_return), ranked by units not by count."),
    dict(id="B15", split="holdout", category="returns",
         question="How much money was refunded for returns received in August 2026?",
         sql=f"SELECT SUM(refund_amount) AS refunds_paid FROM {R} "
             f"WHERE {between('return_date', '2026-08-01', '2026-08-31')}",
         hard="By return date, from fact_return. The order-date refunds on fact_order_line give a different number.",
         trap_sql=f"SELECT SUM(refund_amount) FROM {F} WHERE {between('order_date', '2026-08-01', '2026-08-31')}"),
    dict(id="B16", split="holdout", category="returns",
         question="Which product category had the highest return rate for orders placed in the first half of 2026?",
         sql=f"SELECT p.category, SUM(f.returned_quantity) / SUM(f.quantity) AS return_rate FROM {F} f "
             f"JOIN {P} p ON f.product_key = p.product_key "
             f"WHERE {between('f.order_date', '2026-01-01', '2026-06-30')} GROUP BY p.category ORDER BY return_rate DESC LIMIT 1",
         hard="'First half' has to become January to June."),

    # ------------------------------------------------------------------ ambiguous / definition-sensitive
    dict(id="B17", split="dev", category="ambiguous",
         question="How much did we sell in August 2026?",
         sql=f"SELECT SUM(net_amount) AS net_sales FROM {F} WHERE {between('order_date', '2026-08-01', '2026-08-31')}",
         hard="'Sell' = net sales (after discounts, before returns). Gross sales, net revenue or units are other readings.",
         trap_sql=f"SELECT SUM(gross_amount) FROM {F} WHERE {between('order_date', '2026-08-01', '2026-08-31')}"),
    dict(id="B18", split="dev", category="ambiguous",
         question="What was the return rate in July 2026?",
         sql=f"SELECT SUM(returned_quantity) / SUM(quantity) AS return_rate FROM {F} "
             f"WHERE {between('order_date', '2026-07-01', '2026-07-31')}",
         hard="Return rate is by order date (July orders and their returns). Dividing returns received in July "
              "by July sales mixes cohorts.",
         trap_sql=f"SELECT (SELECT SUM(quantity) FROM {R} WHERE NOT is_orphan_return AND "
                  f"{between('return_date', '2026-07-01', '2026-07-31')}) / (SELECT SUM(quantity) FROM {F} "
                  f"WHERE {between('order_date', '2026-07-01', '2026-07-31')})"),
    dict(id="B19", split="holdout", category="ambiguous",
         question="What was our revenue in Q2 2026?",
         sql=f"SELECT SUM(net_amount) - SUM(refund_amount) AS net_revenue FROM {F} "
             f"WHERE {between('order_date', '2026-04-01', '2026-06-30')}",
         hard="'Revenue' = net revenue (after refunds), the company definition. Net sales is the common wrong answer.",
         trap_sql=f"SELECT SUM(net_amount) FROM {F} WHERE {between('order_date', '2026-04-01', '2026-06-30')}"),
    dict(id="B20", split="dev", category="ambiguous",
         question="Who were our top 10 customers in 2026?",
         sql=f"SELECT customer_id, SUM(net_amount) - SUM(refund_amount) AS net_revenue FROM {F} "
             f"WHERE {between('order_date', '2026-01-01', '2026-12-31')} GROUP BY customer_id "
             f"ORDER BY net_revenue DESC LIMIT 10",
         hard="'Top' needs a measure: ranked by net revenue. Order count or gross sales rank differently.",
         trap_sql=f"SELECT customer_id, COUNT(DISTINCT order_id) AS orders FROM {F} "
                  f"WHERE {between('order_date', '2026-01-01', '2026-12-31')} GROUP BY customer_id "
                  f"ORDER BY orders DESC, customer_id LIMIT 10"),
    dict(id="B21", split="holdout", category="ambiguous",
         question="What was the average basket size in July 2026?",
         sql=f"SELECT SUM(quantity) / COUNT(DISTINCT order_id) AS units_per_order FROM {F} "
             f"WHERE {between('order_date', '2026-07-01', '2026-07-31')}",
         hard="'Basket size' = units per order here; it is often confused with average order value.",
         trap_sql=f"SELECT SUM(net_amount) / COUNT(DISTINCT order_id) FROM {F} "
                  f"WHERE {between('order_date', '2026-07-01', '2026-07-31')}"),

    # ------------------------------------------------------------------ join-sensitive
    dict(id="B22", split="dev", category="join",
         question="What were net sales in August 2026 by customer membership tier?",
         sql=f"SELECT c.membership, SUM(f.net_amount) AS net_sales FROM {F} f JOIN {C} c ON f.customer_key = c.customer_key "
             f"WHERE {between('f.order_date', '2026-08-01', '2026-08-31')} GROUP BY c.membership ORDER BY net_sales DESC",
         hard="Tier at order time (SCD2 via customer_key). Joining on customer_id matches every version and double-counts.",
         trap_sql=f"SELECT c.membership, SUM(f.net_amount) AS net_sales FROM {F} f JOIN {C} c ON f.customer_id = c.customer_id "
                  f"WHERE {between('f.order_date', '2026-08-01', '2026-08-31')} GROUP BY c.membership ORDER BY net_sales DESC"),
    dict(id="B23", split="dev", category="join",
         question="How many orders did Premium members place in Q2 2026?",
         sql=f"SELECT COUNT(DISTINCT f.order_id) AS orders FROM {F} f JOIN {C} c ON f.customer_key = c.customer_key "
             f"WHERE c.membership = 'Premium' AND {between('f.order_date', '2026-04-01', '2026-06-30')}",
         hard="Premium at the time of the order; today's tier gives a different count.",
         trap_sql=f"SELECT COUNT(DISTINCT f.order_id) FROM {F} f JOIN {C} c ON f.customer_id = c.customer_id AND c.is_current "
                  f"WHERE c.membership = 'Premium' AND {between('f.order_date', '2026-04-01', '2026-06-30')}"),
    dict(id="B24", split="holdout", category="join",
         question="What was the return rate for orders from customers in Germany in Q1 2026?",
         sql=f"SELECT SUM(f.returned_quantity) / SUM(f.quantity) AS return_rate FROM {F} f "
             f"JOIN {C} c ON f.customer_key = c.customer_key "
             f"WHERE c.country = 'DE' AND {between('f.order_date', '2026-01-01', '2026-03-31')}",
         hard="'Germany' is stored as 'DE'; country at order time.",
         trap_sql=f"SELECT SUM(f.returned_quantity) / SUM(f.quantity) FROM {F} f "
                  f"JOIN {C} c ON f.customer_id = c.customer_id "
                  f"WHERE c.country = 'DE' AND {between('f.order_date', '2026-01-01', '2026-03-31')}"),
    dict(id="B25", split="dev", category="join",
         question="What share of net sales in 2026 came from products that are missing from the product catalogue?",
         sql=f"SELECT SUM(CASE WHEN is_orphan_product THEN net_amount ELSE 0 END) / SUM(net_amount) AS unknown_product_share "
             f"FROM {F} WHERE {between('order_date', '2026-01-01', '2026-12-31')}",
         hard="Needs is_orphan_product or the 'Unknown' category; an inner join on the raw product id hides these sales.",
         trap_sql=f"SELECT SUM(CASE WHEN p.product_id IS NULL THEN f.net_amount ELSE 0 END) / SUM(f.net_amount) "
                  f"FROM {F} f JOIN {P} p ON f.source_product_id = p.product_id "
                  f"WHERE {between('f.order_date', '2026-01-01', '2026-12-31')}"),
]
