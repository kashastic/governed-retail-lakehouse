# Metric definitions

Defined once in `semantic/sales_metrics.sql` and `semantic/returns_metrics.sql`. The dashboard and Genie use
these measures; nothing re-derives them. Values for August 2026 are the expected results on this dataset
(see `genie/expected_answers.csv`).

## Sales (`semantic.sales_metrics`, by order date)

| Measure | Formula | Notes |
|---|---|---|
| `gross_sales` | `SUM(quantity * unit_price)` | Before discounts and returns. |
| `discounts` | `SUM(discount_amount)` | |
| `net_sales` | `SUM(gross - discount)` | What customers paid. **"Sales", "how much did we sell".** Aug 2026: €4,058,704.60 |
| `refunds_on_orders` | `SUM(refund_amount)` | Refunds belonging to orders placed in the period. |
| `net_revenue` | `net_sales - refunds_on_orders` | **"Revenue".** Moves after the period closes, as returns come in. |
| `orders` | `COUNT(DISTINCT order_id)` | The fact is one row per line, so never `COUNT(*)`. |
| `aov` | `net_sales / orders` | Net sales per order, **before** returns: the basket at checkout. Aug 2026: €312.30 |
| `units_sold` | `SUM(quantity)` | |
| `units_per_order` | `units_sold / orders` | **"Basket size".** |
| `returned_units` | `SUM(returned_quantity)` | |
| `return_rate` | `returned_units / units_sold` | **"Return rate".** Units, for orders placed in the period. |
| `return_rate_value` | `refunds_on_orders / net_sales` | Only when value is asked for explicitly. |
| `active_customers` | `COUNT(DISTINCT customer_id)` | Customers with at least one order in the period. |
| `active_customers_30d` | same, over the 30 days up to and including each date (`trailing 30 day inclusive`), semi-additive (last) | For a daily trend line. 2026-08-31: 7,326 |

## Returns (`semantic.returns_metrics`, by return date)

| Measure | Formula | Notes |
|---|---|---|
| `returns` | `COUNT(DISTINCT return_id)` | |
| `returned_units` | `SUM(quantity)` | |
| `refunds_paid` | `SUM(refund_amount)` | **"Refunds in <period>".** Includes unmatched (orphan) returns: the money left the company. |
| `avg_days_to_return` | `AVG(days_to_return)` | Matched returns only. |

## Decisions and edge cases

**Why return rate is by order date (cohort), not by return date.** Units returned in July mostly belong to June and
July orders. Dividing them by July sales mixes cohorts and swings with sales volume. Per order cohort, the rate
answers "of what we sold in July, how much came back?" (B18: 8.21% by order date vs 8.39% the other way).

**The cost of cohort attribution: immature periods.** Returns arrive up to 30 days after the order. The latest
month's return rate is too low and its net revenue too high until the window closes. The dashboard marks months whose
window is still open (`ds_return_cohort_maturity`) instead of hiding them.

**Why AOV is before returns.** AOV describes the basket at checkout and is used for pricing and promotions. A
post-return AOV would mix in a lagging, separately tracked metric. Revenue after returns is `net_revenue`.

**Why units and not value for the main return rate.** Unit return rate is comparable across categories with very
different prices. Electronics' few returns are expensive, so a value-based rate would mostly measure price.
`return_rate_value` is there when finance asks for it.

**Membership and country are "at order time".** They come from the customer version valid when the order was
placed (SCD2), so an upgrade in September doesn't rewrite August's Premium sales.

**Ratios are never averaged.** Every ratio is a ratio of sums computed by `MEASURE()` at the grain asked for.
Averaging monthly AOVs gives a different and wrong number.

**Which days the 30-day window covers.** Without `inclusive`, a trailing window leaves out the date itself (seen in
the smoke test), so "active in the last 30 days" on a date would ignore that day's buyers. The last cell of
`semantic/sales_metrics.sql` checks it: on 2026-08-31 the measure returns 7,326, equal to plain SQL over
08-02..08-31, not 7,510 (08-01..08-31) or 7,382 (08-01..08-30).

## Verified on Databricks (2026-10-25)

`notebooks/04_validate.py` compares each core measure with plain SQL on the gold tables, written separately from
the metric view, for all time and for August 2026. After all 15 batches, all 12 checks are equal:

| Measure | All time | Aug 2026 |
|---|---|---|
| `net_sales` | €50,889,830.02 | €4,058,704.60 |
| `net_revenue` | €46,623,902.37 | €3,705,710.34 |
| `orders` | 158,960 | 12,996 |
| `aov` | €320.14 | €312.30 |
| `return_rate` | 8.03% | 7.60% |
| `active_customers` | 17,560 | 7,510 |
