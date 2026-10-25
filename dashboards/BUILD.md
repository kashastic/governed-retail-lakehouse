# The "Retail KPIs" dashboard

The dashboard is versioned as code: `dashboards/retail_kpis.lvdash.json`. Databricks opens any `*.lvdash.json`
file in a Git folder as an AI/BI dashboard, so after a pull it appears as `retail_kpis` in this folder.
`tests/test_dashboard.py` checks that every widget uses a dataset, measure and field that exists.

## Datasets

| Dataset | Source |
|---|---|
| `ds_sales` | metric view `retail.semantic.sales_metrics` |
| `ds_returns` | metric view `retail.semantic.returns_metrics` |
| `ds_return_cohort_maturity` | SQL from `datasets.sql` |
| `ds_data_issues` | SQL from `datasets.sql` |
| `ds_data_issues_daily` | SQL from `datasets.sql` (backfill excluded) |

The metric views are datasets themselves. Each widget asks for `MEASURE(aov)` at its own grain, so no ratio is
ever averaged across rows.

## Page 1: Overview

| Widget | Type | Dataset | Fields |
|---|---|---|---|
| Order date | Filter (date range) | `ds_sales` | `order_date` |
| Product category, Sales channel, Customer country | Filters (multi-select) | `ds_sales` | `category`, `channel`, `country` |
| Net revenue, Net sales, Average order value | Counters | `ds_sales` | `net_revenue`, `net_sales`, `aov` (EUR) |
| Orders, Active customers | Counters | `ds_sales` | `orders`, `active_customers` |
| Return rate | Counter | `ds_sales` | `return_rate` (as %) |
| Net sales and net revenue by order month | Line | `ds_sales` | x `order_month`, y `net_sales` and `net_revenue` |
| Net sales by category | Bar | `ds_sales` | x `net_sales`, y `category`, sorted descending |
| Active customers, trailing 30 days | Line | `ds_sales` | x `order_date`, y `active_customers_30d` |

## Page 2: Returns

| Widget | Type | Dataset | Fields |
|---|---|---|---|
| Refunds paid | Counter | `ds_returns` | `refunds_paid` |
| Cohort note | Text | | Return rate is by order date; months whose return window is still open will rise. |
| Return rate by order month | Bar | `ds_return_cohort_maturity` | x `order_month`, y `return_rate`, colour `return_window` (closed / still open) |
| Return rate by category | Bar | `ds_sales` | x `return_rate`, y `category` |
| Returns received by month and reason | Stacked bar | `ds_returns` | x `return_month`, y `returns`, colour `reason` |
| Average days to return by category | Bar | `ds_returns` | x `avg_days_to_return`, y `category` |

## Page 3: Data quality

| Widget | Type | Dataset | Fields |
|---|---|---|---|
| Note | Text | | Where quarantined, orphan and duplicate rows go. |
| Issues by type | Bar | `ds_data_issues` | x `rows` (sum), y `issue`, colour `handling` |
| Issues per daily batch | Stacked bar | `ds_data_issues_daily` | x `batch_id`, y `rows` (sum), colour `issue`. The backfill's ~4,000 issues would flatten the daily batches. |
| Detail | Table | `ds_data_issues` | all columns |

## Verified on Databricks (2026-10-25)

All three pages render from the Git folder. Unfiltered, the counters equal `04_validate`'s all-time numbers
(€46.62M net revenue, €320.14 AOV, 8.03% return rate). With Order date set to August 2026 they show €4.06M net
sales, €312.3 AOV, 7.6% return rate and 7.51K active customers, equal to the August checks. So the ratios are
recomputed by the metric view for the filtered period, not averaged. On the data-quality page, batch 7 has the
most issues because it re-sends batch 6's orders file, whose bad rows are quarantined again.

## Changing it

Either edit the JSON and run `pytest tests/test_dashboard.py`, or change the dashboard in the UI and export it
(⋮ → **Export**) over `dashboards/retail_kpis.lvdash.json`. Commit either way.

## Finish

1. **Publish** with *embed credentials* off, so each viewer only sees data they are granted.
2. Share with `retail_analysts` (Can view).
3. Take a screenshot of each page into `screenshots/` (`dashboard_overview.png`, `dashboard_returns.png`,
   `dashboard_dq.png`).
