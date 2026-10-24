# Building the "Retail KPIs" dashboard

AI/BI dashboards are built in the UI. Build it once from this checklist (about an hour), then export it to
`dashboards/retail_kpis.lvdash.json` so it is versioned with the code.

**New → Dashboard.** Name it `Retail KPIs`. In the **Data** tab add:

| Dataset | Source |
|---|---|
| `ds_sales` | metric view `retail.semantic.sales_metrics` |
| `ds_returns` | metric view `retail.semantic.returns_metrics` |
| `ds_return_cohort_maturity` | SQL from `datasets.sql` |
| `ds_data_issues` | SQL from `datasets.sql` |

Use the metric views directly wherever possible. Each widget then asks for `MEASURE(aov)` at its own grain,
and no ratio is ever averaged across rows.

## Page 1: Overview

| Widget | Type | Dataset | Fields |
|---|---|---|---|
| Date range | Filter (date range) | `ds_sales` | `order_date`. Apply to all widgets on the page. |
| Category, Channel, Country | Filters (multi-select) | `ds_sales` | `category`, `channel`, `country` |
| Net revenue | Counter | `ds_sales` | `net_revenue` |
| Net sales | Counter | `ds_sales` | `net_sales` |
| Orders | Counter | `ds_sales` | `orders` |
| Average order value | Counter | `ds_sales` | `aov` |
| Return rate | Counter | `ds_sales` | `return_rate` (format as %) |
| Active customers | Counter | `ds_sales` | `active_customers` |
| Sales trend | Line | `ds_sales` | x `order_month`, y `net_sales` and `net_revenue` |
| Sales by category | Bar | `ds_sales` | x `net_sales`, y `category`, sorted descending |
| Active customers, trailing 30 days | Line | `ds_sales` | x `order_date`, y `active_customers_30d` |

## Page 2: Returns

| Widget | Type | Dataset | Fields |
|---|---|---|---|
| Return rate by category | Bar | `ds_sales` | x `return_rate`, y `category` |
| Return rate by order month | Line | `ds_return_cohort_maturity` | x `order_month`, y `return_rate`, colour `return_window_closed` |
| Cohort note | Text | | "Return rate is by order date. Returns arrive up to 30 days after the order, so months whose return window is still open (orange) will rise." |
| Returns by reason | Stacked bar | `ds_returns` | x `return_month`, y `returns`, colour `reason` |
| Refunds paid | Counter | `ds_returns` | `refunds_paid` |
| Days to return by category | Bar | `ds_returns` | x `avg_days_to_return`, y `category` |

## Page 3: Data quality

| Widget | Type | Dataset | Fields |
|---|---|---|---|
| Issues by type | Bar | `ds_data_issues` | x `rows` (sum), y `issue`, colour `handling` |
| Issues by batch | Stacked bar | `ds_data_issues` | x `batch_id`, y `rows` (sum), colour `issue` |
| Detail | Table | `ds_data_issues` | all columns |
| Note | Text | | "Quarantined rows are kept in silver.*_quarantine with the rules they broke. Orphan rows are kept in gold under the Unknown product/order and flagged. Rules and actions: docs/dq_rules.md." |

## Finish

1. **Publish** with *embed credentials* off, so each viewer only sees data they are granted.
2. Share with `retail_analysts` (Can view).
3. Take a screenshot of each page into `screenshots/` (`dashboard_overview.png`, `dashboard_returns.png`,
   `dashboard_dq.png`).
4. Export: dashboard menu (⋮) → **Export** → save as `dashboards/retail_kpis.lvdash.json` in the repo.
