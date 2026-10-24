-- Datasets for the "Retail KPIs" AI/BI dashboard. See dashboards/BUILD.md for the widgets.
--
-- Preferred: add the metric views themselves as datasets (Data tab → Add data source → pick the
-- metric view). Widgets then compute every measure at their own grain, so ratios like AOV and
-- return rate are always correct. The SQL below is the fallback if a widget needs a custom query.
-- Do not average a pre-computed ratio across rows: re-aggregate with MEASURE() at the widget's grain.

-- ds_sales: metric view as a dataset (preferred)
--   retail.semantic.sales_metrics

-- ds_returns: metric view as a dataset (preferred)
--   retail.semantic.returns_metrics

-- ds_sales_monthly_by_category (fallback for the trend and category widgets)
SELECT order_month,
       category,
       MEASURE(net_sales)       AS net_sales,
       MEASURE(net_revenue)     AS net_revenue,
       MEASURE(orders)          AS orders,
       MEASURE(aov)             AS aov,
       MEASURE(units_sold)      AS units_sold,
       MEASURE(returned_units)  AS returned_units,
       MEASURE(return_rate)     AS return_rate
FROM retail.semantic.sales_metrics
GROUP BY ALL;

-- ds_return_cohort_maturity: how complete each order month's returns are.
-- Returns arrive up to 30 days after the order, so the latest month's return rate is still rising.
SELECT order_month,
       MEASURE(return_rate)  AS return_rate,
       datediff(DATE'2026-09-14', last_day(order_month)) AS days_since_month_end,
       datediff(DATE'2026-09-14', last_day(order_month)) >= 30 AS return_window_closed
FROM retail.semantic.sales_metrics
GROUP BY order_month;

-- ds_refunds_by_reason (returns received, by return month)
SELECT return_month,
       reason,
       MEASURE(returns)      AS returns,
       MEASURE(refunds_paid) AS refunds_paid
FROM retail.semantic.returns_metrics
GROUP BY ALL;

-- ds_data_issues (data-quality page)
SELECT feed, batch_id, issue, handling, rows
FROM retail.dq.issue_summary;
