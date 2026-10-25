-- Datasets of the "Retail KPIs" dashboard (dashboards/retail_kpis.lvdash.json). See dashboards/BUILD.md.
--
-- ds_sales and ds_returns are the metric views themselves, not SQL: widgets compute every measure at
-- their own grain, so ratios like AOV and return rate are always correct. Only the two datasets below
-- are SQL. Never average a pre-computed ratio across rows; re-aggregate with MEASURE().

-- ds_sales:   retail.semantic.sales_metrics
-- ds_returns: retail.semantic.returns_metrics

-- ds_return_cohort_maturity: how complete each order month's returns are.
-- Returns arrive up to 30 days after the order, so the latest month's return rate is still rising.
-- 2026-09-14 is the last day of data.
SELECT order_month,
       MEASURE(return_rate) AS return_rate,
       CASE WHEN datediff(DATE'2026-09-14', last_day(order_month)) >= 30
            THEN 'closed' ELSE 'still open' END AS return_window
FROM retail.semantic.sales_metrics
GROUP BY order_month;

-- ds_data_issues (data-quality page)
SELECT feed, batch_id, issue, handling, `rows`
FROM retail.dq.issue_summary;
