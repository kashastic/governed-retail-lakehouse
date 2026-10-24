# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Validate the pipeline against ground truth
# MAGIC
# MAGIC The generator records every defect it injects in `_manifest/<batch_id>.json`. This notebook checks that the
# MAGIC pipeline caught exactly those, and that the metric views agree with plain SQL on the gold tables.
# MAGIC Only batches the pipeline has already loaded are counted. The same checks run locally in `tests/test_end_to_end.py`.
# MAGIC
# MAGIC Run after a pipeline update. Everything should be `PASS`. Screenshot the result table for the README.

# COMMAND ----------

dbutils.widgets.text("landing_root", "/Volumes/retail/raw/landing")

# COMMAND ----------

import glob
import json
from collections import Counter

landing_root = dbutils.widgets.get("landing_root")
loaded = {r.batch_id for r in spark.sql("""
    SELECT DISTINCT regexp_extract(_source_file, '/(batch_[0-9]{3})/', 1) AS batch_id
    FROM retail.bronze.orders_raw""").collect()}
manifests = [json.load(open(p)) for p in sorted(glob.glob(f"{landing_root}/_manifest/batch_*.json"))]
manifests = [m for m in manifests if m["batch_id"] in loaded]
print("Batches checked:", [m["batch_id"] for m in manifests])


def total(feed, field):
    out = Counter()
    for m in manifests:
        value = m[feed][field]
        out.update(value if isinstance(value, dict) else {field: value})
    return out


def scalar(sql):
    return spark.sql(sql).collect()[0][0]


def quarantined(table):
    rows = spark.sql(f"SELECT rule, count(*) AS n FROM (SELECT explode(failed_rules) AS rule FROM {table}) GROUP BY rule")
    return Counter({r.rule: r.n for r in rows.collect()})


results = []


def check(name, expected, actual, tolerance=None):
    ok = abs(float(expected) - float(actual)) <= tolerance if tolerance is not None else expected == actual
    results.append((name, str(expected), str(actual), "PASS" if ok else "FAIL"))

# COMMAND ----------

# MAGIC %md ### Data quality vs the manifest

# COMMAND ----------

line_failures = +total("orders", "rule_failures")
return_failures = +total("returns", "rule_failures")
check("Order lines quarantined, per rule", dict(sorted(line_failures.items())),
      dict(sorted(quarantined("retail.silver.order_lines_quarantine").items())))
check("Returns quarantined, per rule", dict(sorted(return_failures.items())),
      dict(sorted(quarantined("retail.silver.returns_quarantine").items())))

arrived = total("orders", "order_lines_arrived")["order_lines_arrived"]
duplicates = total("orders", "duplicate_lines")["duplicate_lines"]
check("Silver order lines = arrived - quarantined - duplicates",
      arrived - sum(line_failures.values()) - duplicates, scalar("SELECT count(*) FROM retail.silver.order_lines"))

r_arrived = total("returns", "records_arrived")["records_arrived"]
r_dupes = total("returns", "duplicate_records")["duplicate_records"]
check("Silver returns = arrived - quarantined - duplicates",
      r_arrived - sum(return_failures.values()) - r_dupes, scalar("SELECT count(*) FROM retail.silver.returns"))

check("Orphan product lines kept and flagged", total("orders", "orphan_product_lines")["orphan_product_lines"],
      scalar("SELECT count(*) FROM retail.gold.fact_order_line WHERE is_orphan_product"))
check("Orphan returns kept and flagged", total("returns", "orphan_returns")["orphan_returns"],
      scalar("SELECT count(*) FROM retail.gold.fact_return WHERE is_orphan_return"))

# COMMAND ----------

# MAGIC %md ### Gold integrity

# COMMAND ----------

for table in ("fact_order_line_key_check", "fact_return_key_check", "dim_customer_key_check"):
    check(f"No duplicate keys ({table})", 0, scalar(f"SELECT duplicate_keys FROM retail.dq.{table}"))
check("Net sales preserved silver -> gold",
      scalar("SELECT sum(quantity * unit_price - discount_amount) FROM retail.silver.order_lines"),
      scalar("SELECT sum(net_amount) FROM retail.gold.fact_order_line"))
check("Every order line found its customer version", 0,
      scalar("SELECT count(*) FROM retail.gold.fact_order_line WHERE customer_key = -1"))

# COMMAND ----------

# MAGIC %md ### Metric views vs plain SQL on gold
# MAGIC Skipped until `semantic/sales_metrics.sql` has been run.

# COMMAND ----------

KPI_CHECKS = {
    "net_sales": "SUM(net_amount)",
    "net_revenue": "SUM(net_amount) - SUM(refund_amount)",
    "orders": "COUNT(DISTINCT order_id)",
    "aov": "SUM(net_amount) / COUNT(DISTINCT order_id)",
    "return_rate": "SUM(returned_quantity) / SUM(quantity)",
    "active_customers": "COUNT(DISTINCT customer_id)",
}
if spark.catalog.tableExists("retail.semantic.sales_metrics"):
    for measure, expr in KPI_CHECKS.items():
        for label, mv_filter, gold_filter in [
            ("all time", "", ""),
            ("Aug 2026", "WHERE order_month = DATE'2026-08-01'",
             "WHERE order_date >= DATE'2026-08-01' AND order_date < DATE'2026-09-01'"),
        ]:
            mv = scalar(f"SELECT MEASURE({measure}) FROM retail.semantic.sales_metrics {mv_filter}")
            gold = scalar(f"SELECT {expr} FROM retail.gold.fact_order_line {gold_filter}")
            check(f"Metric view = gold SQL: {measure}, {label}", gold, mv, tolerance=1e-6)
else:
    print("retail.semantic.sales_metrics not found: run semantic/sales_metrics.sql first.")

# COMMAND ----------

summary = spark.createDataFrame(results, "check string, expected string, actual string, status string")
display(summary)
failed = [r for r in results if r[3] == "FAIL"]
assert not failed, f"{len(failed)} check(s) failed: {[r[0] for r in failed]}"
