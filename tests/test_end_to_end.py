"""Run the whole pipeline locally on all 15 batches and reconcile it with the generator's manifests.

These are the same checks notebooks/04_validate.py runs in Databricks.
"""
from collections import Counter

import pytest


def _manifest_totals(manifests, feed: str, field: str) -> Counter:
    total = Counter()
    for m in manifests:
        value = m[feed][field]
        if isinstance(value, dict):
            total.update(value)
        else:
            total[field] += value
    return total


def _quarantined(spark, table: str) -> Counter:
    rows = spark.sql(f"SELECT rule, count(*) AS n FROM (SELECT explode(failed_rules) AS rule FROM {table}) "
                     "GROUP BY rule").collect()
    return Counter({r.rule: r.n for r in rows})


def _scalar(spark, sql: str):
    return spark.sql(sql).collect()[0][0]


def test_quarantine_matches_injected_defects(spark, local_tables):
    expected_lines = +_manifest_totals(local_tables, "orders", "rule_failures")
    expected_returns = +_manifest_totals(local_tables, "returns", "rule_failures")
    assert _quarantined(spark, "silver.order_lines_quarantine") == expected_lines
    assert _quarantined(spark, "silver.returns_quarantine") == expected_returns


def test_silver_row_counts_reconcile(spark, local_tables):
    arrived = _manifest_totals(local_tables, "orders", "order_lines_arrived")["order_lines_arrived"]
    failed = sum(_manifest_totals(local_tables, "orders", "rule_failures").values())
    duplicates = _manifest_totals(local_tables, "orders", "duplicate_lines")["duplicate_lines"]
    assert _scalar(spark, "SELECT count(*) FROM silver.order_lines") == arrived - failed - duplicates

    r_arrived = _manifest_totals(local_tables, "returns", "records_arrived")["records_arrived"]
    r_failed = sum(_manifest_totals(local_tables, "returns", "rule_failures").values())
    r_dupes = _manifest_totals(local_tables, "returns", "duplicate_records")["duplicate_records"]
    assert _scalar(spark, "SELECT count(*) FROM silver.returns") == r_arrived - r_failed - r_dupes


def test_orphans_are_kept_and_flagged(spark, local_tables):
    orphan_lines = _manifest_totals(local_tables, "orders", "orphan_product_lines")["orphan_product_lines"]
    orphan_returns = _manifest_totals(local_tables, "returns", "orphan_returns")["orphan_returns"]
    assert _scalar(spark, "SELECT count(*) FROM gold.fact_order_line WHERE is_orphan_product") == orphan_lines
    assert _scalar(spark, "SELECT count(*) FROM gold.fact_order_line WHERE product_key = 'UNKNOWN'") == orphan_lines
    assert _scalar(spark, "SELECT count(*) FROM gold.fact_return WHERE is_orphan_return") == orphan_returns


@pytest.mark.parametrize("table", ["dq.fact_order_line_key_check", "dq.fact_return_key_check",
                                   "dq.dim_customer_key_check"])
def test_gold_keys_are_unique(spark, local_tables, table):
    assert _scalar(spark, f"SELECT duplicate_keys FROM {table}") == 0


def test_gold_preserves_revenue_and_resolves_every_customer(spark, local_tables):
    silver = _scalar(spark, "SELECT sum(quantity * unit_price - discount_amount) FROM silver.order_lines")
    gold = _scalar(spark, "SELECT sum(net_amount) FROM gold.fact_order_line")
    assert silver == gold
    assert _scalar(spark, "SELECT count(*) FROM gold.fact_order_line WHERE customer_key = -1") == 0
    assert _scalar(spark, "SELECT count(*) FROM gold.fact_order_line WHERE returned_quantity > quantity") == 0


def test_every_valid_return_lands_on_its_order_line(spark, local_tables):
    returned_units = _scalar(spark, "SELECT sum(quantity) FROM gold.fact_return WHERE NOT is_orphan_return")
    assert _scalar(spark, "SELECT sum(returned_quantity) FROM gold.fact_order_line") == returned_units


def test_issue_summary_totals(spark, local_tables):
    rows = {r.issue: r.rows for r in spark.sql(
        "SELECT issue, sum(rows) AS rows FROM dq.issue_summary GROUP BY issue").collect()}
    expected = (+_manifest_totals(local_tables, "orders", "rule_failures")
                + +_manifest_totals(local_tables, "returns", "rule_failures"))
    expected["orphan_product"] = _manifest_totals(local_tables, "orders", "orphan_product_lines")["orphan_product_lines"]
    expected["orphan_return"] = _manifest_totals(local_tables, "returns", "orphan_returns")["orphan_returns"]
    assert rows == dict(expected)
