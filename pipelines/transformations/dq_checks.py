"""Data-quality checks that need more than one row at a time.

- Key checks fail the update (expect_or_fail). A duplicate key in gold means
  two different records claim the same order line, or a join fanned out. That
  is a bug or a conflict, not bad input, so the run stops for a person to look.
- issue_summary collects every known data issue by feed, batch and rule for
  the dashboard's data-quality page.
"""
from pyspark import pipelines as dp

from utilities import transforms


@dp.materialized_view(name="dq.fact_order_line_key_check",
                      comment="Fails the update if order_line_id is not unique in gold.fact_order_line.")
@dp.expect_or_fail("unique_order_line_id", "duplicate_keys = 0")
def fact_order_line_key_check():
    return transforms.key_check(spark.read.table("gold.fact_order_line"), "order_line_id")


@dp.materialized_view(name="dq.fact_return_key_check",
                      comment="Fails the update if return_id is not unique in gold.fact_return.")
@dp.expect_or_fail("unique_return_id", "duplicate_keys = 0")
def fact_return_key_check():
    return transforms.key_check(spark.read.table("gold.fact_return"), "return_id")


@dp.materialized_view(name="dq.dim_customer_key_check",
                      comment="Fails the update if customer versions overlap (duplicate customer_key).")
@dp.expect_or_fail("unique_customer_key", "duplicate_keys = 0")
def dim_customer_key_check():
    return transforms.key_check(spark.read.table("gold.dim_customer"), "customer_key")


@dp.materialized_view(name="dq.issue_summary",
                      comment="Known data issues by feed, batch and rule, and whether they were quarantined or kept and flagged.")
def issue_summary():
    return transforms.issue_summary(
        spark.read.table("silver.order_lines_quarantine"),
        spark.read.table("silver.returns_quarantine"),
        spark.read.table("gold.fact_order_line"),
        spark.read.table("gold.fact_return"),
    )
