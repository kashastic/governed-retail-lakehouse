"""Silver: typed, validated and de-duplicated records, plus quarantine tables.

Order lines and returns follow the same pattern:

    bronze -> typed view -+-> validated streaming table (expect_all_or_drop)
                          |       -> silver materialized view (exact copies removed)
                          +-> quarantine streaming table (rows breaking any rule, with failed_rules)

One rules dict drives both branches, so the rows the expectations drop are
exactly the rows that land in quarantine. Expectations run before
de-duplication, so the event log counts every bad row that arrived.

The validated and quarantine tables are append-only streams; removing exact
copies happens in a materialized view, so no stream in the pipeline keeps state.

Customers and products are change feeds applied with AUTO CDC (formerly
APPLY CHANGES): customers keep full history (SCD type 2), products keep the
latest version (SCD type 1).
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

from utilities import rules, transforms

# --------------------------------------------------------------------------- order lines

@dp.temporary_view(comment="Order lines exploded from bronze orders, typed, with a content hash.")
def order_lines_exploded():
    return transforms.explode_order_lines(spark.readStream.table("bronze.orders_raw"))


@dp.table(name="order_lines_validated", private=True,
          comment="Order lines that pass every rule, before de-duplication.")
@dp.expect_all_or_drop(rules.ORDER_LINE_RULES)
def order_lines_validated():
    return spark.readStream.table("order_lines_exploded")


@dp.materialized_view(name="silver.order_lines",
                      comment="Valid order lines, one row per line. Exact re-sends removed by content hash; "
                              "the first arrival is kept.")
def order_lines():
    return transforms.dedupe_exact(spark.read.table("order_lines_validated"))


@dp.table(name="silver.order_lines_quarantine",
          comment="Order lines that broke at least one rule. failed_rules names each broken rule.")
def order_lines_quarantine():
    return transforms.quarantine(spark.readStream.table("order_lines_exploded"), rules.ORDER_LINE_RULES)


# --------------------------------------------------------------------------- returns

@dp.temporary_view(comment="Returns typed, with a content hash.")
def returns_typed():
    return transforms.type_returns(spark.readStream.table("bronze.returns_raw"))


@dp.table(name="returns_validated", private=True,
          comment="Returns that pass every rule, before de-duplication.")
@dp.expect_all_or_drop(rules.RETURN_RULES)
def returns_validated():
    return spark.readStream.table("returns_typed")


@dp.materialized_view(name="silver.returns",
                      comment="Valid returns, one row per return. Exact re-sends removed; the first arrival is kept.")
def returns():
    return transforms.dedupe_exact(spark.read.table("returns_validated"))


@dp.table(name="silver.returns_quarantine",
          comment="Returns that broke at least one rule. failed_rules names each broken rule.")
def returns_quarantine():
    return transforms.quarantine(spark.readStream.table("returns_typed"), rules.RETURN_RULES)


# --------------------------------------------------------------------------- customers (SCD type 2)

@dp.temporary_view(comment="Typed customer change feed.")
@dp.expect_all_or_drop(rules.CUSTOMER_CDC_RULES)
def customers_cdc_clean():
    return transforms.type_customer_cdc(spark.readStream.table("bronze.customers_cdc_raw"))


dp.create_streaming_table(
    name="silver.customers",
    comment=("Customer history, SCD type 2: one row per version, valid from __START_AT to __END_AT. "
             "Contains personal data (name, email); analysts read gold.dim_customer instead."))

dp.create_auto_cdc_flow(
    target="silver.customers",
    source="customers_cdc_clean",
    keys=["customer_id"],
    sequence_by=F.col("change_ts"),
    except_column_list=transforms.CDC_EXCEPT_COLUMNS,
    stored_as_scd_type="2",
)


# --------------------------------------------------------------------------- products (SCD type 1)

@dp.temporary_view(comment="Typed product change feed.")
@dp.expect_all_or_drop(rules.PRODUCT_CDC_RULES)
def products_cdc_clean():
    return transforms.type_product_cdc(spark.readStream.table("bronze.products_cdc_raw"))


dp.create_streaming_table(name="silver.products", comment="Product catalogue, latest version of each product.")

dp.create_auto_cdc_flow(
    target="silver.products",
    source="products_cdc_clean",
    keys=["product_id"],
    sequence_by=F.col("change_ts"),
    except_column_list=transforms.CDC_EXCEPT_COLUMNS,
    stored_as_scd_type="1",
)
