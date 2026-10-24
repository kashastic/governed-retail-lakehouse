"""Gold: star schema as materialized views.

Each table is selected and cast from its spec in utilities/catalog_docs.py, and
the same spec supplies the schema with column comments, so the descriptions
live in Unity Catalog next to the data.

Two warn-only expectations count known integrity problems without dropping
revenue: order lines for products missing from the catalogue, and returns that
point to an order line that doesn't exist. Both are kept and flagged.
"""
from pyspark import pipelines as dp

from utilities import catalog_docs as docs
from utilities import transforms

CALENDAR_START = spark.conf.get("calendar_start", "2025-01-01")
CALENDAR_END = spark.conf.get("calendar_end", "2026-12-31")


@dp.materialized_view(name="gold.dim_date", comment=docs.TABLE_COMMENTS["dim_date"],
                      schema=docs.schema_ddl(docs.DIM_DATE))
def dim_date():
    return docs.conform(transforms.build_dim_date(spark, CALENDAR_START, CALENDAR_END), docs.DIM_DATE)


@dp.materialized_view(name="gold.dim_customer", comment=docs.TABLE_COMMENTS["dim_customer"],
                      schema=docs.schema_ddl(docs.DIM_CUSTOMER))
def dim_customer():
    built = transforms.build_dim_customer(spark, spark.read.table("silver.customers"))
    return docs.conform(built, docs.DIM_CUSTOMER)


@dp.materialized_view(name="gold.dim_product", comment=docs.TABLE_COMMENTS["dim_product"],
                      schema=docs.schema_ddl(docs.DIM_PRODUCT))
def dim_product():
    built = transforms.build_dim_product(spark, spark.read.table("silver.products"))
    return docs.conform(built, docs.DIM_PRODUCT)


@dp.materialized_view(name="gold.fact_order_line", comment=docs.TABLE_COMMENTS["fact_order_line"],
                      schema=docs.schema_ddl(docs.FACT_ORDER_LINE))
@dp.expect("known_product", "NOT is_orphan_product")
@dp.expect("known_customer_version", "customer_key <> -1")
@dp.expect("returned_within_sold_quantity", "returned_quantity <= quantity")
def fact_order_line():
    built = transforms.build_fact_order_line(
        spark.read.table("silver.order_lines"),
        spark.read.table("gold.dim_customer"),
        spark.read.table("gold.dim_product"),
        spark.read.table("silver.returns"),
    )
    return docs.conform(built, docs.FACT_ORDER_LINE)


@dp.materialized_view(name="gold.fact_return", comment=docs.TABLE_COMMENTS["fact_return"],
                      schema=docs.schema_ddl(docs.FACT_RETURN))
@dp.expect("known_order_line", "NOT is_orphan_return")
def fact_return():
    built = transforms.build_fact_return(spark.read.table("silver.returns"),
                                         spark.read.table("gold.fact_order_line"))
    return docs.conform(built, docs.FACT_RETURN)
