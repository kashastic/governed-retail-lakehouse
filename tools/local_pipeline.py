"""Run the pipeline's transformations locally, in batch mode.

The Databricks pipeline (pipelines/transformations) wires the same functions to
streaming tables, AUTO CDC flows and materialized views. Here they run on a
local SparkSession over a landing folder written by the generator, so the logic,
the manifest reconciliation and the Genie ground-truth SQL can all be checked
without a workspace.

Differences from Databricks, on purpose:
- batch reads instead of Auto Loader, and dropDuplicates instead of the watermark;
- scd2_from_cdc / scd1_from_cdc stand in for AUTO CDC;
- tables are written to a local warehouse as gold.x instead of retail.gold.x.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from utilities import catalog_docs as docs
from utilities import rules, schemas, transforms


def _read(spark: SparkSession, root: str, entity: str, fmt: str, schema) -> DataFrame:
    reader = (spark.read.format(fmt).schema(schema)
              .option("recursiveFileLookup", "true")
              .option("pathGlobFilter", f"*.{fmt}"))
    if fmt == "csv":
        reader = reader.option("header", "true")
    return reader.load(f"{root}/{entity}").select(
        "*", F.col("_metadata.file_path").alias("_source_file"),
        F.current_timestamp().alias("_ingested_at"))


def _passes(rule_set: dict[str, str]) -> str:
    return " AND ".join(f"({cond})" for cond in rule_set.values())


def _save(spark: SparkSession, df: DataFrame, name: str) -> DataFrame:
    df.write.mode("overwrite").saveAsTable(name)
    return spark.read.table(name)


def run(spark: SparkSession, root: str, calendar: tuple[str, str] = ("2025-01-01", "2026-12-31")) -> None:
    for schema in ("silver", "gold", "dq"):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")

    # Silver: order lines
    exploded = transforms.explode_order_lines(_read(spark, root, "orders", "json", schemas.ORDERS))
    _save(spark, transforms.quarantine(exploded, rules.ORDER_LINE_RULES), "silver.order_lines_quarantine")
    _save(spark, exploded.where(_passes(rules.ORDER_LINE_RULES)), "silver.order_lines_validated")
    lines = _save(spark, transforms.dedupe_exact(spark.read.table("silver.order_lines_validated"), "order_ts"),
                  "silver.order_lines")

    # Silver: returns
    typed = transforms.type_returns(_read(spark, root, "returns", "json", schemas.RETURNS))
    _save(spark, transforms.quarantine(typed, rules.RETURN_RULES), "silver.returns_quarantine")
    _save(spark, typed.where(_passes(rules.RETURN_RULES)), "silver.returns_validated")
    returns = _save(spark, transforms.dedupe_exact(spark.read.table("silver.returns_validated"), "return_ts"),
                    "silver.returns")

    # Silver: change feeds
    cust = transforms.type_customer_cdc(_read(spark, root, "customers", "csv", schemas.CUSTOMERS_CDC))
    customers = _save(spark, transforms.scd2_from_cdc(cust.where(_passes(rules.CUSTOMER_CDC_RULES)),
                                                      "customer_id", "change_ts", transforms.CDC_EXCEPT_COLUMNS),
                      "silver.customers")
    prod = transforms.type_product_cdc(_read(spark, root, "products", "csv", schemas.PRODUCTS_CDC))
    products = _save(spark, transforms.scd1_from_cdc(prod.where(_passes(rules.PRODUCT_CDC_RULES)),
                                                     "product_id", "change_ts", transforms.CDC_EXCEPT_COLUMNS),
                     "silver.products")

    # Gold
    _save(spark, docs.conform(transforms.build_dim_date(spark, *calendar), docs.DIM_DATE), "gold.dim_date")
    dim_customer = _save(spark, docs.conform(transforms.build_dim_customer(spark, customers), docs.DIM_CUSTOMER),
                         "gold.dim_customer")
    dim_product = _save(spark, docs.conform(transforms.build_dim_product(spark, products), docs.DIM_PRODUCT),
                        "gold.dim_product")
    fact = _save(spark, docs.conform(transforms.build_fact_order_line(lines, dim_customer, dim_product, returns),
                                     docs.FACT_ORDER_LINE), "gold.fact_order_line")
    fact_return = _save(spark, docs.conform(transforms.build_fact_return(returns, fact), docs.FACT_RETURN),
                        "gold.fact_return")

    # DQ
    _save(spark, transforms.key_check(fact, "order_line_id"), "dq.fact_order_line_key_check")
    _save(spark, transforms.key_check(fact_return, "return_id"), "dq.fact_return_key_check")
    _save(spark, transforms.key_check(dim_customer, "customer_key"), "dq.dim_customer_key_check")
    _save(spark, transforms.issue_summary(spark.read.table("silver.order_lines_quarantine"),
                                          spark.read.table("silver.returns_quarantine"), fact, fact_return),
          "dq.issue_summary")


def local_sql(sql: str) -> str:
    """Databricks SQL written for catalog `retail` -> local two-part names."""
    return sql.replace("retail.", "")
