"""Pure PySpark transformations used by the pipeline.

Nothing here imports the pipelines module, so every function can be unit-tested
on a local SparkSession (tests/test_transforms.py). The pipeline files in
pipelines/transformations only wire these functions to tables.
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

BATCH_ID_PATTERN = r"/(batch_\d{3})/"
UNKNOWN_PRODUCT_KEY = "UNKNOWN"
UNKNOWN_CUSTOMER_KEY = -1

ORDER_LINE_BUSINESS_COLS = ["order_id", "customer_id", "order_ts", "channel", "currency",
                            "line_no", "product_id", "quantity", "unit_price", "discount_amount"]
RETURN_BUSINESS_COLS = ["return_id", "order_id", "line_no", "product_id", "return_ts",
                        "quantity", "reason", "refund_amount"]


# --------------------------------------------------------------------------- helpers

def batch_id(path: Column) -> Column:
    """Landing batch from a file path such as .../orders/batch_007/orders.json."""
    return F.regexp_extract(path, BATCH_ID_PATTERN, 1)


def record_hash(*cols: str) -> Column:
    """Content hash of the business columns. Two rows share it only if they are exact copies."""
    return F.sha2(F.to_json(F.struct(*cols)), 256)


def failed_rules(rules: dict[str, str]) -> Column:
    """Array with the name of every rule the row breaks. A rule that evaluates to NULL counts as broken."""
    checks = [F.when(~F.coalesce(F.expr(cond), F.lit(False)), F.lit(name)) for name, cond in rules.items()]
    return F.array_compact(F.array(*checks))


# --------------------------------------------------------------------------- silver

def explode_order_lines(orders: DataFrame) -> DataFrame:
    """One row per order line, typed, with the source batch and a content hash.

    Bad timestamps become NULL (try_to_timestamp) and are caught by the
    valid_order_ts rule instead of failing the stream.
    """
    lines = orders.select(
        "order_id", "customer_id",
        F.try_to_timestamp(F.col("order_ts")).alias("order_ts"),
        "channel", "currency",
        F.explode("items").alias("item"),
        "_source_file", "_ingested_at",
    )
    lines = lines.select(
        "order_id", "customer_id", "order_ts", "channel", "currency",
        F.col("item.line_no").alias("line_no"),
        F.col("item.product_id").alias("product_id"),
        F.col("item.quantity").alias("quantity"),
        F.col("item.unit_price").cast("decimal(10,2)").alias("unit_price"),
        F.col("item.discount_amount").cast("decimal(12,2)").alias("discount_amount"),
        batch_id(F.col("_source_file")).alias("batch_id"),
        "_source_file", "_ingested_at",
    )
    return lines.withColumn("record_hash", record_hash(*ORDER_LINE_BUSINESS_COLS))


def type_returns(returns: DataFrame) -> DataFrame:
    df = returns.select(
        "return_id", "order_id", "line_no", "product_id",
        F.try_to_timestamp(F.col("return_ts")).alias("return_ts"),
        "quantity", "reason",
        F.col("refund_amount").cast("decimal(12,2)").alias("refund_amount"),
        batch_id(F.col("_source_file")).alias("batch_id"),
        "_source_file", "_ingested_at",
    )
    return df.withColumn("record_hash", record_hash(*RETURN_BUSINESS_COLS))


def type_customer_cdc(cdc: DataFrame) -> DataFrame:
    return cdc.select(
        "customer_id", "email", "first_name", "last_name", "city", "country", "membership",
        F.try_to_timestamp(F.col("signup_date")).cast("date").alias("signup_date"),
        "operation",
        F.try_to_timestamp(F.col("change_ts")).alias("change_ts"),
        batch_id(F.col("_source_file")).alias("batch_id"),
        "_source_file", "_ingested_at",
    )


def type_product_cdc(cdc: DataFrame) -> DataFrame:
    return cdc.select(
        "product_id", "product_name", "category", "brand",
        F.col("list_price").cast("decimal(10,2)").alias("list_price"),
        "operation",
        F.try_to_timestamp(F.col("change_ts")).alias("change_ts"),
        batch_id(F.col("_source_file")).alias("batch_id"),
        "_source_file", "_ingested_at",
    )


CDC_EXCEPT_COLUMNS = ["operation", "change_ts", "batch_id", "_source_file", "_ingested_at"]


def quarantine(df: DataFrame, rules: dict[str, str]) -> DataFrame:
    """Rows that break at least one rule, with the names of the rules they break."""
    return (df.withColumn("failed_rules", failed_rules(rules))
              .where(F.size("failed_rules") > 0)
              .withColumn("quarantined_at", F.current_timestamp()))


def dedupe_exact(df: DataFrame, event_time: str, delay: str = "3 days") -> DataFrame:
    """Drop exact re-sends (same content hash) that arrive within `delay` of each other.

    Only exact copies are removed. Two different rows with the same business key
    both pass through, and the gold key check fails the update, because that is
    a conflict a person has to look at.

    The watermark bounds the state: hashes older than `delay` behind the newest
    event are forgotten. Batch DataFrames (local tests) fall back to dropDuplicates.
    """
    if not df.isStreaming:
        return df.dropDuplicates(["record_hash"])
    return df.withWatermark(event_time, delay).dropDuplicatesWithinWatermark(["record_hash"])


# --------------------------------------------------------------------------- local stand-ins for AUTO CDC
# The pipeline uses dp.create_auto_cdc_flow. These two functions reproduce its
# output shape for local tests and the local end-to-end check.

def scd2_from_cdc(cdc: DataFrame, key: str, sequence_by: str, except_columns: list[str]) -> DataFrame:
    w = Window.partitionBy(key).orderBy(sequence_by)
    return (cdc.withColumn("__START_AT", F.col(sequence_by))
               .withColumn("__END_AT", F.lead(sequence_by).over(w))
               .drop(*except_columns))


def scd1_from_cdc(cdc: DataFrame, key: str, sequence_by: str, except_columns: list[str]) -> DataFrame:
    w = Window.partitionBy(key).orderBy(F.col(sequence_by).desc())
    return (cdc.withColumn("_rn", F.row_number().over(w))
               .where("_rn = 1")
               .drop("_rn", *except_columns))


# --------------------------------------------------------------------------- gold

def build_dim_date(spark: SparkSession, start: str, end: str) -> DataFrame:
    days = spark.sql(f"SELECT explode(sequence(DATE'{start}', DATE'{end}', INTERVAL 1 DAY)) AS date")
    return days.select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        "date",
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.trunc("date", "month").alias("month_start"),
        F.date_trunc("week", "date").cast("date").alias("week_start"),
        F.date_format("date", "EEEE").alias("day_name"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend"),
    )


def build_dim_customer(spark: SparkSession, customers_scd2: DataFrame) -> DataFrame:
    """One row per customer version from the SCD2 table, without direct identifiers, plus an unknown member."""
    versions = customers_scd2.select(
        F.xxhash64("customer_id", "__START_AT").alias("customer_key"),
        "customer_id", "membership", "city", "country", "signup_date",
        F.col("__START_AT").alias("valid_from"),
        F.col("__END_AT").alias("valid_to"),
        F.col("__END_AT").isNull().alias("is_current"),
    )
    unknown = spark.range(1).select(
        F.lit(UNKNOWN_CUSTOMER_KEY).cast("bigint").alias("customer_key"),
        F.lit("UNKNOWN").alias("customer_id"),
        F.lit("Unknown").alias("membership"),
        F.lit("Unknown").alias("city"),
        F.lit("Unknown").alias("country"),
        F.lit(None).cast("date").alias("signup_date"),
        F.lit(None).cast("timestamp").alias("valid_from"),
        F.lit(None).cast("timestamp").alias("valid_to"),
        F.lit(True).alias("is_current"),
    )
    return versions.unionByName(unknown)


def build_dim_product(spark: SparkSession, products: DataFrame) -> DataFrame:
    current = products.select(
        F.col("product_id").alias("product_key"),
        "product_id", "product_name", "category", "brand",
        F.col("list_price").alias("current_list_price"),
    )
    unknown = spark.range(1).select(
        F.lit(UNKNOWN_PRODUCT_KEY).alias("product_key"),
        F.lit(UNKNOWN_PRODUCT_KEY).alias("product_id"),
        F.lit("Unknown product").alias("product_name"),
        F.lit("Unknown").alias("category"),
        F.lit("Unknown").alias("brand"),
        F.lit(None).cast("decimal(10,2)").alias("current_list_price"),
    )
    return current.unionByName(unknown)


def build_fact_order_line(lines: DataFrame, dim_customer: DataFrame, dim_product: DataFrame,
                          returns: DataFrame) -> DataFrame:
    """One row per order line.

    - Customer: point-in-time join to the version valid when the order was placed.
    - Product: LEFT join; lines whose product is missing from the catalogue keep
      their revenue under the 'UNKNOWN' product instead of disappearing.
    - Returns: aggregated per line and attached to the original order line.
    """
    customers = dim_customer.where(F.col("customer_key") != UNKNOWN_CUSTOMER_KEY).select(
        F.col("customer_key").alias("c_customer_key"),
        F.col("customer_id").alias("c_customer_id"),
        F.col("valid_from").alias("c_valid_from"),
        F.col("valid_to").alias("c_valid_to"),
    )
    products = dim_product.where(F.col("product_key") != UNKNOWN_PRODUCT_KEY).select(
        F.col("product_key").alias("p_product_key"))
    returned = returns.groupBy("order_id", "line_no").agg(
        F.sum("quantity").alias("r_returned_quantity"),
        F.sum("refund_amount").alias("r_refund_amount"),
    )

    in_version = ((F.col("customer_id") == F.col("c_customer_id"))
                  & (F.col("order_ts") >= F.col("c_valid_from"))
                  & (F.col("c_valid_to").isNull() | (F.col("order_ts") < F.col("c_valid_to"))))
    joined = (lines.join(customers, in_version, "left")
                   .join(products, F.col("product_id") == F.col("p_product_key"), "left")
                   .join(returned, ["order_id", "line_no"], "left"))

    gross = (F.col("quantity") * F.col("unit_price")).cast("decimal(12,2)")
    return joined.select(
        F.concat_ws("-", "order_id", F.col("line_no").cast("string")).alias("order_line_id"),
        "order_id", "line_no", "order_ts",
        F.to_date("order_ts").alias("order_date"),
        F.date_format("order_ts", "yyyyMMdd").cast("int").alias("order_date_key"),
        F.coalesce("c_customer_key", F.lit(UNKNOWN_CUSTOMER_KEY).cast("bigint")).alias("customer_key"),
        "customer_id",
        F.coalesce("p_product_key", F.lit(UNKNOWN_PRODUCT_KEY)).alias("product_key"),
        F.col("product_id").alias("source_product_id"),
        F.col("p_product_key").isNull().alias("is_orphan_product"),
        "channel", "quantity", "unit_price",
        gross.alias("gross_amount"),
        "discount_amount",
        (gross - F.col("discount_amount")).cast("decimal(12,2)").alias("net_amount"),
        F.coalesce("r_returned_quantity", F.lit(0)).cast("int").alias("returned_quantity"),
        F.coalesce("r_refund_amount", F.lit(0)).cast("decimal(12,2)").alias("refund_amount"),
        "batch_id",
    )


def build_fact_return(returns: DataFrame, fact_order_line: DataFrame) -> DataFrame:
    """One row per return. Orphan returns are kept, mapped to the unknown members and flagged."""
    lines = fact_order_line.select(
        "order_id", "line_no",
        F.col("order_line_id").alias("f_order_line_id"),
        F.col("order_date").alias("f_order_date"),
        F.col("product_key").alias("f_product_key"),
        F.col("customer_key").alias("f_customer_key"),
    )
    joined = returns.join(lines, ["order_id", "line_no"], "left")
    return_date = F.to_date("return_ts")
    return joined.select(
        "return_id", "return_ts",
        return_date.alias("return_date"),
        F.date_format("return_ts", "yyyyMMdd").cast("int").alias("return_date_key"),
        F.concat_ws("-", "order_id", F.col("line_no").cast("string")).alias("order_line_id"),
        "order_id", "line_no",
        F.col("f_order_date").alias("order_date"),
        F.coalesce("f_product_key", F.lit(UNKNOWN_PRODUCT_KEY)).alias("product_key"),
        F.coalesce("f_customer_key", F.lit(UNKNOWN_CUSTOMER_KEY).cast("bigint")).alias("customer_key"),
        "reason", "quantity", "refund_amount",
        F.datediff(return_date, F.col("f_order_date")).alias("days_to_return"),
        F.col("f_order_line_id").isNull().alias("is_orphan_return"),
        "batch_id",
    )


# --------------------------------------------------------------------------- data-quality views

def key_check(df: DataFrame, key: str) -> DataFrame:
    """Single row: total rows, distinct keys and how many keys appear more than once."""
    per_key = df.groupBy(key).agg(F.count("*").alias("n"))
    return per_key.agg(
        F.sum("n").alias("total_rows"),
        F.count("*").alias("distinct_keys"),
        F.coalesce(F.sum(F.when(F.col("n") > 1, 1)), F.lit(0)).alias("duplicate_keys"),
    )


def issue_summary(order_lines_quarantine: DataFrame, returns_quarantine: DataFrame,
                  fact_order_line: DataFrame, fact_return: DataFrame) -> DataFrame:
    """Every known data issue by feed, batch and rule, with how the pipeline handled it."""
    def quarantined(df: DataFrame, feed: str) -> DataFrame:
        return df.select(F.lit(feed).alias("feed"), "batch_id",
                         F.explode("failed_rules").alias("issue"),
                         F.lit("quarantined").alias("handling"))

    def flagged(df: DataFrame, feed: str, flag: str, issue: str) -> DataFrame:
        return df.where(F.col(flag)).select(F.lit(feed).alias("feed"), "batch_id",
                                            F.lit(issue).alias("issue"),
                                            F.lit("kept, flagged").alias("handling"))

    issues = (quarantined(order_lines_quarantine, "order_lines")
              .unionByName(quarantined(returns_quarantine, "returns"))
              .unionByName(flagged(fact_order_line, "order_lines", "is_orphan_product", "orphan_product"))
              .unionByName(flagged(fact_return, "returns", "is_orphan_return", "orphan_return")))
    return issues.groupBy("feed", "batch_id", "issue", "handling").agg(F.count("*").alias("rows"))
