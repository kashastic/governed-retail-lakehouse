"""Unit tests for pipelines/utilities/transforms.py on small hand-made data."""
import json
from datetime import datetime

from pyspark.sql import Row
from pyspark.sql import functions as F

from utilities import rules, schemas, transforms

SRC = "/Volumes/retail/raw/landing/orders/batch_003/orders.json"


def _orders(spark, records):
    lines = spark.sparkContext.parallelize([json.dumps(r) for r in records])
    parsed = spark.read.schema(schemas.ORDERS).json(lines)
    return parsed.withColumn("_source_file", F.lit(SRC)).withColumn("_ingested_at", F.current_timestamp())


def _order(order_id="O1", customer_id="C1", ts="2026-09-03T10:00:00", items=None):
    items = items or [{"line_no": 1, "product_id": "P1", "quantity": 1, "unit_price": 10.0, "discount_amount": 0.0}]
    return {"order_id": order_id, "customer_id": customer_id, "order_ts": ts, "channel": "web",
            "currency": "EUR", "items": items}


def _item(line_no, product_id="P1", quantity=1, unit_price=10.0, discount=0.0):
    return {"line_no": line_no, "product_id": product_id, "quantity": quantity,
            "unit_price": unit_price, "discount_amount": discount}


def test_explode_order_lines_types_and_traces_rows(spark):
    df = transforms.explode_order_lines(_orders(spark, [
        _order("O1", items=[_item(1), _item(2, "P2", 2, 5.5)]),
        _order("O2", ts="not a timestamp"),
    ]))
    rows = {(r.order_id, r.line_no): r for r in df.collect()}
    assert len(rows) == 3
    assert rows[("O1", 2)].quantity == 2 and str(rows[("O1", 2)].unit_price) == "5.50"
    assert rows[("O1", 1)].batch_id == "batch_003"
    assert rows[("O2", 1)].order_ts is None            # caught later by valid_order_ts
    assert rows[("O1", 1)].record_hash != rows[("O1", 2)].record_hash


def test_quarantine_names_every_broken_rule(spark):
    df = transforms.explode_order_lines(_orders(spark, [
        _order("O1"),                                                    # clean
        _order(None, items=[_item(1, quantity=0)]),                      # two broken rules
        _order("O3", items=[_item(1, unit_price=-4.0)]),
    ]))
    bad = {tuple(r.failed_rules) for r in transforms.quarantine(df, rules.ORDER_LINE_RULES).collect()}
    assert bad == {("valid_order_id", "valid_quantity"), ("valid_unit_price",)}


def test_rule_that_evaluates_to_null_counts_as_broken(spark):
    df = spark.createDataFrame([Row(x=None)], "x int")
    out = transforms.quarantine(df, {"positive_x": "x > 0"}).collect()
    assert [r.failed_rules for r in out] == [["positive_x"]]


def test_dedupe_removes_exact_copies_only(spark):
    df = transforms.explode_order_lines(_orders(spark, [
        _order("O1"), _order("O1"),                                      # exact re-send
        _order("O2", items=[_item(1, quantity=1)]),
        _order("O2", items=[_item(1, quantity=3)]),                      # same key, different content
    ]))
    kept = transforms.dedupe_exact(df, "order_ts").collect()
    assert sorted((r.order_id, r.quantity) for r in kept) == [("O1", 1), ("O2", 1), ("O2", 3)]


def test_streaming_dedupe_drops_a_resent_file_on_the_next_run(spark, tmp_path):
    """Two triggered runs with one checkpoint: the second run's re-sent file adds nothing."""
    landing, out, ckpt = tmp_path / "landing", tmp_path / "out", tmp_path / "ckpt"
    day6 = [_order(f"O6{i}", ts=f"2026-09-06T1{i}:00:00") for i in range(3)]
    day7 = [_order(f"O7{i}", ts=f"2026-09-07T1{i}:00:00") for i in range(2)]

    def write(name, records):
        landing.mkdir(exist_ok=True)
        (landing / name).write_text("".join(json.dumps(r) + "\n" for r in records))

    def run_once():
        stream = (spark.readStream.schema(schemas.ORDERS).json(str(landing))
                  .withColumn("_source_file", F.col("_metadata.file_path"))
                  .withColumn("_ingested_at", F.current_timestamp()))
        deduped = transforms.dedupe_exact(transforms.explode_order_lines(stream), "order_ts")
        query = (deduped.writeStream.format("parquet").option("checkpointLocation", str(ckpt))
                 .trigger(availableNow=True).start(str(out)))
        query.awaitTermination()

    write("batch_006.json", day6)
    run_once()
    write("batch_007.json", day7)
    write("batch_007_resent_batch_006.json", day6)
    run_once()
    ids = sorted(r.order_id for r in spark.read.parquet(str(out)).collect())
    assert ids == ["O60", "O61", "O62", "O70", "O71"]


def _cdc(spark, rows):
    return spark.createDataFrame(rows, "customer_id string, membership string, change_ts timestamp, "
                                       "city string, country string, signup_date date, operation string")


def test_scd2_and_point_in_time_join(spark):
    t0, t1 = datetime(2026, 1, 1), datetime(2026, 5, 1)
    cdc = _cdc(spark, [("C1", "Basic", t0, "Berlin", "DE", t0.date(), "INSERT"),
                       ("C1", "Plus", t1, "Berlin", "DE", t0.date(), "UPDATE")])
    scd2 = transforms.scd2_from_cdc(cdc, "customer_id", "change_ts", ["operation", "change_ts"])
    dim_customer = transforms.build_dim_customer(spark, scd2)
    versions = {r.membership: r for r in dim_customer.collect()}
    assert versions["Basic"].valid_to == t1 and versions["Plus"].valid_to is None
    assert versions["Unknown"].customer_key == -1

    lines = transforms.explode_order_lines(_orders(spark, [
        _order("O1", "C1", "2026-03-01T09:00:00"),
        _order("O2", "C1", "2026-06-01T09:00:00"),
    ]))
    products = spark.createDataFrame([("P1", "Mug", "Home & Kitchen", "Lumo", 10.0)],
                                     "product_id string, product_name string, category string, brand string, "
                                     "list_price double")
    dim_product = transforms.build_dim_product(spark, products)
    empty_returns = spark.createDataFrame([], "order_id string, line_no int, quantity int, refund_amount decimal(12,2)")
    fact = transforms.build_fact_order_line(lines, dim_customer, dim_product, empty_returns)
    key_by_order = {r.order_id: r.customer_key for r in fact.collect()}
    assert key_by_order["O1"] == versions["Basic"].customer_key
    assert key_by_order["O2"] == versions["Plus"].customer_key


def test_fact_keeps_orphan_revenue_and_attaches_returns(spark):
    lines = transforms.explode_order_lines(_orders(spark, [
        _order("O1", items=[_item(1, "P1", 2, 10.0, 2.0), _item(2, "P404", 1, 30.0)]),
    ]))
    t0 = datetime(2025, 1, 1)
    dim_customer = transforms.build_dim_customer(spark, transforms.scd2_from_cdc(
        _cdc(spark, [("C1", "Basic", t0, "Paris", "FR", t0.date(), "INSERT")]),
        "customer_id", "change_ts", ["operation", "change_ts"]))
    dim_product = transforms.build_dim_product(spark, spark.createDataFrame(
        [("P1", "Mug", "Home & Kitchen", "Lumo", 10.0)],
        "product_id string, product_name string, category string, brand string, list_price double"))
    returns = spark.createDataFrame(
        # Midday timestamps: naive Python datetimes are shifted from local time to the UTC session.
        [("R1", "O1", 1, datetime(2026, 9, 10, 12), 1, "damaged_in_transit", 9.0, "batch_005"),
         ("R2", "O999", 1, datetime(2026, 9, 11, 12), 1, "changed_mind", 5.0, "batch_006")],
        "return_id string, order_id string, line_no int, return_ts timestamp, quantity int, reason string, "
        "refund_amount double, batch_id string").withColumn("refund_amount", F.col("refund_amount").cast("decimal(12,2)"))

    fact = {r.line_no: r for r in transforms.build_fact_order_line(lines, dim_customer, dim_product, returns).collect()}
    assert fact[2].product_key == "UNKNOWN" and fact[2].is_orphan_product and str(fact[2].net_amount) == "30.00"
    assert str(fact[1].gross_amount) == "20.00" and str(fact[1].net_amount) == "18.00"
    assert fact[1].returned_quantity == 1 and str(fact[1].refund_amount) == "9.00"

    fact_df = transforms.build_fact_order_line(lines, dim_customer, dim_product, returns)
    out = {r.return_id: r for r in transforms.build_fact_return(returns, fact_df).collect()}
    assert not out["R1"].is_orphan_return and out["R1"].days_to_return == 7
    assert out["R2"].is_orphan_return and out["R2"].product_key == "UNKNOWN" and out["R2"].customer_key == -1


def test_key_check_counts_duplicate_keys(spark):
    df = spark.createDataFrame([("a",), ("b",), ("b",), ("c",), ("c",), ("c",)], "k string")
    row = transforms.key_check(df, "k").collect()[0]
    assert (row.total_rows, row.distinct_keys, row.duplicate_keys) == (6, 3, 2)


def test_dim_date(spark):
    rows = {r.date.isoformat(): r for r in transforms.build_dim_date(spark, "2026-09-04", "2026-09-07").collect()}
    assert rows["2026-09-05"].is_weekend and not rows["2026-09-07"].is_weekend
    assert rows["2026-09-07"].date_key == 20260907 and rows["2026-09-07"].week_start.isoformat() == "2026-09-07"
