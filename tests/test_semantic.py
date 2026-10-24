"""Static and local checks for the metric views in semantic/*.sql.

Databricks evaluates metric views; locally we parse the YAML, check that every
column it references exists in the gold specs, and evaluate each non-window
measure as a plain aggregate over the same joins.
"""
import re
from pathlib import Path

import pytest
import yaml

from utilities import catalog_docs as docs

SEMANTIC = Path(__file__).resolve().parents[1] / "semantic"
SPECS = {
    "retail.gold.fact_order_line": docs.FACT_ORDER_LINE,
    "retail.gold.fact_return": docs.FACT_RETURN,
    "retail.gold.dim_customer": docs.DIM_CUSTOMER,
    "retail.gold.dim_product": docs.DIM_PRODUCT,
    "retail.gold.dim_date": docs.DIM_DATE,
}


def load(name: str) -> dict:
    text = (SEMANTIC / f"{name}.sql").read_text()
    mv = yaml.safe_load(re.search(r"\$\$(.*?)\$\$", text, re.S).group(1))
    for join in mv["joins"]:
        # PyYAML follows YAML 1.1, where a bare `on` key parses as boolean True.
        if True in join:
            join["on"] = join.pop(True)
    return mv


@pytest.mark.parametrize("name", ["sales_metrics", "returns_metrics"])
def test_metric_view_references_existing_columns(name):
    mv = load(name)
    assert mv["version"] == 1.1
    aliases = {"source": {c for c, _, _ in SPECS[mv["source"]]}}
    for join in mv["joins"]:
        aliases[join["name"]] = {c for c, _, _ in SPECS[join["source"]]}
    names = [f["name"] for f in mv["fields"]] + [m["name"] for m in mv["measures"]]
    assert len(names) == len(set(names)), "field and measure names must be unique"
    for item in mv["fields"] + mv["measures"] + [{"expr": j["on"]} for j in mv["joins"]]:
        for alias, column in re.findall(r"\b([a-z_]+)\.([a-z_]+)\b", item["expr"]):
            assert column in aliases[alias], f"{alias}.{column} not found ({item.get('name', 'join')})"
        assert len(item.get("synonyms", [])) <= 10


def _local_query(mv: dict, measures: list[str], where: str = "") -> str:
    def local(table: str) -> str:
        return table.replace("retail.", "")
    joins = " ".join(f"LEFT JOIN {local(j['source'])} {j['name']} ON {j['on']}" for j in mv["joins"])
    exprs = ", ".join(f"{m['expr']} AS {m['name']}" for m in mv["measures"] if m["name"] in measures)
    return f"SELECT {exprs} FROM {local(mv['source'])} source {joins} {where}"


def test_sales_measures_match_plain_sql(spark, local_tables):
    mv = load("sales_metrics")
    measures = [m["name"] for m in mv["measures"] if "window" not in m]
    via_view = spark.sql(_local_query(mv, measures, "WHERE order_calendar.month_start = DATE'2026-08-01'")).collect()[0]
    plain = spark.sql("""
        SELECT SUM(net_amount) AS net_sales,
               SUM(net_amount) - SUM(refund_amount) AS net_revenue,
               COUNT(DISTINCT order_id) AS orders,
               SUM(returned_quantity) / SUM(quantity) AS return_rate
        FROM gold.fact_order_line
        WHERE order_date >= DATE'2026-08-01' AND order_date < DATE'2026-09-01'""").collect()[0]
    for name in ("net_sales", "net_revenue", "orders", "return_rate"):
        assert via_view[name] == plain[name], name
    assert float(via_view["aov"]) == pytest.approx(float(plain["net_sales"]) / plain["orders"])


def test_returns_measures_evaluate(spark, local_tables):
    mv = load("returns_metrics")
    row = spark.sql(_local_query(mv, [m["name"] for m in mv["measures"]])).collect()[0]
    assert row["returns"] > 0 and row["refunds_paid"] > 0 and 1 <= row["avg_days_to_return"] <= 30
