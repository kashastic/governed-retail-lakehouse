"""Static checks for dashboards/retail_kpis.lvdash.json.

The dashboard only renders in Databricks, so check here what a typo would break there: every widget
uses a dataset that exists, every MEASURE() and field exists in the metric view, every SQL column
is selected by its dataset, and every encoding points at a field its query returns.
"""
import json
import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
DASHBOARD = json.loads((ROOT / "dashboards" / "retail_kpis.lvdash.json").read_text())
DATASETS = {d["name"]: d for d in DASHBOARD["datasets"]}
WIDGETS = [(page["name"], item["widget"]) for page in DASHBOARD["pages"] for item in page["layout"]]


def metric_view(asset_name: str) -> dict:
    name = asset_name.rsplit(".", 1)[1]
    text = (ROOT / "semantic" / f"{name}.sql").read_text()
    mv = yaml.safe_load(re.search(r"\$\$(.*?)\$\$", text, re.S).group(1))
    return {"measures": {m["name"] for m in mv["measures"]}, "fields": {f["name"] for f in mv["fields"]}}


def sql_columns(dataset: dict) -> set:
    select = "".join(dataset["queryLines"]).split("FROM")[0]
    return set(re.findall(r"(\w+)`?\s*(?:,|$)", select, re.M))


def test_sql_datasets_use_existing_measures():
    sales = metric_view("retail.semantic.sales_metrics")
    for ds in DATASETS.values():
        for measure in re.findall(r"MEASURE\((\w+)\)", "".join(ds.get("queryLines", []))):
            assert measure in sales["measures"], (ds["name"], measure)


@pytest.mark.parametrize("page, widget", WIDGETS, ids=[w["name"] for _, w in WIDGETS])
def test_widget_references_exist(page, widget):
    for q in widget.get("queries", []):
        ds = DATASETS[q["query"]["datasetName"]]
        for field in q["query"]["fields"]:
            expr = field["expression"]
            if "asset_name" in ds:
                mv = metric_view(ds["asset_name"])
                for measure in re.findall(r"MEASURE\(`(\w+)`\)", expr):
                    assert measure in mv["measures"], (widget["name"], measure)
                for column in re.findall(r"(?<!MEASURE\()`(\w+)`", expr):
                    assert column in mv["fields"] | {"associative_filter_predicate_group"}, (widget["name"], column)
            else:
                for column in re.findall(r"`(\w+)`", expr):
                    assert column in sql_columns(ds) | {"associative_filter_predicate_group"}, (widget["name"], column)

    if "spec" not in widget:
        assert widget["multilineTextboxSpec"]["lines"]
        return
    returned = {f["name"] for q in widget["queries"] for f in q["query"]["fields"]}
    encodings = widget["spec"]["encodings"]
    used = [e["fieldName"] for e in encodings.get("fields", []) + encodings.get("columns", [])]
    for channel in ("x", "y", "color", "value"):
        enc = encodings.get(channel, {})
        used += [enc["fieldName"]] if "fieldName" in enc else [f["fieldName"] for f in enc.get("fields", [])]
    assert used, widget["name"]
    assert set(used) <= returned, (widget["name"], set(used) - returned)


def test_ratios_come_from_the_metric_view_not_from_averaging():
    """AOV and return rate must be MEASURE()s at the widget's grain; AVG over rows would be wrong."""
    for _, widget in WIDGETS:
        for q in widget.get("queries", []):
            for field in q["query"]["fields"]:
                assert not re.search(r"AVG\(`(aov|return_rate)`\)", field["expression"]), widget["name"]
