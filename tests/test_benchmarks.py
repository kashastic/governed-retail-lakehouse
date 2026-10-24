"""The Genie benchmark is well-formed, its SQL runs, and every trap really changes the answer."""
from collections import Counter

import pytest

from genie.benchmarks import BENCHMARKS
from tools import export_benchmarks
from tools.local_pipeline import local_sql


def test_benchmark_shape():
    assert len(BENCHMARKS) == 25
    assert len({b["id"] for b in BENCHMARKS}) == 25
    assert Counter(b["split"] for b in BENCHMARKS) == {"dev": 15, "holdout": 10}
    assert Counter(b["category"] for b in BENCHMARKS) == {
        "kpi": 6, "breakdown": 5, "returns": 5, "ambiguous": 5, "join": 4}
    for b in BENCHMARKS:
        assert "retail.gold." in b["sql"] and "semantic" not in b["sql"]


@pytest.mark.parametrize("bench", BENCHMARKS, ids=[b["id"] for b in BENCHMARKS])
def test_ground_truth_runs_and_trap_differs(spark, local_tables, bench):
    rows = spark.sql(local_sql(bench["sql"])).collect()
    assert rows and all(v is not None for v in rows[0])
    if bench.get("trap_sql"):
        trap = spark.sql(local_sql(bench["trap_sql"])).collect()
        assert export_benchmarks.format_rows(trap) != export_benchmarks.format_rows(rows)


def test_committed_files_are_up_to_date(spark, local_tables):
    assert export_benchmarks.QUESTIONS_CSV.read_text() == export_benchmarks.questions_csv(), \
        "run: python -m tools.export_benchmarks --answers"
    assert export_benchmarks.ANSWERS_CSV.read_text() == export_benchmarks.answers_csv(spark), \
        "run: python -m tools.export_benchmarks --answers"
