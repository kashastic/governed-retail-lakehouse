"""Write genie/benchmarks.csv and genie/expected_answers.csv.

    python -m tools.export_benchmarks             # questions + ground-truth SQL
    python -m tools.export_benchmarks --answers   # also run the pipeline locally and record each answer

The data is deterministic, so the expected answers computed locally are the
answers the Databricks gold tables must give too. They double as a check that
the Databricks pipeline matches the local one.
"""
from __future__ import annotations

import argparse
import csv
import io
import sys
import tempfile
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
for p in (ROOT, ROOT / "pipelines"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from genie.benchmarks import BENCHMARKS  # noqa: E402

QUESTIONS_CSV = ROOT / "genie" / "benchmarks.csv"
ANSWERS_CSV = ROOT / "genie" / "expected_answers.csv"


def _csv(header: list[str], rows: list[list[str]]) -> str:
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(header)
    writer.writerows(rows)
    return buf.getvalue()


def questions_csv() -> str:
    return _csv(["id", "split", "category", "question", "ground_truth_sql", "what_makes_it_hard"],
                [[b["id"], b["split"], b["category"], b["question"], b["sql"], b["hard"]] for b in BENCHMARKS])


def _fmt(value) -> str:
    if isinstance(value, float):
        return f"{value:.6f}"
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)


def format_rows(rows) -> str:
    return " | ".join(", ".join(_fmt(v) for v in row) for row in rows)


def answers_csv(spark) -> str:
    from tools.local_pipeline import local_sql
    rows = []
    for b in BENCHMARKS:
        answer = format_rows(spark.sql(local_sql(b["sql"])).collect())
        trap = format_rows(spark.sql(local_sql(b["trap_sql"])).collect()) if b.get("trap_sql") else ""
        rows.append([b["id"], answer, trap])
    return _csv(["id", "expected_answer", "wrong_reading_gives"], rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--answers", action="store_true", help="run the pipeline locally and record answers")
    args = parser.parse_args()
    QUESTIONS_CSV.write_text(questions_csv())
    print(f"wrote {QUESTIONS_CSV.relative_to(ROOT)}")
    if not args.answers:
        return

    from pyspark.sql import SparkSession

    from generator import build_world, plan_batches, write_batch
    from tools import local_pipeline

    with tempfile.TemporaryDirectory() as tmp:
        spark = (SparkSession.builder.master("local[4]").config("spark.ui.enabled", "false")
                 .config("spark.sql.session.timeZone", "UTC")
                 .config("spark.sql.shuffle.partitions", "8")
                 .config("spark.sql.warehouse.dir", f"{tmp}/warehouse").getOrCreate())
        spark.sparkContext.setLogLevel("ERROR")
        world = build_world()
        for batch in plan_batches(world.cfg):
            write_batch(world, batch, f"{tmp}/landing")
        local_pipeline.run(spark, f"{tmp}/landing")
        ANSWERS_CSV.write_text(answers_csv(spark))
        spark.stop()
    print(f"wrote {ANSWERS_CSV.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
