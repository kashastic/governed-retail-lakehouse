"""Score the Genie runs recorded in genie/runs/run_*.csv.

    python -m tools.score_runs

Each row needs `verdict` = good | bad. For a bad row, `failure_type` is one of the
types in genie/instructions.md. `scoring_artifact` counts as correct in the
"reviewed" accuracy and as wrong in the "strict" one. Paste the output into
docs/evaluation_report.md, so the numbers there always come from the run files.
"""
from __future__ import annotations

import csv
from collections import Counter
from pathlib import Path

RUNS = Path(__file__).resolve().parents[1] / "genie" / "runs"
FAILURE_TYPES = {"wrong_join", "wrong_metric_definition", "wrong_grain", "wrong_time_filter",
                 "unresolved_ambiguity", "missing_metadata", "scoring_artifact"}


def load(path: Path) -> list[dict]:
    with path.open() as f:
        rows = list(csv.DictReader(f))
    for r in rows:
        verdict, failure = r["verdict"].strip().lower(), r["failure_type"].strip()
        if verdict not in {"good", "bad"}:
            raise ValueError(f"{path.name} {r['id']}: verdict must be good or bad, got {verdict!r}")
        if verdict == "bad" and failure not in FAILURE_TYPES:
            raise ValueError(f"{path.name} {r['id']}: unknown failure_type {failure!r}")
        r["strict"] = verdict == "good"
        r["reviewed"] = verdict == "good" or failure == "scoring_artifact"
    return rows


def pct(rows: list[dict], key: str) -> str:
    return f"{sum(r[key] for r in rows)}/{len(rows)} ({100 * sum(r[key] for r in rows) / len(rows):.0f}%)"


def main() -> None:
    lines = ["| Run | Split | Strict | Reviewed |", "|---|---|---|---|"]
    failures: dict[str, Counter] = {}
    for path in sorted(RUNS.glob("run_*.csv")):
        try:
            rows = load(path)
        except ValueError as e:
            print(f"skipping {path.name}: {e}")
            continue
        run = path.stem.split("_")[1]
        for split in ("dev", "holdout"):
            subset = [r for r in rows if r["split"] == split]
            lines.append(f"| {run} | {split} | {pct(subset, 'strict')} | {pct(subset, 'reviewed')} |")
        lines.append(f"| {run} | **all** | {pct(rows, 'strict')} | {pct(rows, 'reviewed')} |")
        failures[run] = Counter(r["failure_type"] for r in rows if r["verdict"].lower() == "bad")
    print("\n".join(lines))
    if failures:
        types = sorted(FAILURE_TYPES)
        print("\n| Failure type | " + " | ".join(failures) + " |")
        print("|---|" + "---|" * len(failures))
        for t in types:
            print(f"| {t} | " + " | ".join(str(failures[run][t]) for run in failures) + " |")


if __name__ == "__main__":
    main()
