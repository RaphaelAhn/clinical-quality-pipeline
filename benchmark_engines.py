"""End-to-end run_batch time: reference Python engine vs vectorized SQL engine on the same input.

Measures the whole batch (CSV read, validation, staging, conflict checks, transactional publish).
The Python engine is slow at this size, so it runs once; the SQL engine runs --repeat times (median).
"""
import argparse
import json
import platform
import statistics
import tempfile
from pathlib import Path
from time import perf_counter

import duckdb

from clinical_pipeline import generate, run_batch


def timed(folder, db, day, engine):
    start = perf_counter()
    report = run_batch(folder, db, day, engine=engine)
    return perf_counter() - start, report


def main(rows_per_hospital: int, repeat: int, output: Path):
    day = "2026-09-25"
    with tempfile.TemporaryDirectory(prefix="clinical-bench-") as tmp:
        root = Path(tmp)
        generate(root / "landing", day, rows_per_hospital)
        py_seconds, py = timed(root / "landing", root / "python.duckdb", day, "python")
        sql_runs = [timed(root / "landing", root / f"sql{i}.duckdb", day, "sql") for i in range(repeat)]
    sql_seconds = statistics.median(s for s, _ in sql_runs)
    sql = sql_runs[0][1]
    assert all(r["snapshot_sha256"] == py["snapshot_sha256"] for _, r in sql_runs)
    result = {
        "environment": {"python": platform.python_version(), "platform": platform.platform(),
                        "duckdb": duckdb.__version__},
        "scope": "Synthetic data, local disk, whole run_batch incl. transactional publish",
        "input_rows": py["input_rows"],
        "python_engine_seconds": round(py_seconds, 2),
        "sql_engine_seconds_median": round(sql_seconds, 2),
        "sql_engine_runs": [round(s, 2) for s, _ in sql_runs],
        "speedup": round(py_seconds / sql_seconds, 1),
        "python_rows_per_second": round(py["input_rows"] / py_seconds),
        "sql_rows_per_second": round(sql["input_rows"] / sql_seconds),
        "same_snapshot_sha256": True,
        "same_counts_and_quarantine": {k: py[k] for k in ("accepted_rows", "duplicate_rows", "rejected_rows")}
                                      == {k: sql[k] for k in ("accepted_rows", "duplicate_rows", "rejected_rows")}
                                      and py["quarantine"] == sql["quarantine"],
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=500_000, help="rows per hospital")
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--output", type=Path, default=Path("evidence/engine-benchmark.json"))
    args = parser.parse_args()
    main(args.rows, args.repeat, args.output)
