"""Produce measured evidence in a fresh local directory, without network calls."""
import argparse
import csv
import json
import platform
import tempfile
from pathlib import Path
from time import perf_counter

import duckdb

from clinical_pipeline import COLUMNS, PipelineError, digest_snapshot, generate, run_batch, write_control
from gx_check import audit


def demonstrate(output: Path, rows: int):
    day = "2026-09-25"
    with tempfile.TemporaryDirectory(prefix="clinical-demo-") as tmp:
        root = Path(tmp)
        landing, db = root / "landing", root / "clinical.duckdb"
        manifest = generate(landing, day, rows)
        start = perf_counter()
        baseline = run_batch(landing, db, day)
        duration = perf_counter() - start
        replay = run_batch(landing, db, day)
        assert baseline["snapshot_sha256"] == replay["snapshot_sha256"]
        assert baseline["accepted_rows"] == manifest["expected_unique"]
        assert baseline["duplicate_rows"] == manifest["expected_duplicates"]
        assert baseline["rejected_rows"] == manifest["expected_rejected"]
        path = landing / "hospital_B.csv"
        with path.open(newline="", encoding="utf-8") as stream:
            records = list(csv.DictReader(stream))
        records = [r for r in records if r["measurement_id"] != f"SYN-M-{rows-1}"]
        with path.open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=COLUMNS)
            writer.writeheader()
            writer.writerows(records)
        write_control(landing, "B", len(records))
        delayed = run_batch(landing, db, day)
        assert delayed["accepted_rows"] == baseline["accepted_rows"] - 1
        generate(landing, day, rows)
        recovered_late = run_batch(landing, db, day)
        assert recovered_late["snapshot_sha256"] == baseline["snapshot_sha256"]
        rollback = {}
        for point in ("after_delete", "after_insert"):
            try:
                run_batch(landing, db, day, fail_at=point)
            except PipelineError as exc:
                assert str(exc) == "INJECTED_" + point.upper()
            else:
                raise AssertionError("failure injection did not fail")
            with duckdb.connect(str(db)) as conn:
                rollback[point] = digest_snapshot(conn, day) == baseline["snapshot_sha256"]
            assert rollback[point]
        retry = run_batch(landing, db, day)
        assert retry["snapshot_sha256"] == baseline["snapshot_sha256"]
        gx_report = audit(db, day)
        assert gx_report["success"], gx_report["failed"]
        with duckdb.connect(str(db)) as conn:
            statuses = dict(conn.execute("SELECT status,count(*) FROM runs GROUP BY status").fetchall())
        result = {
            "environment": {"python": platform.python_version(), "platform": platform.platform(), "duckdb": duckdb.__version__},
            "scope": "Synthetic local DuckDB demo; no Docker/Airflow runtime verification",
            "baseline": baseline, "baseline_seconds": round(duration, 4),
            "input_rows_per_second": round(baseline["input_rows"] / duration, 2),
            "same_input_same_snapshot": True, "late_arrival_matches_baseline": True,
            "rollback_preserves_snapshot": rollback, "retry_matches_baseline": True,
            "run_status_counts": statuses, "great_expectations_audit": gx_report,
        }
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(result, indent=2), encoding="utf-8")
        print(json.dumps({k: v for k, v in result.items() if k != "baseline"}, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path("evidence/demo.json"))
    parser.add_argument("--rows", type=int, default=1000)
    args = parser.parse_args()
    demonstrate(args.output, args.rows)
