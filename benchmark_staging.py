"""Compare identical normalized rows through row-wise and typed JSON batch inserts."""
import argparse
import hashlib
import json
from datetime import date
from decimal import Decimal
from pathlib import Path
from statistics import median
from time import perf_counter

import duckdb

from clinical_pipeline import insert_stage


def benchmark(rows_count=1000):
    rows = [(date(2026, 9, 25), "A", str(i), 1980, str(i), date(2026, 9, 25),
             str(i), date(2026, 9, 25), "DEMO_GLU", Decimal("90"), "mg/dL") for i in range(rows_count)]
    samples = {"rowwise": [], "json_batch": []}
    hashes = set()
    for repeat in range(3):
        for mode in (list(samples) if repeat % 2 == 0 else list(reversed(samples))):
            with duckdb.connect() as conn:
                conn.execute("""CREATE TABLE stage (batch_date DATE,hospital VARCHAR,person_id VARCHAR,
                birth_year INTEGER,visit_id VARCHAR,visit_date DATE,measurement_id VARCHAR,
                measurement_date DATE,test_code VARCHAR,value DECIMAL(18,4),unit VARCHAR)""")
                start = perf_counter()
                if mode == "rowwise":
                    conn.executemany("INSERT INTO stage VALUES (?,?,?,?,?,?,?,?,?,?,?)", rows)
                else:
                    for begin in range(0, len(rows), 1000):
                        insert_stage(conn, rows[begin:begin+1000])
                samples[mode].append(perf_counter() - start)
                result = conn.execute("SELECT * FROM stage ORDER BY ALL").fetchall()
                assert len(result) == rows_count
                hashes.add(hashlib.sha256(str(result).encode()).hexdigest())
    assert len(hashes) == 1
    medians = {k: median(v) for k, v in samples.items()}
    return {"rows": rows_count, "repeats": 3, "duckdb": duckdb.__version__,
            "scope": "In-memory staging only, pre-normalized synthetic rows; excludes CSV parsing, HMAC, DQ and disk commit",
            "seconds": samples, "median_seconds": medians,
            "rowwise_over_json_batch_ratio": medians["rowwise"] / medians["json_batch"],
            "identical_sorted_output": True, "output_sha256": hashes.pop()}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=1000)
    parser.add_argument("--output", type=Path, default=Path("evidence/staging-benchmark.json"))
    args = parser.parse_args()
    result = benchmark(args.rows)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))
