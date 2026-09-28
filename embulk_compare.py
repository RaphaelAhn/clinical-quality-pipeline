"""Embulk ingestion path vs direct CSV path: the published snapshot must be identical.

Requires Docker. Builds ./embulk, lands each hospital file through Embulk (network disabled),
runs run_batch on both folders and records the comparison in evidence/embulk-compare.json.
"""
import argparse
import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from time import perf_counter

from clinical_pipeline import HOSPITALS, generate, run_batch
from gx_check import audit

IMAGE = "clinical-embulk"


def embulk_land(raw: Path, landing: Path) -> dict:
    landing.mkdir(parents=True, exist_ok=True)
    seconds = {}
    for hospital in HOSPITALS:
        start = perf_counter()
        # On Linux the container must write landing/ as the host user; Docker Desktop maps ownership itself.
        user = ["--user", f"{os.getuid()}:{os.getgid()}", "-e", "HOME=/tmp"] if hasattr(os, "getuid") else []
        subprocess.run(["docker", "run", "--rm", "--network", "none", *user,
                        "-v", f"{raw.resolve()}:/data/raw:ro", "-v", f"{landing.resolve()}:/data/landing",
                        IMAGE, f"/opt/configs/hospital_{hospital}.yml"],
                       check=True, capture_output=True)
        seconds[hospital] = round(perf_counter() - start, 2)
        # The sender's control file travels beside the data; Embulk only lands the CSV.
        shutil.copyfile(raw / f"hospital_{hospital}.rows", landing / f"hospital_{hospital}.rows")
    return seconds


def compare(rows: int, output: Path):
    day = "2026-09-25"
    subprocess.run(["docker", "build", "-q", "-t", IMAGE, str(Path(__file__).parent / "embulk")],
                   check=True, capture_output=True)
    with tempfile.TemporaryDirectory(prefix="clinical-embulk-", dir=Path(__file__).parent) as tmp:
        root = Path(tmp)
        raw, landing = root / "raw", root / "landing"
        manifest = generate(raw, day, rows)
        embulk_seconds = embulk_land(raw, landing)
        direct = run_batch(raw, root / "direct.duckdb", day)
        via_embulk = run_batch(landing, root / "embulk.duckdb", day)
        keys = ("input_rows", "accepted_rows", "duplicate_rows", "rejected_rows", "reject_reasons", "counts")
        result = {
            "scope": "Synthetic data; Embulk 0.11.5 in Docker (network none) lands raw CSV; same run_batch on both paths",
            "rows_per_hospital": rows,
            "manifest": manifest,
            "raw_bytes_differ": any((raw / f"hospital_{h}.csv").read_bytes() != (landing / f"hospital_{h}.csv").read_bytes()
                                    for h in HOSPITALS),
            "direct": {k: direct[k] for k in keys} | {"snapshot_sha256": direct["snapshot_sha256"]},
            "via_embulk": {k: via_embulk[k] for k in keys} | {"snapshot_sha256": via_embulk["snapshot_sha256"]},
            "snapshot_identical": direct["snapshot_sha256"] == via_embulk["snapshot_sha256"],
            "embulk_seconds_per_hospital": embulk_seconds,
            "gx_audit_via_embulk": audit(root / "embulk.duckdb", day)["success"],
        }
    assert result["snapshot_identical"] and result["gx_audit_via_embulk"], result
    assert all(result["direct"][k] == result["via_embulk"][k] for k in keys)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=5000)
    parser.add_argument("--output", type=Path, default=Path("evidence/embulk-compare.json"))
    args = parser.parse_args()
    compare(args.rows, args.output)
