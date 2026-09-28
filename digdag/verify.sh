#!/usr/bin/env bash
# Runtime acceptance for clinical.dig in local mode (`digdag run`, no server/scheduler).
# Scenario: 9/25 ok, 9/26 truncated sender file -> fails at +publish and alerts,
# sender re-sends -> 9/26 ok, 9/25 re-run -> same snapshot hash.
set -uo pipefail
gen() { python -c "from pathlib import Path; from clinical_pipeline import generate; generate(Path('/data/$1/raw'), '$1', 500)"; }
run() { echo "== digdag run session=$1"; digdag run clinical.dig --session "$1" -a 2>&1 \
          | grep -E "Started|Task failed|error:|SOURCE_ROW|Success|Failed|ALERT" | sed 's/^[0-9: +.-]*//' | tail -n 6; }
digdag() { java -jar /opt/digdag.jar "$@"; }

gen 2026-09-25; gen 2026-09-26
run 2026-09-25

echo "== truncate 9/26 raw hospital_B (sender count file unchanged)"
python -c "
p = '/data/2026-09-26/raw/hospital_B.csv'
lines = open(p).read().splitlines(True); open(p, 'w').writelines(lines[:51])"
run 2026-09-26

echo "== sender re-sends 9/26"; gen 2026-09-26
run 2026-09-26
run 2026-09-25

echo "== alerts.log"; cat /data/alerts.log 2>/dev/null || echo "(none)"
echo "== pipeline runs table (batch_date, status, error_code, snapshot_sha256)"
python -c "
import duckdb, json
c = duckdb.connect('/data/clinical.duckdb', read_only=True)
print(json.dumps(c.execute(\"\"\"SELECT batch_date::VARCHAR, status, error_code, json_extract_string(metrics_json,'\$.snapshot_sha256')
  FROM runs ORDER BY recorded_at\"\"\").fetchall(), indent=1))"
