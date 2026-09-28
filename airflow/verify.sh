#!/usr/bin/env bash
# Runtime acceptance inside the Airflow image (SQLite metadata DB, `airflow dags test`, no scheduler).
# Scenario: 9/25 and 9/26 runs, 9/25 re-run (same hash), 9/26 truncated file -> fail without retry, data kept.
set -euo pipefail
export NO_COLOR=1
ROOT="$CLINICAL_DATA_ROOT"
airflow db migrate > /dev/null
echo "== import errors"; airflow dags list-import-errors
python -c "
from pathlib import Path; from clinical_pipeline import generate
for d in ('2026-09-25', '2026-09-26'): generate(Path('$ROOT/landing')/d, d, 500)"

state() { python -c "
import duckdb, json
c = duckdb.connect('$ROOT/clinical.duckdb', read_only=True)
print(json.dumps(c.execute(\"\"\"SELECT batch_date::VARCHAR, status, error_code, json_extract_string(metrics_json,'\$.snapshot_sha256')
  FROM runs ORDER BY recorded_at\"\"\").fetchall()))"; }

# logical date D processes the interval [D-1, D): logical 9/26 -> batch 9/25.
for d in 2026-09-26 2026-09-27 2026-09-26; do
  echo "== dags test logical=$d"
  airflow dags test clinical_daily_snapshot "$d" 2>&1 | grep -E "Marking run|state=|GX_AUDIT|SOURCE_|success" | tail -n 5 || true
done

echo "== truncate 9/26 hospital_B without updating its control file"
python -c "
p = '$ROOT/landing/2026-09-26/hospital_B.csv'
lines = open(p).read().splitlines(True); open(p, 'w').writelines(lines[:51])"
airflow dags test clinical_daily_snapshot 2026-09-27 2>&1 | grep -E "Marking run|state=|SOURCE_ROW_COUNT_MISMATCH|AirflowFailException" | tail -n 5 || true

echo "== pipeline runs table (batch_date, status, error_code, snapshot_sha256)"
state
