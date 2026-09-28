"""Post-load audit of a committed snapshot with Great Expectations (independent of run_batch checks)."""
import argparse
import json
from pathlib import Path

import duckdb
import great_expectations as gx
import great_expectations.expectations as E

TOKEN = r"^[0-9a-f]{64}$"  # HMAC-SHA256 hex; a raw SYN-* id here means de-identification leaked


def suites(expected_rows: int, year: int) -> dict:
    keys = ["person_id", "visit_id", "measurement_id"]
    return {
        "measurement_snapshot": [
            E.ExpectTableRowCountToEqual(value=expected_rows),
            E.ExpectCompoundColumnsToBeUnique(column_list=["batch_date", "hospital", "measurement_id"]),
            *[E.ExpectColumnValuesToNotBeNull(column=c) for c in keys + ["value"]],
            *[E.ExpectColumnValuesToMatchRegex(column=c, regex=TOKEN) for c in keys],
            E.ExpectColumnValuesToBeInSet(column="hospital", value_set=["A", "B"]),
            E.ExpectColumnValuesToBeInSet(column="unit", value_set=["mg/dL"]),
            E.ExpectColumnValuesToBeInSet(column="test_code", value_set=["DEMO_GLU"]),
            # 10000 mmol/L * 18 is the input-contract ceiling after conversion, not a clinical range.
            E.ExpectColumnValuesToBeBetween(column="value", min_value=0, max_value=180000),
        ],
        "patient_snapshot": [
            E.ExpectCompoundColumnsToBeUnique(column_list=["batch_date", "hospital", "person_id"]),
            E.ExpectColumnValuesToMatchRegex(column="person_id", regex=TOKEN),
            E.ExpectColumnValuesToBeBetween(column="birth_year", min_value=1900, max_value=year),
        ],
    }


def audit(database: Path, day: str) -> dict:
    with duckdb.connect(str(database), read_only=True) as conn:
        row = conn.execute("""SELECT metrics_json FROM runs WHERE batch_date=? AND status='COMMITTED'
                              ORDER BY recorded_at DESC LIMIT 1""", [day]).fetchone()
        if row is None:
            raise ValueError("no committed run for this date")
        expected = json.loads(row[0])["accepted_rows"]
        frames = {t: conn.execute(f"SELECT * FROM {t} WHERE batch_date=?", [day]).df()
                  for t in ("measurement_snapshot", "patient_snapshot")}
    # DuckDB DECIMAL arrives as Python Decimal objects; GX range checks need numbers.
    frames["measurement_snapshot"]["value"] = frames["measurement_snapshot"]["value"].astype(float)
    context = gx.get_context(mode="ephemeral")
    source = context.data_sources.add_pandas("clinical")
    report = {"batch_date": day, "success": True, "failed": []}
    for table, expectations in suites(expected, int(day[:4])).items():
        suite = context.suites.add(gx.ExpectationSuite(name=table, expectations=expectations))
        batch = source.add_dataframe_asset(table).add_batch_definition_whole_dataframe("all") \
            .get_batch(batch_parameters={"dataframe": frames[table]})
        result = batch.validate(suite)
        report[table] = {"evaluated": len(result.results),
                         "passed": sum(r.success for r in result.results)}
        for r in result.results:
            if not r.success:
                cfg = r.expectation_config
                report["failed"].append(f"{table}.{cfg.type}:{cfg.kwargs.get('column', '')}")
        report["success"] &= result.success
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--date", required=True)
    args = parser.parse_args()
    out = audit(args.database, args.date)
    print(json.dumps(out, indent=2))
    raise SystemExit(0 if out["success"] else 1)
