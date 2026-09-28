"""Synthetic-only, daily full-snapshot clinical ETL demonstrator."""
from __future__ import annotations

import argparse
import csv
import hashlib
import hmac
import json
import re
import uuid
from collections import Counter
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

import duckdb

DEMO_KEY = b"synthetic-demo-only-not-for-patient-data"
HOSPITALS = ("A", "B")
COLUMNS = ["patient_id", "birth_year", "visit_id", "visit_date", "measurement_id",
           "measurement_date", "test_code", "value", "unit"]
TABLES = ("patient_snapshot", "visit_snapshot", "measurement_snapshot")


class PipelineError(Exception):
    """Safe, fixed error codes only; never include source row contents."""


def token(key: bytes, hospital: str, kind: str, source_id: str) -> str:
    payload = json.dumps([hospital, kind, source_id], separators=(",", ":"))
    return hmac.new(key, payload.encode(), hashlib.sha256).hexdigest()


def parse_date(value: str, hospital: str) -> date:
    pattern, fmt = (r"\d{4}-\d{2}-\d{2}", "%Y-%m-%d") if hospital == "A" else (
        r"\d{4}/\d{2}/\d{2}", "%Y/%m/%d")
    if not re.fullmatch(pattern, value):
        raise ValueError("format")
    return datetime.strptime(value, fmt).date()


def normalize(row: dict, hospital: str, day: date, key: bytes) -> tuple | str:
    if any(row.get(c) is None or not str(row[c]).strip() for c in COLUMNS):
        return "MISSING_FIELD"
    row = {c: row[c].strip() for c in COLUMNS}
    for c in ("patient_id", "visit_id", "measurement_id"):
        if not re.fullmatch(r"SYN-[PVM]-[0-9]+", row[c]):
            return "NON_SYNTHETIC_ID"
    if not (row["patient_id"].startswith("SYN-P-") and
            row["visit_id"].startswith("SYN-V-") and
            row["measurement_id"].startswith("SYN-M-")):
        return "ID_KIND"
    try:
        visit_day = parse_date(row["visit_date"], hospital)
        measurement_day = parse_date(row["measurement_date"], hospital)
    except ValueError:
        return "INVALID_DATE"
    if visit_day != day or measurement_day != day:
        return "OUTSIDE_BATCH_DATE"
    if not re.fullmatch(r"\d{4}", row["birth_year"]):
        return "INVALID_BIRTH_YEAR"
    birth_year = int(row["birth_year"])
    if not 1900 <= birth_year <= day.year:
        return "INVALID_BIRTH_YEAR"
    # DEMO_GLU is a fictional source code, not an OMOP concept mapping.
    if row["test_code"] != "DEMO_GLU":
        return "UNKNOWN_TEST_CODE"
    if row["unit"] not in ("mg/dL", "mmol/L"):
        return "UNKNOWN_UNIT"
    try:
        value = Decimal(row["value"])
        if not value.is_finite() or value < 0 or value > 10000:
            return "INVALID_NUMERIC_VALUE"
        if row["unit"] == "mmol/L":
            value *= Decimal("18")
        value = value.quantize(Decimal("0.0001"))
    except InvalidOperation:
        return "INVALID_NUMERIC_VALUE"
    # Numeric bounds are a demo input contract, not a clinical reference range.
    return (day, hospital, token(key, hospital, "person", row["patient_id"]),
            birth_year, token(key, hospital, "visit", row["visit_id"]), visit_day,
            token(key, hospital, "measurement", row["measurement_id"]),
            measurement_day, "DEMO_GLU", value, "mg/dL")


def input_fingerprint(folder: Path) -> str:
    digest = hashlib.sha256()
    for hospital in HOSPITALS:
        path = folder / f"hospital_{hospital}.csv"
        if not path.is_file():
            raise PipelineError("MISSING_HOSPITAL_FILE")
        if not (folder / f"hospital_{hospital}.rows").is_file():
            raise PipelineError("MISSING_CONTROL_FILE")
        digest.update(hospital.encode())
        digest.update((folder / f"hospital_{hospital}.rows").read_bytes())
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
    return digest.hexdigest()


def write_control(folder: Path, hospital: str, rows: int):
    """Sender-side control file: data row count, excluding the header."""
    (folder / f"hospital_{hospital}.rows").write_text(f"{rows}\n", encoding="ascii")


def read_control(folder: Path, hospital: str) -> int:
    text = (folder / f"hospital_{hospital}.rows").read_text(encoding="ascii").strip()
    if not text.isdigit():
        raise PipelineError("INVALID_CONTROL_FILE")
    return int(text)


def init_db(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS config (name VARCHAR PRIMARY KEY, value VARCHAR);
        CREATE TABLE IF NOT EXISTS patient_snapshot (
          batch_date DATE, hospital VARCHAR, person_id VARCHAR, birth_year INTEGER,
          PRIMARY KEY(batch_date, hospital, person_id));
        CREATE TABLE IF NOT EXISTS visit_snapshot (
          batch_date DATE, hospital VARCHAR, visit_id VARCHAR, person_id VARCHAR,
          visit_date DATE, PRIMARY KEY(batch_date, hospital, visit_id));
        CREATE TABLE IF NOT EXISTS measurement_snapshot (
          batch_date DATE, hospital VARCHAR, measurement_id VARCHAR, visit_id VARCHAR,
          person_id VARCHAR, measurement_date DATE, test_code VARCHAR,
          value DECIMAL(18,4), unit VARCHAR,
          PRIMARY KEY(batch_date, hospital, measurement_id));
        CREATE TABLE IF NOT EXISTS runs (
          run_id VARCHAR PRIMARY KEY, batch_date DATE, status VARCHAR,
          source_sha256 VARCHAR, metrics_json VARCHAR, error_code VARCHAR,
          recorded_at TIMESTAMP DEFAULT current_timestamp);
    """)


def insert_stage(conn, rows):
    """Send one typed JSON batch; retain Decimal/date values as exact strings."""
    names = ("batch_date", "hospital", "person_id", "birth_year", "visit_id", "visit_date",
             "measurement_id", "measurement_date", "test_code", "value", "unit")
    types = ("DATE", "VARCHAR", "VARCHAR", "INTEGER", "VARCHAR", "DATE",
             "VARCHAR", "DATE", "VARCHAR", "DECIMAL(18,4)", "VARCHAR")
    payload = json.dumps([dict(zip(names, row)) for row in rows], default=str)
    schema = json.dumps([dict(zip(names, types))])
    conn.execute("INSERT INTO stage SELECT r.* FROM (SELECT unnest(from_json_strict(?,?)) AS r)",
                 [payload, schema])


def stage_python(conn, path: Path, hospital: str, day: date, key: bytes) -> tuple[int, list]:
    """Reference path: csv module + normalize() per row. Handles every malformed-file case."""
    rows, rejected, buffer = 0, [], []
    with path.open(encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream)
        for row in reader:
            rows += 1
            result = "COLUMN_COUNT" if None in row else normalize(row, hospital, day, key)
            if isinstance(result, str):
                rejected.append({"hospital": hospital, "line": reader.line_num, "reason": result})
            else:
                buffer.append(result)
            if len(buffer) >= 1000:
                insert_stage(conn, buffer)
                buffer.clear()
    if buffer:
        insert_stage(conn, buffer)
    return rows, rejected


def create_sql_token(conn, key: bytes):
    """token() as a native, multi-threaded DuckDB macro: HMAC-SHA256 = H(K^opad || H(K^ipad || msg)).
    Only fast-path ids (SYN-[PVM]-digits, hospital A/B, fixed kinds) reach it, so the string literal
    is byte-identical to token()'s json.dumps payload (no escaping possible)."""
    block = (hashlib.sha256(key).digest() if len(key) > 64 else key).ljust(64, b"\0")
    ipad, opad = (bytes(b ^ pad for b in block).hex() for pad in (0x36, 0x5C))
    conn.execute(f"""CREATE OR REPLACE TEMP MACRO demo_token(h, k, s) AS
        sha256(unhex('{opad}') || unhex(sha256(unhex('{ipad}') || encode('["' || h || '","' || k || '","' || s || '"]'))))""")


def physical_lines(path: Path) -> int:
    count, last = 0, b"\n"
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            count, last = count + chunk.count(b"\n"), chunk[-1:]
    return count + (last != b"\n")


def stage_sql(conn, path: Path, hospital: str, day: date, key: bytes) -> tuple[int, list] | None:
    """Vectorized path. Rows passing a strict ASCII fast predicate (a subset of what normalize()
    accepts, producing identical values) are converted in SQL; every other row goes through
    normalize(), so reject reasons and outputs match the reference path by construction.
    Returns None (caller falls back to stage_python) when line numbers could not be trusted."""
    columns = ", ".join(f"'{c}': 'VARCHAR'" for c in COLUMNS)
    try:
        conn.execute(f"""CREATE OR REPLACE TEMP TABLE raw AS
            SELECT row_number() OVER () + 1 AS line, * FROM read_csv(?, header=true, auto_detect=false,
              columns={{{columns}}}, delim=',', quote='"', escape='"', strict_mode=true)""", [str(path)])
    except duckdb.Error:
        return None  # e.g. wrong column count: the reference path reports it per row
    rows = conn.execute("SELECT count(*) FROM raw").fetchone()[0]
    # One physical line per record (no blank lines, no quoted newlines) keeps quarantine line numbers exact.
    if physical_lines(path) != rows + 1:
        return None
    date_re, fmt = ("[0-9]{4}-[0-9]{2}-[0-9]{2}", "%Y-%m-%d") if hospital == "A" else (
        "[0-9]{4}/[0-9]{2}/[0-9]{2}", "%Y/%m/%d")
    fast = f"""regexp_full_match(patient_id, 'SYN-P-[0-9]+') AND regexp_full_match(visit_id, 'SYN-V-[0-9]+')
      AND regexp_full_match(measurement_id, 'SYN-M-[0-9]+')
      AND regexp_full_match(visit_date, '{date_re}') AND try_strptime(visit_date, '{fmt}')::DATE = $day
      AND regexp_full_match(measurement_date, '{date_re}') AND try_strptime(measurement_date, '{fmt}')::DATE = $day
      AND regexp_full_match(birth_year, '[0-9]{{4}}') AND TRY_CAST(birth_year AS INTEGER) BETWEEN 1900 AND $year
      AND test_code = 'DEMO_GLU' AND unit IN ('mg/dL', 'mmol/L')
      AND regexp_full_match(value, '[0-9]{{1,5}}([.][0-9]{{1,4}})?') AND TRY_CAST(value AS DECIMAL(18,4)) <= 10000"""
    conn.execute(f"CREATE OR REPLACE TEMP TABLE raw_flag AS SELECT *, coalesce({fast}, false) AS fast FROM raw",
                 {"day": day, "year": day.year})
    conn.execute("""INSERT INTO stage SELECT $day, $h, demo_token($h, 'person', patient_id), birth_year::INTEGER,
        demo_token($h, 'visit', visit_id), $day, demo_token($h, 'measurement', measurement_id), $day, 'DEMO_GLU',
        (CASE unit WHEN 'mmol/L' THEN value::DECIMAL(18,4) * 18 ELSE value::DECIMAL(18,4) END)::DECIMAL(18,4), 'mg/dL'
        FROM raw_flag WHERE fast""", {"day": day, "h": hospital})
    rejected, buffer = [], []
    slow = conn.execute(f"SELECT line, {', '.join(COLUMNS)} FROM raw_flag WHERE NOT fast ORDER BY line").fetchall()
    for line, *values in slow:
        result = normalize(dict(zip(COLUMNS, values)), hospital, day, key)
        if isinstance(result, str):
            rejected.append({"hospital": hospital, "line": line, "reason": result})
        else:
            buffer.append(result)
    for i in range(0, len(buffer), 1000):
        insert_stage(conn, buffer[i:i + 1000])
    return rows, rejected


def digest_snapshot(conn, day: str) -> str:
    """Order-independent content hash computed inside DuckDB: per table, sha256 over the sorted
    per-row sha256 of the row text; then sha256 over the table digests. Rows are unique (primary keys),
    so this identifies the set of rows. Replaced a Python fetch + json.dumps that took 12.7 s at 1M rows."""
    parts = []
    for table in TABLES:
        # table names are a fixed internal allowlist, never user input.
        digest = conn.execute(f"""SELECT coalesce(sha256(string_agg(h, '' ORDER BY h)), '')
            FROM (SELECT sha256(t::VARCHAR) AS h FROM {table} t WHERE batch_date = ?)""", [day]).fetchone()[0]
        parts.append(f"{table}:{digest}")
    return hashlib.sha256("\n".join(parts).encode()).hexdigest()


def run_batch(folder: Path, database: Path, day: str, *, key: bytes = DEMO_KEY,
              max_reject_ratio: float = 0.1, fail_at: str | None = None,
              expected_fingerprint: str | None = None, engine: str = "sql") -> dict:
    batch_day = date.fromisoformat(day)
    if engine not in ("sql", "python"):
        raise ValueError("engine must be 'sql' or 'python'")
    if not 0 <= max_reject_ratio <= 1:
        raise ValueError("max_reject_ratio must be between 0 and 1")
    if len(key) < 16:
        raise ValueError("key must contain at least 16 bytes")
    if fail_at not in (None, "after_delete", "after_insert"):
        raise ValueError("unknown failure point")
    database.parent.mkdir(parents=True, exist_ok=True)
    run_id = uuid.uuid4().hex
    conn = duckdb.connect(str(database))
    source_hash = ""
    metrics = {"run_id": run_id, "batch_date": day, "input_rows": 0,
               "accepted_rows": 0, "duplicate_rows": 0, "rejected_rows": 0}
    in_transaction = False
    try:
        init_db(conn)
        source_hash = input_fingerprint(folder)
        if expected_fingerprint is not None and source_hash != expected_fingerprint:
            raise PipelineError("SOURCE_CHANGED")
        key_id = hmac.new(key, b"clinical-demo-key-version", hashlib.sha256).hexdigest()
        existing = conn.execute("SELECT value FROM config WHERE name='key_id'").fetchone()
        if existing and existing[0] != key_id:
            raise PipelineError("KEY_VERSION_MISMATCH")
        conn.execute("""CREATE TEMP TABLE stage (
          batch_date DATE, hospital VARCHAR, person_id VARCHAR, birth_year INTEGER,
          visit_id VARCHAR, visit_date DATE, measurement_id VARCHAR, measurement_date DATE,
          test_code VARCHAR, value DECIMAL(18,4), unit VARCHAR)""")
        rejected = []
        if engine == "sql":
            create_sql_token(conn, key)
        metrics["engine"] = {}
        for hospital in HOSPITALS:
            path = folder / f"hospital_{hospital}.csv"
            with path.open(encoding="utf-8", newline="") as stream:
                if next(csv.reader(stream), None) != COLUMNS:
                    raise PipelineError("SCHEMA_MISMATCH")
            staged = stage_sql(conn, path, hospital, batch_day, key) if engine == "sql" else None
            metrics["engine"][hospital] = "python" if staged is None else "sql"
            rows, bad = staged or stage_python(conn, path, hospital, batch_day, key)
            metrics["input_rows"] += rows
            rejected += bad
            # A truncated or overwritten landing file still parses cleanly; only the sender's count exposes it.
            if rows != read_control(folder, hospital):
                raise PipelineError("SOURCE_ROW_COUNT_MISMATCH")
        if input_fingerprint(folder) != source_hash:
            raise PipelineError("SOURCE_CHANGED")
        metrics["rejected_rows"] = len(rejected)
        metrics["reject_reasons"] = dict(Counter(r["reason"] for r in rejected))
        # Quarantine is a locator + fixed reason. No raw values are copied into audit artifacts.
        metrics["quarantine"] = rejected
        total = metrics["input_rows"]
        if total == 0:
            raise PipelineError("EMPTY_BATCH")
        if len(rejected) / total > max_reject_ratio:
            raise PipelineError("REJECT_RATIO_EXCEEDED")
        conn.execute("CREATE TEMP TABLE clean AS SELECT DISTINCT * FROM stage")
        clean_count = conn.execute("SELECT count(*) FROM clean").fetchone()[0]
        metrics["accepted_rows"] = clean_count
        metrics["duplicate_rows"] = total - len(rejected) - clean_count
        if conn.execute("SELECT count(DISTINCT hospital) FROM clean").fetchone()[0] != len(HOSPITALS):
            raise PipelineError("HOSPITAL_WITHOUT_VALID_ROWS")
        for fields, code in [
            ("hospital, measurement_id", "CONFLICTING_MEASUREMENT"),
        ]:
            if conn.execute(f"SELECT 1 FROM clean GROUP BY {fields} HAVING count(*) > 1 LIMIT 1").fetchone():
                raise PipelineError(code)
        if conn.execute("""SELECT 1 FROM clean GROUP BY hospital, person_id
                           HAVING count(DISTINCT birth_year)>1 LIMIT 1""").fetchone():
            raise PipelineError("CONFLICTING_PERSON")
        if conn.execute("""SELECT 1 FROM clean GROUP BY hospital, visit_id
                           HAVING count(DISTINCT person_id)>1 LIMIT 1""").fetchone():
            raise PipelineError("CONFLICTING_VISIT")

        conn.execute("BEGIN TRANSACTION")
        in_transaction = True
        conn.execute("INSERT INTO config VALUES ('key_id', ?) ON CONFLICT DO NOTHING", [key_id])
        for table in reversed(TABLES):
            conn.execute(f"DELETE FROM {table} WHERE batch_date = ?", [day])
        if fail_at == "after_delete":
            raise PipelineError("INJECTED_AFTER_DELETE")
        conn.execute("INSERT INTO patient_snapshot SELECT DISTINCT batch_date,hospital,person_id,birth_year FROM clean")
        conn.execute("INSERT INTO visit_snapshot SELECT DISTINCT batch_date,hospital,visit_id,person_id,visit_date FROM clean")
        conn.execute("""INSERT INTO measurement_snapshot SELECT batch_date,hospital,measurement_id,
                     visit_id,person_id,measurement_date,test_code,value,unit FROM clean""")
        if fail_at == "after_insert":
            raise PipelineError("INJECTED_AFTER_INSERT")
        orphans = conn.execute("""SELECT count(*) FROM measurement_snapshot m
          LEFT JOIN visit_snapshot v ON m.batch_date=v.batch_date AND m.hospital=v.hospital AND m.visit_id=v.visit_id
          LEFT JOIN patient_snapshot p ON m.batch_date=p.batch_date AND m.hospital=p.hospital AND m.person_id=p.person_id
          WHERE m.batch_date=? AND (v.visit_id IS NULL OR p.person_id IS NULL OR m.person_id<>v.person_id)""", [day]).fetchone()[0]
        if orphans:
            raise PipelineError("REFERENTIAL_INTEGRITY_FAILED")
        metrics["orphan_rows"] = orphans
        metrics["counts"] = {t: conn.execute(f"SELECT count(*) FROM {t} WHERE batch_date=?", [day]).fetchone()[0] for t in TABLES}
        if metrics["counts"]["measurement_snapshot"] != clean_count:
            raise PipelineError("ROW_RECONCILIATION_FAILED")
        metrics["snapshot_sha256"] = digest_snapshot(conn, day)
        metrics["source_sha256"] = source_hash
        metrics["status"] = "COMMITTED"
        conn.execute("INSERT INTO runs(run_id,batch_date,status,source_sha256,metrics_json,error_code) VALUES (?,?,?,?,?,?)",
                     [run_id, day, "COMMITTED", source_hash, json.dumps(metrics), None])
        conn.execute("COMMIT")
        in_transaction = False
        return metrics
    except Exception as exc:
        if in_transaction:
            conn.execute("ROLLBACK")
        code = str(exc) if isinstance(exc, PipelineError) else "INTERNAL_ERROR"
        conn.execute("INSERT INTO runs(run_id,batch_date,status,source_sha256,metrics_json,error_code) VALUES (?,?,?,?,?,?)",
                     [run_id, day, "FAILED", source_hash, json.dumps(metrics), code])
        raise PipelineError(code) from None
    finally:
        conn.close()


def generate(folder: Path, day: str, rows_per_hospital: int = 1000, dirty: bool = True) -> dict:
    if rows_per_hospital < 1:
        raise ValueError("rows_per_hospital must be positive")
    batch_day = date.fromisoformat(day)
    folder.mkdir(parents=True, exist_ok=True)
    manifest = {"synthetic_only": True, "batch_date": day, "expected_unique": 2 * rows_per_hospital,
                "expected_duplicates": 2 if dirty else 0, "expected_rejected": 6 if dirty else 0}
    for hospital in HOSPITALS:
        fmt = "%Y-%m-%d" if hospital == "A" else "%Y/%m/%d"
        rows = []
        for i in range(rows_per_hospital):
            rows.append(dict(zip(COLUMNS, [f"SYN-P-{i//2}", "1980", f"SYN-V-{i}", batch_day.strftime(fmt),
                f"SYN-M-{i}", batch_day.strftime(fmt), "DEMO_GLU",
                "90" if hospital == "A" else "5", "mg/dL" if hospital == "A" else "mmol/L"])))
        if dirty:
            rows.append(rows[0].copy())
            for field, value in (("patient_id", ""), ("measurement_date", "2026-02-30"), ("unit", "unknown")):
                bad = rows[0].copy()
                bad[field] = value
                rows.append(bad)
        with (folder / f"hospital_{hospital}.csv").open("w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=COLUMNS)
            writer.writeheader()
            writer.writerows(rows)
        write_control(folder, hospital, len(rows))
    (folder / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    gen = sub.add_parser("generate")
    gen.add_argument("--folder", type=Path, required=True)
    gen.add_argument("--date", required=True)
    gen.add_argument("--rows", type=int, default=1000)
    run = sub.add_parser("run")
    run.add_argument("--folder", type=Path, required=True)
    run.add_argument("--database", type=Path, required=True)
    run.add_argument("--date", required=True)
    run.add_argument("--max-reject-ratio", type=float, default=0.1)
    args = parser.parse_args()
    result = generate(args.folder, args.date, args.rows) if args.command == "generate" else run_batch(
        args.folder, args.database, args.date, max_reject_ratio=args.max_reject_ratio)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
