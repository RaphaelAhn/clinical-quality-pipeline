import csv
import json
from decimal import Decimal

import duckdb
import pytest

from clinical_pipeline import (COLUMNS, DEMO_KEY, PipelineError, digest_snapshot,
                               generate, input_fingerprint, normalize, run_batch, token,
                               write_control)
from datetime import date

DAY = "2026-09-25"


@pytest.fixture
def batch(tmp_path):
    folder = tmp_path / "landing"
    generate(folder, DAY, 40)
    return folder, tmp_path / "clinical.duckdb"


def rewrite(folder, hospital, change, control=True):
    """Sender re-exports the file; control=False simulates a file damaged after the count was written."""
    path = folder / f"hospital_{hospital}.csv"
    with path.open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    change(rows)
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    if control:
        write_control(folder, hospital, len(rows))


def current_hash(db, day=DAY):
    with duckdb.connect(str(db)) as conn:
        return digest_snapshot(conn, day)


def test_reconciliation_against_generator_manifest(batch):
    folder, db = batch
    expected = json.loads((folder / "manifest.json").read_text())
    report = run_batch(folder, db, DAY)
    assert report["accepted_rows"] == expected["expected_unique"] == 80
    assert report["duplicate_rows"] == expected["expected_duplicates"] == 2
    assert report["rejected_rows"] == expected["expected_rejected"] == 6
    assert report["input_rows"] == sum(report[k] for k in ("accepted_rows", "duplicate_rows", "rejected_rows"))
    assert report["counts"] == {"patient_snapshot": 40, "visit_snapshot": 80, "measurement_snapshot": 80}
    assert report["orphan_rows"] == 0


def test_units_and_hospital_namespaces(batch):
    folder, db = batch
    run_batch(folder, db, DAY)
    with duckdb.connect(str(db)) as conn:
        assert conn.execute("SELECT DISTINCT value,unit FROM measurement_snapshot").fetchall() == [(Decimal("90"), "mg/dL")]
        assert conn.execute("SELECT count(DISTINCT person_id) FROM patient_snapshot").fetchone()[0] == 40
    assert token(DEMO_KEY, "A", "person", "SYN-P-0") != token(DEMO_KEY, "B", "person", "SYN-P-0")


def test_same_input_three_replays(batch):
    folder, db = batch
    reports = [run_batch(folder, db, DAY) for _ in range(3)]
    assert len({r["snapshot_sha256"] for r in reports}) == 1
    assert len({r["run_id"] for r in reports}) == 3


@pytest.mark.parametrize("point", ["after_delete", "after_insert"])
def test_failure_rolls_back_and_retry_commits(batch, point):
    folder, db = batch
    old = run_batch(folder, db, DAY)
    rewrite(folder, "B", lambda rows: [r.update(value="6") for r in rows])
    with pytest.raises(PipelineError, match="INJECTED"):
        run_batch(folder, db, DAY, fail_at=point)
    assert current_hash(db) == old["snapshot_sha256"]
    with duckdb.connect(str(db)) as conn:
        assert conn.execute("SELECT count(*) FROM runs WHERE status='FAILED'").fetchone()[0] == 1
    new = run_batch(folder, db, DAY)
    assert new["snapshot_sha256"] != old["snapshot_sha256"]


def test_late_arrival_reaches_full_baseline(batch):
    folder, db = batch
    baseline = run_batch(folder, db, DAY)
    rewrite(folder, "A", lambda rows: rows.__setitem__(slice(None), [r for r in rows if r["measurement_id"] != "SYN-M-39"]))
    before = run_batch(folder, db, DAY)
    assert before["accepted_rows"] == 79
    generate(folder, DAY, 40)
    after = run_batch(folder, db, DAY)
    assert after["snapshot_sha256"] == baseline["snapshot_sha256"]


@pytest.mark.parametrize("kind", ["measurement", "person", "visit"])
def test_conflicting_entities_block_whole_batch(batch, kind):
    folder, db = batch
    old = run_batch(folder, db, DAY)
    def change(rows):
        if kind == "measurement":
            rows.append(dict(rows[0], value="91"))
        elif kind == "person":
            rows[1]["birth_year"] = "1981"
        else:
            rows[2]["visit_id"] = rows[0]["visit_id"]
    rewrite(folder, "A", change)
    with pytest.raises(PipelineError, match="CONFLICTING"):
        run_batch(folder, db, DAY)
    assert current_hash(db) == old["snapshot_sha256"]


def test_quality_gate_preserves_previous_data(batch):
    folder, db = batch
    old = run_batch(folder, db, DAY)
    rewrite(folder, "A", lambda rows: [r.update(unit="bad") for r in rows])
    with pytest.raises(PipelineError, match="REJECT_RATIO_EXCEEDED"):
        run_batch(folder, db, DAY)
    assert current_hash(db) == old["snapshot_sha256"]


def test_missing_hospital_preserves_previous_data(batch):
    folder, db = batch
    old = run_batch(folder, db, DAY)
    (folder / "hospital_B.csv").rename(folder / "hospital_B.csv.held")
    with pytest.raises(PipelineError, match="MISSING_HOSPITAL_FILE"):
        run_batch(folder, db, DAY)
    assert current_hash(db) == old["snapshot_sha256"]


def test_truncated_landing_file_blocked_by_control_count(batch):
    # Reproduces the Embulk overwrite incident: a clean-parsing file that lost most of its rows.
    folder, db = batch
    old = run_batch(folder, db, DAY)
    rewrite(folder, "B", lambda rows: rows.__delitem__(slice(10, None)), control=False)
    with pytest.raises(PipelineError, match="SOURCE_ROW_COUNT_MISMATCH"):
        run_batch(folder, db, DAY)
    assert current_hash(db) == old["snapshot_sha256"]


@pytest.mark.parametrize("content,code", [(None, "MISSING_CONTROL_FILE"), ("ten", "INVALID_CONTROL_FILE")])
def test_control_file_required_and_numeric(batch, content, code):
    folder, db = batch
    control = folder / "hospital_A.rows"
    control.unlink() if content is None else control.write_text(content)
    with pytest.raises(PipelineError, match=code):
        run_batch(folder, db, DAY)


def test_empty_hospital_blocked_even_if_threshold_allows(batch):
    folder, db = batch
    rewrite(folder, "B", lambda rows: rows.clear())
    with pytest.raises(PipelineError, match="HOSPITAL_WITHOUT_VALID_ROWS"):
        run_batch(folder, db, DAY)


def test_empty_batch_blocked(batch):
    folder, db = batch
    for hospital in ("A", "B"):
        rewrite(folder, hospital, lambda rows: rows.clear())
    with pytest.raises(PipelineError, match="EMPTY_BATCH"):
        run_batch(folder, db, DAY)


def test_source_changed_between_tasks(batch):
    folder, db = batch
    fingerprint = input_fingerprint(folder)
    rewrite(folder, "A", lambda rows: rows.append(rows[0].copy()))
    with pytest.raises(PipelineError, match="SOURCE_CHANGED"):
        run_batch(folder, db, DAY, expected_fingerprint=fingerprint)


def test_key_change_blocked(batch):
    folder, db = batch
    old = run_batch(folder, db, DAY)
    with pytest.raises(PipelineError, match="KEY_VERSION_MISMATCH"):
        run_batch(folder, db, DAY, key=b"different-synthetic-key-123")
    assert current_hash(db) == old["snapshot_sha256"]


def test_other_date_preserved(batch):
    folder, db = batch
    old = run_batch(folder, db, DAY)
    generate(folder, "2026-09-26", 40)
    run_batch(folder, db, "2026-09-26")
    assert current_hash(db) == old["snapshot_sha256"]


def test_raw_identifiers_not_in_tables_or_audit(batch):
    folder, db = batch
    report = run_batch(folder, db, DAY)
    assert "SYN-P-" not in json.dumps(report)
    with duckdb.connect(str(db)) as conn:
        for table in ("patient_snapshot", "visit_snapshot", "measurement_snapshot", "runs"):
            assert "SYN-P-" not in str(conn.execute(f"SELECT * FROM {table}").fetchall())


@pytest.mark.parametrize("field,value,reason", [
    ("measurement_date", "2026-02-30", "INVALID_DATE"),
    ("measurement_date", "09/25/2026", "INVALID_DATE"),
    ("measurement_date", "2026-09-26", "OUTSIDE_BATCH_DATE"),
    ("value", "NaN", "INVALID_NUMERIC_VALUE"),
    ("value", "Infinity", "INVALID_NUMERIC_VALUE"),
    ("value", "-1", "INVALID_NUMERIC_VALUE"),
    ("value", "1e99999", "INVALID_NUMERIC_VALUE"),
    ("value", "oops", "INVALID_NUMERIC_VALUE"),
    ("patient_id", "   ", "MISSING_FIELD"),
    ("patient_id", "real-patient", "NON_SYNTHETIC_ID"),
    ("patient_id", "SYN-V-0", "ID_KIND"),
    ("birth_year", "2027", "INVALID_BIRTH_YEAR"),
    ("birth_year", "1980.0", "INVALID_BIRTH_YEAR"),
    ("test_code", "unmapped", "UNKNOWN_TEST_CODE"),
    ("unit", "g/L", "UNKNOWN_UNIT"),
])
def test_bad_values_have_fixed_reason(field, value, reason):
    row = dict(zip(COLUMNS, ["SYN-P-0", "1980", "SYN-V-0", DAY, "SYN-M-0", DAY, "DEMO_GLU", "90", "mg/dL"]))
    row[field] = value
    assert normalize(row, "A", date.fromisoformat(DAY), DEMO_KEY) == reason


def test_leap_day_valid(tmp_path):
    generate(tmp_path, "2024-02-29", 40)
    assert run_batch(tmp_path, tmp_path / "db.duckdb", "2024-02-29")["accepted_rows"] == 80


def test_schema_drift_rejected(batch):
    folder, db = batch
    path = folder / "hospital_A.csv"
    path.write_text(path.read_text().replace("patient_id", "patient_name", 1))
    with pytest.raises(PipelineError, match="SCHEMA_MISMATCH"):
        run_batch(folder, db, DAY)


def test_buffer_boundary_and_remainder(tmp_path):
    generate(tmp_path, DAY, 1003)
    report = run_batch(tmp_path, tmp_path / "db.duckdb", DAY)
    assert report["accepted_rows"] == 2006
    assert report["duplicate_rows"] == 2
    assert report["rejected_rows"] == 6


TRICKY = [" 90", "90 ", "\t90", "５", "1e3", "+5", "5.", ".5", "90.00005", "1_000", "10000.0001", "0", "00090"]


def append_raw(folder, hospital, text):
    with (folder / f"hospital_{hospital}.csv").open("a", encoding="utf-8", newline="") as stream:
        stream.write(text)


@pytest.mark.parametrize("scenario,engine_used", [
    ("dirty_default", "sql"),
    ("tricky_values", "sql"),
    ("quoted_comma", "sql"),
    ("blank_line", "python"),
    ("extra_column", "python"),
    ("quoted_newline", "python"),
])
def test_sql_engine_matches_reference_python_engine(tmp_path, scenario, engine_used):
    folder = tmp_path / "landing"
    generate(folder, DAY, 40)
    def tricky(rows):
        for i, v in enumerate(TRICKY):
            rows.append(dict(rows[0], measurement_id=f"SYN-M-{900 + i}", visit_id=f"SYN-V-{900 + i}", value=v))
        rows.append(dict(rows[0], measurement_id="SYN-M-950", visit_id="SYN-V-950", unit="MG/DL"))
        rows.append(dict(rows[0], measurement_id="SYN-M-951", visit_id="SYN-V-951", visit_date="2026-9-25"))
        rows.append(dict(rows[0], measurement_id="SYN-M-952", visit_id="SYN-V-952", birth_year="１９８０"))
        rows.append(dict(rows[0], measurement_id=" SYN-M-953", visit_id="SYN-V-953"))
    if scenario == "tricky_values":
        rewrite(folder, "A", tricky)
        rewrite(folder, "B", tricky)
    elif scenario == "quoted_comma":
        rewrite(folder, "A", lambda rows: rows.append(dict(rows[0], measurement_id="SYN-M-960", value="9,0")))
    raw_extra = {"blank_line": "\r\n", "extra_column": "SYN-P-1,1980,SYN-V-970,2026-09-25,SYN-M-970,2026-09-25,DEMO_GLU,90,mg/dL,x\r\n",
                 "quoted_newline": 'SYN-P-1,1980,SYN-V-971,2026-09-25,SYN-M-971,2026-09-25,"DEMO\nGLU",90,mg/dL\r\n'}
    if scenario in raw_extra:
        append_raw(folder, "A", raw_extra[scenario])
        write_control(folder, "A", 44 + (scenario != "blank_line"))
    reports = {}
    for engine in ("python", "sql"):
        reports[engine] = run_batch(folder, tmp_path / f"{engine}.duckdb", DAY, engine=engine)
    assert reports["sql"]["engine"]["A"] == engine_used
    comparable = lambda r: {k: v for k, v in r.items() if k not in ("run_id", "engine")}
    assert comparable(reports["sql"]) == comparable(reports["python"])


def test_first_failed_commit_does_not_pin_key(batch):
    folder, db = batch
    with pytest.raises(PipelineError, match="INJECTED_AFTER_INSERT"):
        run_batch(folder, db, DAY, fail_at="after_insert")
    with duckdb.connect(str(db)) as conn:
        assert conn.execute("SELECT count(*) FROM config").fetchone()[0] == 0
        assert conn.execute("SELECT count(*) FROM measurement_snapshot").fetchone()[0] == 0
    assert run_batch(folder, db, DAY, key=b"another-test-key-12345")["accepted_rows"] == 80
