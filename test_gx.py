import duckdb

from clinical_pipeline import generate, run_batch
from gx_check import audit

DAY = "2026-09-25"


def committed(tmp_path):
    folder, db = tmp_path / "landing", tmp_path / "clinical.duckdb"
    generate(folder, DAY, 40)
    run_batch(folder, db, DAY)
    return db


def test_committed_snapshot_passes_gx(tmp_path):
    report = audit(committed(tmp_path), DAY)
    assert report["success"], report["failed"]
    assert report["measurement_snapshot"]["passed"] == report["measurement_snapshot"]["evaluated"] == 13


def test_gx_catches_leaked_raw_id_and_out_of_contract_value(tmp_path):
    db = committed(tmp_path)
    with duckdb.connect(str(db)) as conn:  # simulate a bad write that bypassed run_batch
        conn.execute("""UPDATE measurement_snapshot SET measurement_id='SYN-M-1', value=999999
                        WHERE rowid = (SELECT min(rowid) FROM measurement_snapshot)""")
    report = audit(db, DAY)
    assert not report["success"]
    assert "measurement_snapshot.expect_column_values_to_match_regex:measurement_id" in report["failed"]
    assert "measurement_snapshot.expect_column_values_to_be_between:value" in report["failed"]


def test_gx_catches_row_count_drift(tmp_path):
    db = committed(tmp_path)
    with duckdb.connect(str(db)) as conn:
        conn.execute("DELETE FROM measurement_snapshot WHERE rowid = (SELECT min(rowid) FROM measurement_snapshot)")
    assert "measurement_snapshot.expect_table_row_count_to_equal:" in audit(db, DAY)["failed"]
