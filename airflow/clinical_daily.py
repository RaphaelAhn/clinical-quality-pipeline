"""Airflow 3 adapter: inspect_source -> validate_and_publish -> gx_audit. Runtime evidence: airflow/verify.sh."""
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path

from airflow.sdk import dag, task, get_current_context
from airflow.exceptions import AirflowFailException
from airflow.timetables.interval import CronDataIntervalTimetable

from clinical_pipeline import PipelineError, input_fingerprint, run_batch


@dag(
    dag_id="clinical_daily_snapshot",
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone="UTC"),
    start_date=datetime(2026, 9, 25, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["synthetic-only", "data-quality"],
)
def clinical_daily_snapshot():
    @task
    def inspect_source():
        context = get_current_context()
        # Accept explicit dates only after ISO date validation in run_batch.
        batch_date = context["data_interval_start"].date().isoformat()
        root = Path(os.environ.get("CLINICAL_DATA_ROOT", "/opt/airflow/clinical-data"))
        folder = root / "landing" / batch_date
        try:
            checksum = input_fingerprint(folder)
        except PipelineError as exc:
            raise AirflowFailException(str(exc)) from None
        return {"date": batch_date, "folder": str(folder), "sha256": checksum,
                "database": str(root / "clinical.duckdb")}

    @task
    def validate_and_publish(source):
        try:
            report = run_batch(Path(source["folder"]), Path(source["database"]), source["date"],
                               expected_fingerprint=source["sha256"])
        except PipelineError as exc:
            if str(exc) == "INTERNAL_ERROR":
                raise
            raise AirflowFailException(str(exc)) from None
        # XCom contains only aggregate metadata, not source records or quarantine locators.
        return {k: report[k] for k in ("run_id", "batch_date", "status", "accepted_rows", "rejected_rows", "snapshot_sha256")}

    @task
    def gx_audit(published):
        from gx_check import audit  # heavy import only in the task that needs it
        root = Path(os.environ.get("CLINICAL_DATA_ROOT", "/opt/airflow/clinical-data"))
        report = audit(root / "clinical.duckdb", published["batch_date"])
        if not report["success"]:
            raise AirflowFailException("GX_AUDIT_FAILED:" + ",".join(report["failed"]))
        return {k: report[k] for k in ("batch_date", "success", "measurement_snapshot", "patient_snapshot")}

    gx_audit(validate_and_publish(inspect_source()))


clinical_daily_snapshot()
