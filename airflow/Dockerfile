FROM apache/airflow:3.1.0-python3.12
RUN pip install --no-cache-dir duckdb==1.4.4 great_expectations==1.23.2
COPY clinical_pipeline.py gx_check.py /opt/airflow/lib/
COPY airflow/clinical_daily.py /opt/airflow/dags/
COPY airflow/verify.sh /opt/airflow/verify.sh
ENV PYTHONPATH=/opt/airflow/lib CLINICAL_DATA_ROOT=/opt/airflow/clinical-data \
    AIRFLOW__CORE__LOAD_EXAMPLES=False GX_ANALYTICS_ENABLED=false TQDM_DISABLE=1
