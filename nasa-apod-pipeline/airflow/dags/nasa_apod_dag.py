# airflow/dags/nasa_apod_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="nasa_apod_batch_pipeline",
    description="Daily NASA APOD extract → load → quality check",
    default_args=default_args,
    schedule="0 6 * * 1",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["nasa", "apod", "batch"],
) as dag:

    def extract(**context):
        import sys
        sys.path.insert(0, "/opt/airflow/pipeline")
        from extract import fetch_apod, save_raw
        target_date = str(context["logical_date"].date())
        data = fetch_apod(target_date)
        save_raw(data, target_date)

    def load(**context):
        import sys
        sys.path.insert(0, "/opt/airflow/pipeline")
        from load import load_file
        target_date = str(context["logical_date"].date())
        load_file(target_date)

    def quality(**context):
        import sys
        sys.path.insert(0, "/opt/airflow/pipeline")
        from quality_checks import run_checks
        target_date = str(context["logical_date"].date())
        run_checks(target_date)

    task_extract = PythonOperator(
        task_id="extract_apod",
        python_callable=extract,
    )

    task_load = PythonOperator(
        task_id="load_to_mysql",
        python_callable=load,
    )

    task_quality = PythonOperator(
        task_id="run_quality_checks",
        python_callable=quality,
    )

    task_extract >> task_load >> task_quality