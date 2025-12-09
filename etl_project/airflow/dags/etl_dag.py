from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from app.etl.pipeline.etl_job import run_etl_job

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'crypto_etl',
    default_args=default_args,
    description='A simple crypto ETL DAG',
    schedule_interval=timedelta(days=1),
)

run_etl = PythonOperator(
    task_id='run_etl_job',
    python_callable=run_etl_job,
    dag=dag,
)
