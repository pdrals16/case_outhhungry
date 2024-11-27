from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from transform.generate import run

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'generate',
    default_args=default_args,
    description='Generate some data.',
    schedule_interval="0 0 * * *",
    start_date=datetime(2024, 11, 1),
    catchup=False,
) as dag:

    task = PythonOperator(
        task_id='execute_generate',
        python_callable=run,
    )

    task
