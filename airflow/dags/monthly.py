import os
import yaml

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

from transform.process_handler import run_refined, run_api_deliver, run_sftp_deliver


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ["correa.igorx@aiqfome.com" ,"diego.pastrellox@aiqfome.com", "leonardo.arturx@aiqfome.com", "pdrals16@gmail.com"],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with open(os.path.join("dags/transform/config/", "tasks_monthly.yaml"), "r") as f:
    tasks_yaml = yaml.safe_load(f)
    with DAG(
        'monthly',
        default_args=default_args,
        description='Deliver monthly view.',
        schedule_interval="0 0 1 * *",
        start_date=datetime(2024, 11, 1),
        catchup=False,
    ) as dag:
        
        config_refined = tasks_yaml["context"]["refined"]
        for table_name, table_config in config_refined.items():
            with TaskGroup(group_id=table_name, dag=dag) as task:
                task_refined = PythonOperator(
                    task_id='refined',
                    python_callable=run_refined,
                    op_kwargs={
                        "view_sql_name": table_config["view_sql_name"],
                        "insert_sql_name": table_config["insert_sql_name"],
                        "target_db_name": table_config["target_db_name"]
                        }
                    )

                deliver_config = table_config["deliver"]
                task_api_deliver = PythonOperator(
                    task_id='api_deliver',
                    python_callable=run_api_deliver,
                    op_kwargs={
                        "table": table_name,
                        "db_name": deliver_config["db_name"],
                        "frequency": deliver_config["frequency"]
                    }
                )

                task_sftp_deliver = PythonOperator(
                    task_id='sftp_deliver',
                    python_callable=run_sftp_deliver,
                    op_kwargs={
                        "table": table_name,
                        "db_name": deliver_config["db_name"],
                        "frequency": deliver_config["frequency"]
                    }
                )

                task_refined >> [task_api_deliver, task_sftp_deliver]
