import os
import yaml

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

from transform.process_handler import run_staged, run_trusted, run_refined, run_api_deliver, run_sftp_deliver


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ["correa.igor@aiqfome.com" ,"diego.pastrello@aiqfome.com", "leonardo.artur@aiqfome.com", "pdrals16@gmail.com"],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with open(os.path.join("dags/transform/config/", "tasks_ingest.yaml"), "r") as f:
    tasks_yaml = yaml.safe_load(f)
    
    with DAG(
        'ingest',
        default_args=default_args,
        description='Ingest data',
        schedule_interval=tasks_yaml["schedule_interval"],
        start_date=datetime(2024, 11, 1),
        catchup=False,
    ) as dag:

        list_task_trusted = []
        contexts = tasks_yaml["contexts"]
        for context, config in contexts.items():
            config_staged = config["staged"]
            config_trusted = config["trusted"]
            config_deliver = config["deliver"]

            with TaskGroup(group_id=context, dag=dag) as task:
                task_staged = PythonOperator(
                    task_id='staged',
                    python_callable=run_staged,
                    op_kwargs={
                        "context": context,
                        "mode": config_staged["mode"],
                        "source_file": config_staged["source_file"],
                        "ingest_sql_name": config_staged["ingest_sql_name"],
                        "insert_sql_name": config_staged["insert_sql_name"],
                        "target_db_name": config_staged["target_db_name"]
                    }
                )

                task_trusted = PythonOperator(
                    task_id='trusted',
                    python_callable=run_trusted,
                    op_kwargs={
                        "context": context,
                        "mode": config_trusted["mode"],
                        "insert_sql_name": config_trusted["insert_sql_name"],
                        "target_db_name": config_trusted["target_db_name"]
                    }
                )
                list_task_trusted.append(task_trusted)

                if config_deliver["api"]:
                    task_api_deliver = PythonOperator(
                        task_id='api_deliver',
                        python_callable=run_api_deliver,
                        op_kwargs={
                            "table": config_deliver["table"],
                            "db_name": config_deliver["db_name"],
                            "frequency": config_deliver["frequency"]
                        }
                    )
                    task_api_deliver.set_upstream(task_trusted)

                if config_deliver["sftp"]:
                    task_sftp_deliver = PythonOperator(
                        task_id='sftp_deliver',
                        python_callable=run_sftp_deliver,
                        op_kwargs={
                            "table": config_deliver["table"],
                            "db_name": config_deliver["db_name"],
                            "frequency": config_deliver["frequency"]
                        }
                    )
                    task_sftp_deliver.set_upstream(task_trusted)

                task_staged >> task_trusted
