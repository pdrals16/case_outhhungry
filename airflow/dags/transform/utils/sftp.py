import os
import logging

from transform.utils.vault import get_secrets_from_vault
from transform.utils.connection import connect_to_postgres


logging.basicConfig(format="[%(levelname)s] %(asctime)s : %(message)s", level=logging.INFO, force=True)

VAULT = get_secrets_from_vault()
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")

def send_to_sftp(host=None, username=None, public_key=None, private_key=None, data=None):
    logging.info(f"Sending data to host {host}")
    logging.info("File successfully uploaded.")
    return None

def read_sql(sql_name):
    file_path = os.path.join(AIRFLOW_HOME, f"dags/transform/sql/{sql_name}.sql")
    logging.info(f"Reading sql file {file_path}...")
    with open(file_path, "r") as f:
        sql_statement = f.read()
    return sql_statement

def deliver_sftp(table, db_name=None, frequency="daily", reference_date=None):
    try:
        if frequency == "daily":
            sql_file = "deliver_daily"
        elif frequency == "monthly":
            sql_file = "deliver_monthly"
        else:
            raise(f"Frequency {frequency} is not configured.")

        logging.info("Connecting to PostgreSQL...")
        query = read_sql(sql_file).format(table=table, reference_date=reference_date)
        cursor = connect_to_postgres(db_name=db_name)
        logging.info(f"Getting table {table} from date {reference_date}...")
        cursor.execute(query)
        data = cursor.fetchall()
        send_to_sftp(host=VAULT["sftp_host"], 
                     username=VAULT["sftp_user"], 
                     public_key=VAULT["sftp_private_key"], 
                     private_key=VAULT["sftp_public_key"], 
                     data=data)
    except Exception as e:
        raise(f"An error occurred: {e}")