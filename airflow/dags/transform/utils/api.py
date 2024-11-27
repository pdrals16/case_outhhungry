import os
import logging

from transform.utils.vault import get_secrets_from_vault
from transform.utils.connection import connect_to_postgres

logging.basicConfig(format="[%(levelname)s] %(asctime)s : %(message)s", level=logging.INFO, force=True)

VAULT = get_secrets_from_vault()
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
ENDPOINT_URL = "https://outhhungry.com/api/data"
BATCH_SIZE = 10

def fetch_data(query, db_name, batch_size):
    cursor = connect_to_postgres(db_name=db_name)
    cursor.execute(query)
    while True:
        batch = cursor.fetchmany(batch_size)
        logging.info(batch)
        if not batch:
            logging.info("There is no batches!")
            break
        yield batch
    
    cursor.close()

def send_data_to_endpoint(data, user, password):
    logging.info(f"Sending data {data} to endpoint {ENDPOINT_URL} with credencials USER: {user} and PASSWORD: {password}...")
    return {"status":200}

def read_sql(sql_name):
    file_path = os.path.join(AIRFLOW_HOME, f"dags/transform/sql/{sql_name}.sql")
    logging.info(f"Reading sql file {file_path}...")
    with open(file_path, "r") as f:
        sql_statement = f.read()
    return sql_statement

def deliver_api(table=None, db_name=None, frequency='daily', reference_date=None):
    if frequency == "daily":
        sql_file = "deliver_daily"
    elif frequency == "monthly":
        sql_file = "deliver_monthly"
    else:
        raise(f"Frequency {frequency} is not configured.")
    
    query = read_sql(sql_file).format(table=table, reference_date=reference_date)
    total_sent = 0
    
    try:
        for batch in fetch_data(query, db_name, BATCH_SIZE):
            response = send_data_to_endpoint(batch, user=VAULT["api_user"], password=VAULT["api_password"])
            total_sent += len(batch)
            logging.info(f"Lote enviado com sucesso. Resposta do servidor: {response}")
    except Exception as e:
        raise(f"Erro ao processar: {e}")
    finally:
        logging.info(f"Total de registros enviados: {total_sent}")
