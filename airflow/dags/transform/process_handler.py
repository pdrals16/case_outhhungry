import os
import duckdb
import json
import logging

from transform.utils.vault import get_secrets_from_vault
from transform.utils.connection import connect_to_postgres
from transform.utils.api import deliver_api
from transform.utils.sftp import deliver_sftp
from transform.operations.base import ContextValues, DuplicatedValues, NullValues


logging.basicConfig(format="[%(levelname)s] %(asctime)s : %(message)s", level=logging.INFO, force=True)

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
VAULT = get_secrets_from_vault()

class Transform():
    def __init__(self, context=None, reference_date=None, mode=None, source_file=None, ingest_sql_name=None, insert_sql_name=None, view_sql_name=None, target_db_name=None):
        self.context = context
        self.reference_date = reference_date
        self.mode = mode
        self.source_file = source_file
        self.ingest_sql_name = ingest_sql_name
        self.insert_sql_name = insert_sql_name
        self.view_sql_name = view_sql_name
        self.target_db_name = target_db_name
        pass
    
    def read_sql(self, sql_name):
        file_path = os.path.join(AIRFLOW_HOME, f"dags/transform/sql/{sql_name}.sql")
        logging.info(f"Reading sql file {file_path}...")
        with open(file_path, "r") as f:
            sql_statement = f.read()
        return sql_statement
    
    def read_files(self):
        sql = self.read_sql(self.ingest_sql_name)
        logging.info(f"Reading file on duckdb the {self.source_file} with reference date {self.reference_date}...")
        duckdb.sql(sql.format(source_file=self.source_file, reference_date=self.reference_date.strftime('%Y-%m-%d'))).to_view(view_name="origin_table", replace=True)
        return None

    def read_table(self):
        logging.info(f"Reading table {self.context} at database outhhungry_staged...")
        duckdb.sql("INSTALL postgres; LOAD postgres;")
        duckdb.sql(f"SELECT * FROM postgres_scan('host=postgres_hungry user={VAULT["postgres_user"]} password={VAULT["postgres_password"]} port=5432 dbname=outhhungry_staged', 'public', {self.context});").to_view(view_name="origin_table", replace=True)
        return None

    def extract(self):
        logging.info(f"Starting extract function!")
        if self.mode == "file":
            logging.info(f"Mode 'file' has been selected.")
            self.read_files()
        elif self.mode == "table":
            logging.info(f"Mode 'table' has been selected.")
            self.read_table()
        else:
            raise(f"Mode {self.mode} is not avaliable!")
        logging.info(f"Extract function has been finished!")
        return None

    def transform(self):
        logging.info(f"Starting transform function!")
        duplicated = DuplicatedValues()
        duplicated.apply()
        
        null_values = NullValues()
        null_values.apply()

        context_values = ContextValues()
        context_values.apply()
        logging.info(f"Transform function has been finished!")
        return None

    def consolidation(self):
        logging.info(f"Starting consolidation function!")
        sql = self.read_sql(self.view_sql_name)
        duckdb.sql("INSTALL postgres; LOAD postgres;")
        duckdb.sql(sql.format(reference_date=self.reference_date, postgres_user=VAULT["postgres_user"], postgres_password=VAULT["postgres_password"])).to_view(view_name="origin_table", replace=True)
        logging.info(f"Consolidation function has been finished!")
        return None

    def load(self):
        logging.info(f"Starting load function!")
        data = duckdb.sql("SELECT * FROM origin_table").fetchall()
        cur = connect_to_postgres(db_name=self.target_db_name)
        insert_query = self.read_sql(self.insert_sql_name)
        print(data)
        cur.executemany(insert_query, data)
        logging.info(f"Load function has been finished!")
        return None

def run_staged(**kwargs):
    transform = Transform(
        context=kwargs["context"],
        reference_date=kwargs['execution_date'],
        mode=kwargs["mode"],
        source_file=kwargs["source_file"],
        ingest_sql_name=kwargs["ingest_sql_name"],
        insert_sql_name=kwargs["insert_sql_name"],
        target_db_name=kwargs["target_db_name"]
        )
    transform.extract()
    transform.load()
    return None

def run_trusted(**kwargs):
    transform = Transform(
        context=kwargs["context"],
        reference_date=kwargs['execution_date'],
        mode=kwargs["mode"],
        insert_sql_name=kwargs["insert_sql_name"],
        target_db_name=kwargs["target_db_name"]
        )
    transform.extract()
    transform.transform()
    transform.load()
    return None

def run_refined(**kwargs):
    transform = Transform(
        reference_date=kwargs['execution_date'],
        view_sql_name=kwargs["view_sql_name"],
        insert_sql_name=kwargs["insert_sql_name"],
        target_db_name=kwargs["target_db_name"]
        )
    transform.consolidation()
    transform.load()
    return None

def run_api_deliver(**kwargs):
    deliver_api(
        table=kwargs["table"],
        db_name=kwargs["db_name"],
        frequency=kwargs["frequency"],
        reference_date=kwargs["execution_date"]
    )
    
def run_sftp_deliver(**kwargs):
    deliver_sftp(
        table=kwargs["table"],
        db_name=kwargs["db_name"],
        frequency=kwargs["frequency"],
        reference_date=kwargs["execution_date"]
    )
    