import psycopg2
from psycopg2 import sql

from transform.utils.vault import get_secrets_from_vault

VAULT = get_secrets_from_vault()
DB_HOST = "postgres_hungry"
DB_PORT = "5432"
DB_USER = VAULT["postgres_user"]
DB_PASSWORD = VAULT["postgres_password"]

def connect_to_postgres(db_name):
    try:
        connection = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=db_name,
            user=DB_USER,
            password=DB_PASSWORD
        )
        print(f"Connection to PostgreSQL database {db_name} successful!")
        connection.autocommit = True
        cursor = connection.cursor()
    except Exception as e:
        raise("An error occurred while connecting to PostgreSQL:", e)
    return cursor
