import os
import json


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")

def get_secrets_from_vault():
    with open(os.path.join(AIRFLOW_HOME, "config/vault.json"), "r") as f:
        secrets = json.loads(f.read())
    return secrets