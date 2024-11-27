import os
import json
import uuid
import random
from datetime import datetime


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")

class Customer:
    def __init__(self):
        self.id = str(uuid.uuid4())
        self.first_name = random.choice(["Pedro","Gabrielle"])
        self.last_name = random.choice(["Augusto","Camille"])
        self.email = f"{self.first_name.lower()}_{self.last_name.lower()}@{random.choice(["gmail","hotmail"])}.com"
        self.cep = f"{random.randint(10000,99999)}-{random.randint(100,999)}"
        self.ddd = random.randint(10,99)
        self.phone = f"9{random.randint(1000,9999)}-{random.randint(1000,9999)}"
        self.created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        pass

    def describe(self):
        return {
            "id": self.id,
            "first_name" : self.first_name,
            "last_name" : self.last_name,
            "email" : self.email,
            "cep" : self.cep,
            "ddd" : self.ddd,
            "phone" : self.phone,
            "created_at" : self.created_at
        }


class Transactions:
    def __init__(self, current_date):
        self.current_date = current_date
        pass

    @property
    def source_customers(self):
        with open(os.path.join(AIRFLOW_HOME, f"data/ouch_hungry/{self.current_date}/clientes.json")) as f:
            data = json.loads(f.read())
        return data
    
    @property
    def source_products(self):
        with open(os.path.join(AIRFLOW_HOME, f"data/ouch_hungry/{self.current_date}/produtos.json")) as f:
            data = json.loads(f.read())
        return data
    
    def describe(self):
        transactions = []
        cities = ["Ouro Preto", "Poços de Caldas", "Araxá", "Diamantina", "Campinas", "Ribeirão Preto", 
                  "São José do Rio Preto", "Sorocaba", "Petrópolis", "Teresópolis", "Nova Friburgo", 
                  "Resende", "Londrina", "Maringá", "Foz do Iguaçu", "Ponta Grossa"]
        products_id = [product.get("id") for product in self.source_products.get("products")]
        for customer in self.source_customers.get("customers"):
            transaction = {
                "id": str(uuid.uuid4()),
                "customer_id": customer.get("id"),
                "product_id": random.choice(products_id),
                "city": random.choice(cities),
                "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            transactions.append(transaction)
        return {
            "transactions": transactions
        }


def generate_customer(num: int = 100):
    customer = []
    for n in range(num):
        customer.append(Customer().describe())
    return {
        "customers": customer
    }

def run():
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    products = {
        "products": [
            {
                "id": "1",
                "description": "Hungry",
                "category": "soda",
                "ean": "737876346211",
                "price": 9.99,
                "created_at": "2024-11-24 23:11:34"
            },
            {
                "id": "2",
                "description": "Ouch",
                "category": "fingerfood",
                "ean": "737876336213",
                "price": 14.99,
                "created_at": "2024-11-25 23:13:54"
            }
        ]
    }
    file_path = os.path.join(AIRFLOW_HOME, f"data/ouch_hungry/{current_date}/produtos.json")
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "+w") as f:
        f.write(json.dumps(products, indent=4))

    customers = generate_customer()
    file_path = os.path.join(AIRFLOW_HOME, f"data/ouch_hungry/{current_date}/clientes.json")
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "+w") as f:
        f.write(json.dumps(customers, indent=4))

    file_path = os.path.join(AIRFLOW_HOME, f"data/ouch_hungry/{current_date}/transacoes.json")
    transactions = Transactions(current_date=current_date)
    with open(file_path, "+w") as f:
        f.write(json.dumps(transactions.describe(), indent=4))
    return None