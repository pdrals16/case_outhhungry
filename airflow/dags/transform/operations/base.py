import re
import logging
import duckdb
import pandas as pd

logging.basicConfig(format="[%(levelname)s] %(asctime)s : %(message)s", level=logging.INFO, force=True)

class BaseOperator():
    def __init__(self):
        pass

    def apply(data):
        return data

class ContextValues(BaseOperator):
    def __init__(self):
        super().__init__()
    
    def apply(self):
        logging.info(f"Applying ContextValues...")
        df = duckdb.sql("SELECT * FROM origin_table").to_df()
        for col in df.columns:
            if col=="email":
                logging.info(f"Applying regex to {col}...")
                pattern = re.compile(r"^[\w\.-]+@[\w\.-]+\.\w+$")
                df = df[df[col].apply(lambda x: bool(pattern.match(str(x))))]
        duckdb.sql("SELECT * FROM df").to_view(view_name="origin_table", replace=True)
        return None
    
class DuplicatedValues(BaseOperator):
    def __init__(self):
        super().__init__()
    
    def apply(self):
        logging.info(f"Applying DuplicatedValues...")
        df = duckdb.sql("SELECT * FROM origin_table").to_df()
        df = df.drop_duplicates()
        duckdb.sql("SELECT * FROM df").to_view(view_name="origin_table", replace=True)
        return None
    
class NullValues(BaseOperator):
    def __init__(self):
        super().__init__()
    
    def apply(self):
        logging.info(f"Applying NullValues...")
        df = duckdb.sql("SELECT * FROM origin_table").to_df()
        df = df.dropna()
        duckdb.sql("SELECT * FROM df").to_view(view_name="origin_table", replace=True)
        return None
