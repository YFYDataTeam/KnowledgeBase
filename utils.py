import cx_Oracle
import pandas as pd
import json
from sqlalchemy import create_engine
import warnings
import os

def read_config(path):
    try:
        with open(path, 'r') as file:
            configs = json.load(file)

        return configs
    except FileNotFoundError:
        print(f"The file {path} was not found.")
    except json.JSONDecodeError:
        print(f"Error decoding JSON from the file {path}.")

class MySQLAgent:
    def __init__(self, config) -> None:
        self.config = config
        self.db_connector()

    def db_connector(self):
        user = self.config['user']
        pw = self.config['pw']
        host = self.config['host']
        port = self.config['port']
        database = self.config['database']

        self.connection_string = f"mysql+pymysql://{user}:{pw}@{host}:{port}/{database}?charset=utf8mb4"

        self.engine = create_engine(self.connection_string)

    def read_table(self, query) -> pd.DataFrame:

        df = pd.read_sql(query, con=self.engine)
        df.columns = df.columns.str.lower()

        return df

    def write_table(self, data, table_name, if_exists, index, data_type):

        data.to_sql(name=table_name, con=self.engine,
                    if_exists=if_exists, index=index, dtype=data_type)
        
        
class OracleAgent:
    def __init__(self, config) -> None:
        self.config = config
        self.db_connector()
    
    def db_connector(self):

        oracle_client_dir = './opt/oracle'

        oracle_client_path = os.path.join(oracle_client_dir, os.listdir(oracle_client_dir)[0])

        cx_Oracle.init_oracle_client(oracle_client_path)
        
        user = self.config['user']
        pw = self.config['pw']
        host = self.config['host']
        port = self.config['port']
        service_name = self.config['service_name']
        self.conn = create_engine(f'oracle+cx_oracle://{user}:{pw}@{host}:{port}/?service_name={service_name}')


    def read_table(self, query):
        warnings.filterwarnings('ignore')

        df = pd.read_sql(query, con=self.conn)
        df.columns = df.columns.str.lower()

        return df
