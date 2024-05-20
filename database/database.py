import os
from dotenv import load_dotenv
import psycopg2
from sqlalchemy import create_engine
import pandas as pd
import requests


class Database:

    def __init__(self):
        # Load environment variables from .env file
        load_dotenv()

        # Fetch the RDS details from environment variables
        self.db_endpoint = os.getenv('DB_ENDPOINT')
        self.db_name = os.getenv('DB_NAME')
        self.db_user = os.getenv('DB_USER')
        self.db_password = os.getenv('DB_PASSWORD')
        self.db_port = os.getenv('DB_PORT', '5432')

        try:
            # Establish the connection
            self.connection = psycopg2.connect(
                host=self.db_endpoint,
                database=self.db_name,
                user=self.db_user,
                password=self.db_password,
                port=self.db_port
            )

            # Create a cursor object
            self.cursor = self.connection.cursor()

            # Create the SQLAlchemy engine
            db_url = f'postgresql+psycopg2://{self.db_user}:{self.db_password}@{self.db_endpoint}:{self.db_port}/{self.db_name}'
            self.engine = create_engine(db_url)

        except Exception as e:
            print(f'Error: {e}')

    def __del__(self):
        # Close the cursor and connection
        self.cursor.close()
        self.connection.close()
        self.engine.dispose()

    def execute_query(self, query_string: str) -> pd.DataFrame:
        df = pd.read_sql(query_string, self.engine)
        return df

    @staticmethod
    def fred_risk_free() -> pd.DataFrame:
        fred_key = '971fa55a70d1cee395102f9d52510052'
        fred_series = 'DGS1MO'
        url = f'https://api.stlouisfed.org/fred/series/observations?series_id={fred_series}&api_key={fred_key}&file_type=json'
        response = requests.get(url)
        data = response.json()
        df = pd.DataFrame(data['observations'])
        return df
