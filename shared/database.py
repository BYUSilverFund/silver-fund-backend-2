import os
from dotenv import load_dotenv
import psycopg2
from sqlalchemy import create_engine, inspect
import pandas as pd


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
    
    def load_cron_log(self, cron_log_string: str) -> None: # This should turn into an sql file
        try:
            query = f"INSERT INTO \"ETL_Cron_Log\" (date, log_text) VALUES (now(), E'{cron_log_string}')"
            self.cursor.execute(query)
            self.connection.commit()
        except Exception as e:
            print(f"Error: {e}")

    def load_dataframe(df: pd.DataFrame, table_name):
        # Takes a pandas dataframe and loads it into
        pass

    def execute_sql(query_string) -> None:
        # Execute sql
        # Return response

        pass
