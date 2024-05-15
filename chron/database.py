import os
from dotenv import load_dotenv
import psycopg2
from sqlalchemy import create_engine
import pandas as pd

class Database():

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

    except Exception as e:
        print(f'Error: {e}')

  def __del__(self):
    # Close the cursor and connection
    self.cursor.close()
    self.connection.close()

  def query(self, query_string):
    # Execute a sample query
    self.cursor.execute(query_string)

    # Fetch the result
    result =  self.cursor.fetchone()

    return result 
  
  def load(self, df, table_name):
    # Create the SQLAlchemy engine
    db_url = f'postgresql+psycopg2://{self.db_user}:{self.db_password}@{self.db_endpoint}:{self.db_port}/{self.db_name}'
    engine = create_engine(db_url)

    try:
        # Using the `to_sql` method to upload the DataFrame
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Table '{table_name}' created successfully and data inserted.")

    except Exception as e:
        print(f"Error: {e}")
        
    finally:
        engine.dispose()