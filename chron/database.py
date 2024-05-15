import os
from dotenv import load_dotenv
import psycopg2

class Database():

  def __init__(self):
    # Load environment variables from .env file
    load_dotenv()

    # Fetch the RDS details from environment variables
    db_endpoint = os.getenv('DB_ENDPOINT')
    db_name = os.getenv('DB_NAME')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_port = os.getenv('DB_PORT', '5432')

    try:
        # Establish the connection
        self.connection = psycopg2.connect(
            host=db_endpoint,
            database=db_name,
            user=db_user,
            password=db_password,
            port=db_port
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