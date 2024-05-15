import os
from dotenv import load_dotenv
import psycopg2

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
    conn = psycopg2.connect(
        host=db_endpoint,
        database=db_name,
        user=db_user,
        password=db_password,
        port=db_port
    )

    # Create a cursor object
    cur = conn.cursor()

    # Execute a sample query
    cur.execute('SELECT version()')

    # Fetch the result
    db_version = cur.fetchone()
    print(f'PostgreSQL database version: {db_version}')

    # Close the cursor and connection
    cur.close()
    conn.close()
except Exception as e:
    print(f'Error: {e}')
