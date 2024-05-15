from config import config
from extractor import Extractor
from transform import Transformer
from database import Database

# Entry point for chron job

def main():
  funds = config.keys() # To use in the future to loop through each account

  token = config['grad']['token']
  query_types = config['grad']['queries'].keys()

  for query_type in query_types: 
    query_id = config['grad']['queries'][query_type]

    # Extract
    raw_data = Extractor.ibkr_query(token,query_id)

    # Transform
    transformed_data = Transformer.transform(raw_data, query_type)

    # Load
    database = Database()
    database.load(transformed_data, query_type)

    
if __name__ == "__main__":
  main()