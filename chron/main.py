from config import config
from extractor import Extractor
from transform import Transformer
from loader import Loader

# Entry point for chron job

def main():
  funds = config.keys()

  token = config['grad']['token']
  query_types = config['grad']['queries'].keys()

  for query_type in query_types: 
    query_id = config['grad']['queries'][query_type]

    raw_data = Extractor.ibkr_query(token,query_id)

    transformed_data = Transformer.transform(raw_data, query_type)
    print(transformed_data)

    Loader.load(transformed_data, query_type)

    
if __name__ == "__main__":
  main()