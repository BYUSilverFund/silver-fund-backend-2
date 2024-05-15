from config import config
from ibkr_query import ibkr_query
from parser import Parser

# Entry point for chron job

def main():
  funds = config.keys()

  token = config['grad']['token']
  queries = config['grad']['queries'].keys()

  for query in queries: 
    query_id = config['grad']['queries'][query]
    raw_data = ibkr_query(token,query_id)
    parsed_data = Parser.parse(raw_data, query)
    print(parsed_data)
    
if __name__ == "__main__":
  main()