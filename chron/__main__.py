from config import config
from extractor import Extractor
from transform import Transformer
from database import Database

# Entry point for chron job

def main():
  funds = config.keys()

  for fund in funds:
    token = config[fund]['token']
    query_types = config[fund]['queries'].keys()

    print(f"Begining {fund.capitalize()} Fund ETL")

    for query_type in query_types: 
      query_id = config[fund]['queries'][query_type]

      # Extract
      print(f"Executing IBKR {query_type} query")
      raw_data = Extractor.ibkr_query(token,query_id)

      # Transform
      print(f"Transforming the {query_type} data")
      transformed_data = Transformer.transform(raw_data, fund, query_type)

      # Load
      database = Database()
      database.load_df(transformed_data, query_type)
      print(f"Data loaded into the {query_type} table in the database.")


if __name__ == "__main__":
  main()