from config import config
from ibkr_query import ibkr_query

# Entry point for chron job

def main():
  funds = config.keys()

  token = config['grad']['token']
  query_id = config['grad']['delta_nav_id']
  
  df = ibkr_query(token,query_id)

  print(df)


if __name__ == "__main__":
  main()