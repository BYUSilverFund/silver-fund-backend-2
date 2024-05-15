import pandas as pd

class Loader():

  def load(df: pd.DataFrame, query: str):    
    match query:

      case 'delta_nav':
        load_delta_nav(df)

      case 'positions':
        load_positions(df)

      case 'dividends':
        load_dividends(df)

      case 'trades':
        load_trades(df)
        
      case _:
        raise("Bad query key name")


    return df
  
def load_delta_nav(df):
  # Use .to_sql() to merge data into database
  return

def load_positions(df):
  # Use .to_sql() to merge data into database
  return

def load_dividends(df):
  # Use .to_sql() to merge data into database
  return

def load_trades(df):
  # Use .to_sql() to merge data into database
  return