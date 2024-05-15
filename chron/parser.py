import pandas as pd

class Parser():
  
  def parse(df: pd.DataFrame, query: str):
    df = df[df['ClientAccountID'] != 'ClientAccountID']
    
    match query:

      case 'delta_nav':
        df = parse_delta_nav(df)

      case 'positions':
        df = parse_positions(df)

      case 'dividends':
        df = parse_dividends(df)

      case 'trades':
        df = parse_trades(df)
        
      case _:
        raise("Bad query key name")


    return df

def parse_delta_nav(df):
  df = df.copy()
  df['date'] = pd.to_datetime(df['FromDate'], format='%Y%m%d')
  return df

def parse_positions(df):
  df = df.copy()
  df['date'] = pd.to_datetime(df['ReportDate'], format='%Y%m%d')
  return df

def parse_dividends(df):
  df = df.copy()
  df['date'] = pd.to_datetime(df['ExDate'], format='%Y%m%d') # Should this be PayDate instead?
  return df

def parse_trades(df):
  df = df.copy()
  df['date'] = pd.to_datetime(df['ReportDate'], format='%Y%m%d')
  return df