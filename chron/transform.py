import pandas as pd

class Transformer():
  
  def transform(df: pd.DataFrame, fund: str, query: str) -> pd.DataFrame:
    df = df.copy()
    df = df[df['ClientAccountID'] != 'ClientAccountID'] # This transformation happens for all dataframes
    df['fund'] = fund
    
    match query:

      case 'nav':
        df = transform_nav(df)

      case 'delta_nav':
        df = transform_delta_nav(df)

      case 'positions':
        df = transform_positions(df)

      case 'dividends':
        df = transform_dividends(df)

      case 'trades':
        df = transform_trades(df)
        
      case _:
        raise("Bad query key name")

    return df
  
def transform_nav(df):
  df = df.copy()
  df['date'] = pd.to_datetime(df['ReportDate'], format='%Y%m%d')
  return df

def transform_delta_nav(df):
  df = df.copy()
  df['date'] = pd.to_datetime(df['FromDate'], format='%Y%m%d')
  return df

def transform_positions(df):
  df = df.copy()
  df['date'] = pd.to_datetime(df['ReportDate'], format='%Y%m%d')
  return df

def transform_dividends(df):
  df = df.copy()
  df['date'] = pd.to_datetime(df['ExDate'], format='%Y%m%d') # Should this be PayDate instead?
  return df

def transform_trades(df):
  df = df.copy()
  df['date'] = pd.to_datetime(df['ReportDate'], format='%Y%m%d')
  return df