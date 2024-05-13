import pandas as pd

class Database:

  def example():
    return "Hello world"
  
  def get_holdings_returns(fund, start_date, end_date = pd.Timestamp.today().strftime('%Y-%m-%d')):

    #Filters
    holdings = pd.read_csv("database/data/holdings.csv", index_col=0)
    holdings = holdings.query("@start_date <= Date <= @end_date")
    holdings = holdings.query("Fund == @fund")

    tickers = holdings['Ticker'].unique()

    result = {}

    result['Date'] = holdings['Date'].to_numpy(),
    result['Tickers'] = tickers

    stock_returns_arr = []
    for ticker in tickers:
      stock = holdings.query("Ticker == @ticker")
      result[ticker] = stock['Daily Return'].to_numpy()

    return result

  def bmk_returns(start_date, end_date = pd.Timestamp.today().strftime('%Y-%m-%d')):

    bmk = pd.read_csv("database/data/bmk.csv", index_col=0)
    bmk = bmk.query("@start_date <= Date <= @end_date")

    result = {
      'Date':bmk['Date'].to_numpy(),
      'Daily Return': bmk['Daily Return'].to_numpy()
    }

    return result

  def rf_returns(start_date, end_date = pd.Timestamp.today().strftime('%Y-%m-%d')):
    rf = pd.read_csv("database/data/rf.csv", index_col=0)
    rf = rf.query("@start_date <= Date <= @end_date")

    result = {
      'Date': rf['Date'].to_numpy(),
      'RF Daily': rf['RF Daily'].to_numpy()
    }

    return result
