import os
import requests
import re
import time
from io import StringIO
import pandas as pd
from fredapi import Fred
from dotenv import load_dotenv
import pandas_market_calendars as mcal

def ibkr_query(fund, token, query):
    # Checks
    if token is None:
        print('No token specified')
        return

    if query is None:
        print('No query id specified')
        return

    # Request 1
    url = f'https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService/SendRequest?t={token}&q={query}&v=3'
    user_agent = {'User-agent': 'Python/3.9'}
    response = requests.get(url, headers=user_agent)
    reference_code = re.findall('(?<=<ReferenceCode>)\d*(?=<\/ReferenceCode>)', response.text)[0]

    # Request 2
    time.sleep(15) # Consider changing this so that it adapts base on which fund is querying
    url = f'https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService/GetStatement?t={token}&q={reference_code}&v=3'
    response = requests.get(url)

    # Result
    csv_string = StringIO(response.text)
    df = pd.read_csv(csv_string)

    return df


def fred_query() -> pd.DataFrame:
    load_dotenv()

    series_id = 'DGS10'
    api_key = os.getenv("FRED_API_KEY")

    fred = Fred(api_key=api_key)
    data = fred.get_series(series_id).rename("yield").to_frame()

    data.reset_index(inplace=True)
    data.rename(columns={'index': 'date'}, inplace=True)

    return data

def calendar_query():
    today = pd.Timestamp.today()
    start_date = today - pd.Timedelta(days=7)
    end_date = today + pd.Timedelta(days=7)

    nyse = mcal.get_calendar('NYSE')
    df = nyse.schedule(start_date,end_date)
    df = df.reset_index().rename(columns={'index':'date'})

    return df