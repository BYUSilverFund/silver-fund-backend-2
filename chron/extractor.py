import os
import requests
import re
import time
from io import StringIO
import pandas as pd
from fredapi import Fred
from dotenv import load_dotenv
import pandas_market_calendars as mcal

def ibkr_query(fund, token, query_id):
    
    # Checks
    if not token:
        print('No token specified')
        return

    if not query_id:
        print('No query id specified')
        return

    # Request 1
    url = f'https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService/SendRequest?t={token}&q={query_id}&v=3'
    user_agent = {'User-agent': 'Python/3.9'}
    response = requests.get(url, headers=user_agent)
    reference_codes = re.findall(r'(?<=<ReferenceCode>)\d*(?=</ReferenceCode>)', response.text)
    if not reference_codes:
        raise ValueError(f'No ReferenceCode found in response from ndcdyn.interactivebrokers.com endpoint this is likely due to an expired token')
    reference_code = reference_codes[0]

    # Request 2
    time_to_sleep = 15 if fund == 'quant' else 10
    time.sleep(time_to_sleep) # Consider changing this so that it adapts base on which fund is querying
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

    return transform_rf(data)

def calendar_query():
    today = pd.Timestamp.today()
    start_date = '2020-01-01'
    end_date = today + pd.Timedelta(days=7)

    nyse = mcal.get_calendar('NYSE')
    df = nyse.schedule(start_date,end_date)
    df = df.reset_index().rename(columns={'index':'caldt'})

    return df

def transform_rf(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = df.rename(columns={'date': 'caldt'})

    df.loc[:, 'yield'] = df['yield'] * .01

    df['yield'] = df['yield'].fillna(df['yield'].shift(1))

    df['yield_lag'] = df['yield'].shift(1)

    df['P0'] = 100 / (1 + df['yield_lag'] * 30 / 360)
    df['P1'] = 100 / (1 + df['yield'] * 29 / 360)

    df['return'] = df['P1'] / df['P0'] - 1
    df['yield'] = df['yield'] / 360

    df = df[['caldt', 'yield', 'return']]

    return df