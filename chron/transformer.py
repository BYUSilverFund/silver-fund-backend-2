import pandas as pd


def transform(df: pd.DataFrame, fund: str, query: str) -> pd.DataFrame:
    xf_df = df[df['ClientAccountID'] != 'ClientAccountID'].copy()  # This transformation happens for all dataframes
    xf_df['fund'] = fund

    match query:

        case 'nav':
            xf_df = transform_nav(xf_df)

        case 'delta_nav':
            xf_df = transform_delta_nav(xf_df)

        case 'positions':
            xf_df = transform_positions(xf_df)

        case 'dividends':
            xf_df = transform_dividends(xf_df)

        case 'trades':
            xf_df = transform_trades(xf_df)

        case _:
            raise "Bad query key name"

    return xf_df

def transform_nav(df):
    xf_df = df
    xf_df['date'] = pd.to_datetime(xf_df['ReportDate'], format='%Y%m%d')
    return xf_df


def transform_delta_nav(df):
    xf_df = df
    xf_df['date'] = pd.to_datetime(xf_df['FromDate'], format='%Y%m%d')
    return xf_df


def transform_positions(df):
    xf_df = df
    xf_df['date'] = pd.to_datetime(xf_df['ReportDate'], format='%Y%m%d')
    return xf_df


def transform_dividends(df):
    xf_df = df
    xf_df['date'] = pd.to_datetime(xf_df['ExDate'], format='%Y%m%d')
    return xf_df


def transform_trades(df):
    xf_df = df
    xf_df['date'] = pd.to_datetime(xf_df['ReportDate'], format='%Y%m%d')
    return xf_df
