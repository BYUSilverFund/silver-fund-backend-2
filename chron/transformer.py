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


def transform_rf(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df.loc[:, 'yield'] = df['yield'] * .01

    df['yield'] = df['yield'].fillna(df['yield'].shift(1))

    df['yield_lag'] = df['yield'].shift(1)

    df['P0'] = 100 / (1 + df['yield_lag'] * 30 / 360)
    df['P1'] = 100 / (1 + df['yield'] * 29 / 360)

    df['return'] = df['P1'] / df['P0'] - 1
    df['yield'] = df['yield'] / 360

    df = df[['date', 'yield', 'return']]

    return df


def transform_bmk(df: pd.DataFrame) -> pd.DataFrame:
    xf_df = df[df['ClientAccountID'] != 'ClientAccountID'].copy()
    xf_df['date'] = pd.to_datetime(xf_df['ReportDate'], format='%Y%m%d')
    xf_df['MarkPrice'] = pd.to_numeric(xf_df['MarkPrice'])

    xf_df = xf_df[xf_df['Symbol'] == 'IWV']

    xf_df['ending_value'] = xf_df['MarkPrice']
    xf_df['starting_value'] = xf_df['ending_value'].shift(1)
    xf_df['return'] = xf_df['ending_value'] / xf_df['starting_value'] - 1

    xf_df = xf_df[['date', 'starting_value', 'ending_value', 'return']]

    return xf_df
