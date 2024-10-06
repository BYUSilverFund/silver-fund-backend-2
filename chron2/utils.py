import pandas as pd
from jinja2 import Template

def clean_ibkr_dataframe(df: pd.DataFrame):
    # Remove extra header rows
    xf_df = df[df['ClientAccountID'] != 'ClientAccountID'].copy()  # This transformation happens for all dataframes

    # Add date column (using other column in dataframe)
    if 'ReportDate' in xf_df.columns:
        xf_df['date'] = pd.to_datetime(xf_df['ReportDate'], format='%Y%m%d')

    elif 'FromDate' in xf_df.columns:
        xf_df['date'] = pd.to_datetime(xf_df['FromDate'], format='%Y%m%d')  

    # Drop duplicates
    xf_df = xf_df.drop_duplicates()

    return xf_df

def get_fund(client_account_id):    
    if client_account_id in ('U4297056', 'U4033278'):
        return 'undergrad'

    elif client_account_id == 'U12702064':
        return 'grad'
    
    elif client_account_id == 'U12702120':
        return 'quant'
    
    elif client_account_id == 'U10797691':
        return 'brigham_capital'

    else:
        raise ValueError

def render_sql(sql_file, params=None):

    if params:
        with open(sql_file, 'r') as file:

            template = Template(file.read())

            return template.render(params)
        
    else:
        with open(sql_file, 'r') as file:

            query_string = file.read()

            return query_string