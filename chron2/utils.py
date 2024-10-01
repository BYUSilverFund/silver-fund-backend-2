import pandas as pd
from jinja2 import Template

# Declare columns to keep here

def clean_ibkr_dataframe(df: pd.DataFrame, query, fund):
    # Keep only specified columns
    
    # Remove extra header rows
    xf_df = df[df['ClientAccountID'] != 'ClientAccountID'].copy()  # This transformation happens for all dataframes

    # Add fund column
    xf_df['fund'] = fund

    # Add date column (using other column in dataframe)
    if 'ReportDate' in xf_df.columns:
        xf_df['date'] = pd.to_datetime(xf_df['ReportDate'], format='%Y%m%d')
    elif 'FromDate' in xf_df.columns:
        xf_df['date'] = pd.to_datetime(xf_df['FromDate'], format='%Y%m%d')  

    # Drop duplicates
    xf_df = xf_df.drop_duplicates()

    return xf_df

def render_sql(sql_file, params=None):

    if params:
        with open(sql_file, 'r') as file:

            template = Template(file.read())

            return template.render(params)
        
    else:
        with open(sql_file, 'r') as file:

            query_string = file.read()

            return query_string