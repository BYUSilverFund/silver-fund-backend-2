import pandas as pd
from jinja2 import Template

# Declare columns to keep here

def clean_ibkr_dataframe(df: pd.DataFrame, query, fund):
    # Remove extra header rows
    # Keep only specified columns
    # Add fund column
    # Add date column (using other column in dataframe)
    xf_df = df[df['ClientAccountID'] != 'ClientAccountID'].copy()  # This transformation happens for all dataframes
    xf_df['fund'] = fund
    xf_df['date'] = pd.to_datetime(xf_df['ReportDate'], format='%Y%m%d')
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