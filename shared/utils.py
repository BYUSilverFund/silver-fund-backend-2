import pandas as pd
from jinja2 import Template

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