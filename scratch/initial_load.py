import re
import time
from io import StringIO
import pandas as pd
import formatting_functions as ff
import pyarrow.parquet as pq
import numpy as np

pd.set_option('display.max_columns', None)
#pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)

Trades_grad = pd.read_csv('data2/Trades_grad.csv')

############################################################################################################
# Format Delta_NAV_grad
############################################################################################################
Delta_NAV_grad = pd.read_csv('data2/Delta_NAV_grad.csv')
c1 = Delta_NAV_grad[Delta_NAV_grad['FromDate'] != 'FromDate']
c2 = c1.copy()
c3 = ff.format_delta_nav(c2)
c3.to_parquet('data_parquet/Delta_NAV_grad.parquet')

############################################################################################################
# Format Dividends
############################################################################################################
Dividends_grad = pd.read_csv('data2/Dividends_grad.csv')
c1 = Dividends_grad[Dividends_grad['ExDate'] != 'ExDate']
c2 = c1.copy()
c3 = ff.format_dividends(c2)
c3.to_parquet('data_parquet/Dividends_grad.parquet')

############################################################################################################
# Format Holdings
############################################################################################################
Holdings_grad = pd.read_csv('data2/Holdings_grad.csv')
c1 = Holdings_grad[Holdings_grad['Conid'] != 'Conid']
c2 = c1.copy()
c3 = ff.format_holdings(c2)
c3.to_parquet('data_parquet/Holdings_grad.parquet')

############################################################################################################
# Format Trades
############################################################################################################
Trades_grad = pd.read_csv('data2/Trades_grad.csv')
c1 = Trades_grad[Trades_grad['Conid'] != 'Conid']
c2 = c1.copy()
c3 = ff.format_trades(c2)
c3.to_parquet('data_parquet/Trades_grad.parquet')

############################################################################################################