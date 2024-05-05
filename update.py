import pandas as pd
from datetime import datetime
import IBKR_update as up
import performance as pf
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)

current_date = datetime.now()
print("Current Date:", current_date.strftime("%Y-%m-%d"))

IBundergrad= up.IBKRupdate('undergrad', "155834512513051746551265")
IBundergrad.new_cash.to_parquet('data/undergrad_cash.parquet')
IBundergrad.new_holdings.to_parquet('data/undergrad_holdings.parquet')
IBundergrad.new_dividends.to_parquet('data/undergrad_dividends.parquet')
IBundergrad.new_ttw.to_parquet('data/undergrad_ttw.parquet')
IBundergrad.new_mms.to_parquet('data/undergrad_mms.parquet')
datebeg = pd.to_datetime('2023-09-18')
dateend = pd.to_datetime('2024-04-12')
#Pfundergrad = pf.Performance(IBundergrad,date1=datebeg, date2=dateend)

IBBC = up.IBKRupdate('BC', "170229039687872048124200")
IBBC.new_cash.to_parquet('data/BC_cash.parquet')
IBBC.new_holdings.to_parquet('data/BC_holdings.parquet')
IBBC.new_dividends.to_parquet('data/BC_dividends.parquet')
IBBC.new_ttw.to_parquet('data/BC_ttw.parquet')
IBBC.new_mms.to_parquet('data/BC_mms.parquet')
datebeg = pd.to_datetime('2023-09-18')
dateend = pd.to_datetime('2024-04-12')
PfBC = pf.Performance(IBBC,date1=datebeg, date2=dateend)

###############################################
# Merge BC into undergrad
###############################################
# cash file
# Select only the necessary columns
merged_df = pd.merge(IBundergrad.new_cash, IBBC.new_cash[['date', 'total']], on='date', suffixes=('_df1', '_df2'))
merged_df['total'] = merged_df['total_df1'] + merged_df['total_df2']
merged_df['stock'] = merged_df['stock'] + merged_df['total_df2']
ug_cash = merged_df[['date', 'cash', 'stock', 'd_accruals', 'i_accruals', 'total', 'funds']]

# holdings file
bc_positions0 = IBBC.new_cash[['date', 'total']]
bc_positions = bc_positions0.copy()
bc_positions['symbol'] = 'BRG-CAP'
bc_positions['securityID'] = 'BRG-CAP9999'
bc_positions['shares'] = 1
bc_positions['price'] = bc_positions['total']
bc_positions.rename(columns={'total': 'value'}, inplace=True)
bc_positions = bc_positions.sort_values(by='date')
bc_positions['lagged_price'] = bc_positions['price'].shift(1)
ug_holdings = IBundergrad.new_holdings._append(bc_positions)
ug_holdings.reset_index(drop=True, inplace=True)


# Dividends File
merged_df = pd.merge(IBBC.new_dividends, IBBC.new_holdings[['date', 'securityID', 'shares']], left_on=['securityID', 'exdate'], right_on=['securityID', 'date'])
merged_df['total_dividends'] = merged_df['divpershare'] * merged_df['shares']
ug_div = merged_df.groupby(['exdate', 'paydate'])['total_dividends'].sum()
ug_dividends = ug_div.reset_index()
ug_dividends['symbol'] = 'BRG-CAP'
ug_dividends['securityID'] = 'BRG-CAP9999'
IBBC.new_dividends = IBBC.new_dividends.rename(columns={'divpershare': 'dividend_per_share'})

ug_dividends = ug_dividends.rename(columns={'total_dividends': 'divpershare'})


# ttw file
merged_df = pd.merge(IBBC.new_ttw, IBundergrad.new_ttw, on='date', suffixes=('_df1', '_df2'))
merged_df['NetTradesPurchases'] = merged_df['NetTradesPurchases_df1'] + merged_df['NetTradesPurchases_df2']
merged_df['NetTradesSales'] = merged_df['NetTradesSales_df1'] + merged_df['NetTradesSales_df2']
merged_df['AccountTransfers'] = merged_df['AccountTransfers_df1'] + merged_df['AccountTransfers_df2']
merged_df['Deposits'] = merged_df['Deposits_df1'] + merged_df['Deposits_df2']
merged_df['Withdrawals'] = merged_df['Withdrawals_df1'] + merged_df['Withdrawals_df2']
ug_ttw = merged_df[['date', 'NetTradesPurchases', 'NetTradesSales', 'AccountTransfers', 'Deposits', 'Withdrawals']]

# mms file
merged_df = pd.merge(IBBC.new_mms, IBundergrad.new_mms, on='date', suffixes=('_df1', '_df2'))
merged_df['MarktoMarket'] = merged_df['MarktoMarket_df1'] + merged_df['MarktoMarket_df2']
ug_mms = merged_df[['date', 'MarktoMarket']]

ugupdate = up.IBKRupdate(cashfile=ug_cash, holdingsfile=ug_holdings, dividendsfile=ug_dividends, ttwfile=ug_ttw, mmsfile=ug_mms)
datebeg = pd.to_datetime('2023-09-18')
dateend = pd.to_datetime('2024-04-12')
PfUG = pf.Performance(ugupdate,date1=datebeg, date2=dateend)
quit()


#create IBKRupdate object using grad token
IBgrad= up.IBKRupdate('grad', "819997527277769608738195")
IBgrad.new_cash.to_parquet('data/grad_cash.parquet')
IBgrad.new_holdings.to_parquet('data/grad_holdings.parquet')
IBgrad.new_dividends.to_parquet('data/grad_dividends.parquet')
IBgrad.new_ttw.to_parquet('data/grad_ttw.parquet')
IBgrad.new_mms.to_parquet('data/grad_mms.parquet')
datebeg = pd.to_datetime('2024-01-01')
dateend = pd.to_datetime('2024-04-12')
Pfgrad = pf.Performance(IBgrad, date1=datebeg, date2=dateend)
quit()

#create IBKRupdate object using grad token
IBgrad= up.IBKRupdate('grad', "819997527277769608738195")
IBgrad.new_cash.to_parquet('data/grad_cash.parquet')
IBgrad.new_holdings.to_parquet('data/grad_holdings.parquet')
IBgrad.new_dividends.to_parquet('data/grad_dividends.parquet')
IBgrad.new_ttw.to_parquet('data/grad_ttw.parquet')
IBgrad.new_mms.to_parquet('data/grad_mms.parquet')
Pfgrad = pf.Performance(IBgrad)

