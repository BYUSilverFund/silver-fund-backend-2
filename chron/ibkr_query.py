import requests
import re
import time
from io import StringIO
import pandas as pd

def ibkr_query(token, query):

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
  reference_code = re.findall('(?<=<ReferenceCode>)\d*(?=<\/ReferenceCode>)',response.text)[0]

  # Request 2
  time.sleep(2)
  url = f'https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService/GetStatement?t={token}&q={reference_code}&v=3'
  response = requests.get(url)

  # Result
  csv_string = StringIO(response.text)
  df = pd.read_csv(csv_string)

  return df