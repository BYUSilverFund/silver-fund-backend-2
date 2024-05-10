import pandas as pd
from datetime import datetime
import IBKR_update_class as ud
import performance as pf
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)


IBgrad= ud.update('grad', "819997527277769608738195")