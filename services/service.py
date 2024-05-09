from database.database import Database
from functions.functions import *
import numpy as np
import json

class Service:

  def example():
    data = Database.example()
    return data
  
  def portfolio_summary():
    # Fake data until database is up and running.
    # This will normally be where the DB functions are called.
    np.random.seed(42)
    port_returns = np.random.rand(50)
    bmk_returns = np.array([2*x+1 for x in port_returns])
    rf = np.array([.1 for x in range(50)])

    xs_port_returns = port_returns - rf
    xs_bmk_returns = bmk_returns - rf

    port_beta = portfolio_beta(xs_port_returns, xs_bmk_returns)
    port_alpha = portfolio_alpha(xs_port_returns,xs_bmk_returns)
    port_vol = portfolio_volatility(port_returns)
    port_ir = portfolio_information_ratio(port_returns, bmk_returns)
    print(port_ir)

    response = {
      'beta': port_beta,
      'alpha': port_alpha,
      'vol': port_vol,
      'information_ratio': port_ir
    }

    return json.dumps(response)