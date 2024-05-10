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

    ####
    np.random.seed(42)
    port_returns = np.random.rand(50)
    epsilon = np.random.randn(50) * 0.5
    bmk_returns = 2 * port_returns + 1 + epsilon
    rf = np.array([.1 for x in range(50)])
    ####

    xs_port_returns = port_returns - rf
    xs_bmk_returns = bmk_returns - rf

    port_beta = beta(xs_port_returns, xs_bmk_returns)
    port_alpha = alpha(xs_port_returns,xs_bmk_returns)
    port_vol = volatility(port_returns)
    port_ir = portfolio_information_ratio(port_returns, bmk_returns)

    port_beta = round(port_beta,2)
    port_alpha[0] = round(port_alpha[0],2)
    port_alpha[1] = round(port_alpha[1],2)
    port_alpha[2] = round(port_alpha[2],2)
    port_vol = round(port_vol,2)
    port_ir = round(port_ir,2)

    response = {
      'beta': port_beta,
      'alpha': port_alpha[0],
      'confidence_interval': [ port_alpha[1], port_alpha[2]],
      'vol': port_vol,
      'information_ratio': port_ir
    }

    return json.dumps(response)