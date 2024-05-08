from typing import List
import statsmodels.api as sm
import numpy as np

def portfolio_alpha(xs_port_returns: np.ndarray, xs_bmk_returns: np.ndarray) -> float:

  xs_port_returns = sm.add_constant(xs_port_returns)

  model = sm.OLS(xs_bmk_returns, xs_port_returns)
  results = model.fit()

  alpha = results.params[0]

  return alpha


def portfolio_beta(xs_port_returns: np.ndarray, xs_bmk_returns: np.ndarray) -> float:

  xs_port_returns = sm.add_constant(xs_port_returns)

  model = sm.OLS(xs_bmk_returns, xs_port_returns)
  results = model.fit()

  beta = results.params[1]

  return beta


def portfolio_volatility(port_returns: np.ndarray) -> float:

  standard_deviation = port_returns.std()

  return standard_deviation * np.sqrt(250)
