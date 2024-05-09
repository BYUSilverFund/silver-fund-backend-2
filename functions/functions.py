import statsmodels.api as sm
import numpy as np

TRADING_DAYS = 252

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

  return standard_deviation * np.sqrt(TRADING_DAYS)


def portfolio_tracking_error(port_returns: np.ndarray, bmk_returns: np.ndarray) -> float:

  difference = port_returns - bmk_returns

  return difference.std() * np.sqrt(TRADING_DAYS)


def portfolio_information_ratio(port_returns: np.ndarray, bmk_returns: np.ndarray) -> float:

  difference = port_returns - bmk_returns

  tracking_error = portfolio_tracking_error(port_returns,bmk_returns)
  
  return difference / tracking_error
