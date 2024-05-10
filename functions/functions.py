import statsmodels.api as sm
import numpy as np

TRADING_DAYS = 252

def alpha(xs_returns: np.ndarray, xs_bmk_returns: np.ndarray) -> float:
  """
  Calculate the alpha of a security or portfolio.

  Parameters:
  - xs_returns: Excess daily returns of the security or portfolio.
  - xs_bmk_returns: Excess daily returns of the benchmark.

  Returns:
  - list: A list containing the alpha, lower bound of the 95% confidence interval, and upper bound of the 95% confidence interval.
  """

  xs_returns = sm.add_constant(xs_returns)

  model = sm.OLS(xs_bmk_returns, xs_returns)
  results = model.fit()

  alpha = results.params[0]

  print(results.params)

  confidence_interval = results.conf_int(alpha=0.05) # 95% confidence interval
  alpha_ci = confidence_interval[0]
  print(alpha_ci)

  return [alpha, alpha_ci[0], alpha_ci[1]]


def beta(xs_returns: np.ndarray, xs_bmk_returns: np.ndarray) -> float:
  """
  Calculate the beta of a security or portfolio to the benchmark.

  Parameters:
  - xs_returns: Excess daily returns of the security or portfolio.
  - xs_bmk_returns: Excess daily returns of the benchmark.

  Returns:
  - float: beta.
  """

  xs_returns = sm.add_constant(xs_returns)

  model = sm.OLS(xs_bmk_returns, xs_returns)
  results = model.fit()

  beta = results.params[1]

  return beta


def volatility(returns: np.ndarray) -> float:
  """
  Calculate the volatility of a security or portfolio.

  Parameters:
  - returns: Daily returns of the security or portfolio.

  Returns:
  - float: volatility (annualized).
  """

  standard_deviation = returns.std()

  return standard_deviation * np.sqrt(TRADING_DAYS)


def portfolio_tracking_error(port_returns: np.ndarray, bmk_returns: np.ndarray) -> float:
  """
  Calculate the tracking error of a portfolio to the benchmark.

  Parameters:
  - port_returns: Daily returns of the portfolio.
  - bmk_returns: Daily returns of the benchmark.

  Returns:
  - float: tracking error (annualized).
  """

  difference = port_returns - bmk_returns

  return difference.std() * np.sqrt(TRADING_DAYS)


def portfolio_information_ratio(port_returns: np.ndarray, bmk_returns: np.ndarray) -> float:
  """
  Calculate the information ratio of a portfolio to the benchmark.

  Parameters:
  - port_returns: Daily returns of the portfolio.
  - bmk_returns: Daily returns of the benchmark.

  Returns:
  - float: information ratio.
  """

  cum_port_return = port_returns[-1] / port_returns[0] - 1
  cum_bmk_return = bmk_returns[-1] / bmk_returns[0] - 1

  tracking_error = portfolio_tracking_error(port_returns,bmk_returns)
  
  return (cum_port_return - cum_bmk_return) / tracking_error


def total_return(returns: np.ndarray) -> float:
  """
  Calculate the total return of a security or portfolio.

  Parameters:
  - returns: Daily returns of the security or portfolio.

  Returns:
  - float: total return.
  """

  log_returns = np.log(returns)
  total_return = log_returns.sum()

  return total_return
