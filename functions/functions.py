import statsmodels.api as sm
import numpy as np
import pandas as pd

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

    intercept = results.params.iloc[0]

    return intercept


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

    slope = results.params[1]

    return slope


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

    tracking_error = portfolio_tracking_error(port_returns, bmk_returns)

    return (cum_port_return - cum_bmk_return) / tracking_error


def total_return(starting_value: float, ending_value: float) -> float:
    """
  Calculate the total return of a security or portfolio.

  Parameters:
  - returns: Daily returns of the security or portfolio.

  Returns:
  - float: total return.
  """

    return ending_value / starting_value - 1


def returns_vector(starting_values: pd.Series, ending_values: pd.Series) -> np.ndarray:
    """
  Calculate the daily returns vector of a security or portfolio.

  Parameters:
  - starting_values: Starting values of the security or portfolio.
  - ending_values: Ending values of the security or portfolio.

  Returns:
  - np.ndarray: daily return vector
  """
    daily_returns = ending_values / starting_values - 1
    return daily_returns.values
