import statsmodels.api as sm
import numpy as np
import pandas as pd

TRADING_DAYS = 252


def total_return(returns: pd.Series, annualized: bool = True) -> float:
    """
  Calculate the total return of a security or portfolio.

  Parameters:
  - returns: Daily returns of the security or portfolio.

  Returns:
  - float: total return.
  """
    compounded_return = (1 + returns).prod() - 1

    if annualized:
        periods = len(returns)

        annualized_return = (1 + compounded_return) ** (TRADING_DAYS / periods) - 1

        return annualized_return

    else:
        return compounded_return


def volatility(returns: pd.Series, annualized: bool = True) -> float:
    """
  Calculate the volatility of a security or portfolio.

  Parameters:
  - returns: Daily returns of the security or portfolio.

  Returns:
  - float: volatility (annualized).
  """

    standard_deviation = returns.std()

    return standard_deviation * np.sqrt(TRADING_DAYS) if annualized else standard_deviation


def alpha(xs_returns: pd.Series, xs_bmk_returns: pd.Series, annualized: bool = True) -> float:
    """
  Calculate the alpha of a security or portfolio.

  Parameters:
  - xs_returns: Excess daily returns of the security or portfolio.
  - xs_bmk_returns: Excess daily returns of the benchmark.

  Returns:
  - list: A list containing the alpha, lower bound of the 95% confidence interval, and upper bound of the 95% confidence interval.
  """
    X = xs_bmk_returns
    Y = xs_returns

    X = sm.add_constant(X)
    model = sm.OLS(Y, X).fit()

    intercept, slope = model.params

    return intercept * TRADING_DAYS if annualized else intercept


def beta(xs_returns: pd.Series, xs_bmk_returns: pd.Series) -> float:
    """
  Calculate the beta of a security or portfolio to the benchmark.

  Parameters:
  - xs_returns: Excess daily returns of the security or portfolio.
  - xs_bmk_returns: Excess daily returns of the benchmark.

  Returns:
  - float: beta.
  """
    X = xs_bmk_returns
    Y = xs_returns

    X = sm.add_constant(X)
    model = sm.OLS(Y, X).fit()

    intercept, slope = model.params

    return slope


def sharpe_ratio(returns: pd.Series, rf_returns: pd.Series, annualized: bool = True) -> float:
    """
  Calculate the Sharpe Ratio of a portfolio.

  Parameters:
  - xs_returns: Excess daily returns of the portfolio.

  Returns:
  - float: Sharpe Ratio.
  """
    numerator = (returns - rf_returns).mean()
    denominator = returns.std()

    ratio = numerator / denominator

    annual_factor = TRADING_DAYS / np.sqrt(TRADING_DAYS)

    return ratio * annual_factor if annualized else ratio


def tracking_error(returns: pd.Series, bmk_returns: pd.Series, annualized: bool = True) -> float:
    """
  Calculate the tracking_error/active_risk of a portfolio.

  Parameters:
  - returns: Daily returns of the portfolio.
  - bmk_returns: Daily returns of the benchmark.


  Returns:
  - float: tracking_error/active_risk.
  """
    port_tracking_error = np.sqrt((returns - bmk_returns).std())
    annual_factor = np.sqrt(TRADING_DAYS)

    return port_tracking_error * annual_factor if annualized else port_tracking_error


def information_ratio(returns: pd.Series, bmk_returns: pd.Series, rf_returns: pd.Series, annualized: bool = True) -> float:
    """
  Calculate the Information Ratio of a portfolio.

  Parameters:
  - returns: Daily returns of the portfolio.
  - bmk_returns: Daily returns of the benchmark.
  - rf_returns: Daily returns of the risk-free rate.



  Returns:
  - float: information ratio.
  """
    xs_returns = returns - rf_returns
    xs_bmk_returns = bmk_returns - rf_returns

    port_alpha = alpha(xs_returns, xs_bmk_returns)
    port_tracking_error = tracking_error(returns, bmk_returns)

    ratio = port_alpha / port_tracking_error

    annual_factor = TRADING_DAYS / np.sqrt(TRADING_DAYS)

    return ratio * annual_factor if annualized else ratio
