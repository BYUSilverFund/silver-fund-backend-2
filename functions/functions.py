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

        annualized_return = (1 + compounded_return) * (TRADING_DAYS / periods) - 1

        return annualized_return

    else:
        return compounded_return


def holding_period_return(values: pd.Series, dividends: pd.Series, annualized: bool = True) -> float:
    starting_value = values.iloc[0]
    ending_value = values.iloc[-1]

    dividends_sum = dividends.sum()

    result = (ending_value + dividends_sum) / starting_value - 1

    if annualized:
        periods = len(values)

        annualized_return = (1 + result) * (TRADING_DAYS / periods) - 1

        return annualized_return

    else:
        return result


def cumulative_return_vector(df: pd.DataFrame, date_col: str, value_col: str, return_col: str, bmk_col: str) -> pd.DataFrame:
    xf = df[[date_col, value_col, return_col, bmk_col]].copy()

    xf[date_col] = xf[date_col].dt.strftime('%Y-%m-%d')

    # Portfolio
    xf['cumulative_return_port'] = (1 + xf[return_col]).cumprod() - 1

    xf['cumulative_return_port'] = round(xf['cumulative_return_port'] * 100, 2)

    # Benchmark
    xf['cumulative_return_bmk'] = (1 + xf[bmk_col]).cumprod() - 1

    xf['cumulative_return_bmk'] = round(xf['cumulative_return_bmk'] * 100, 2)

    xf = xf.drop(columns=[return_col, bmk_col])

    return xf


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

    num_days = xs_returns.count()

    X = xs_bmk_returns
    Y = xs_returns

    X = sm.add_constant(X)
    model = sm.OLS(Y, X).fit()

    intercept, slope = model.params

    return intercept * TRADING_DAYS if annualized else (1 + intercept) ** num_days - 1


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
    port_tracking_error = (returns - bmk_returns).std()
    annual_factor = np.sqrt(TRADING_DAYS)

    return port_tracking_error * annual_factor if annualized else port_tracking_error


def information_ratio(returns: pd.Series, bmk_returns: pd.Series, rf_returns: pd.Series,
                      annualized: bool = True) -> float:
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


def alpha_contribution(xs_returns: pd.Series, xs_bmk_returns: pd.Series, weights: pd.Series,
                       annualized: bool = True) -> float:
    """
  Calculate the contribution to alpha of a security.

  Parameters:
  - xs_returns: Excess daily returns of the security or portfolio.
  - xs_bmk_returns: Excess daily returns of the benchmark.
  - weights: Weights at the open of each trading day for a particular holding in a portfolio

  Returns:
  - list: A list containing the alpha, lower bound of the 95% confidence interval, and upper bound of the 95% confidence interval.
  """
    X = xs_bmk_returns * weights
    Y = xs_returns

    X = sm.add_constant(X)
    model = sm.OLS(Y, X).fit()

    intercept, slope = model.params

    return intercept * TRADING_DAYS if annualized else intercept
