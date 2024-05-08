from typing import List
import statsmodels.api as sm

def portfolio_alpha(xs_port_returns: List[float], xs_bmk_returns: List[float]):

  xs_port_returns = sm.add_constant(xs_port_returns)

  model = sm.OLS(xs_bmk_returns, xs_port_returns)
  results = model.fit()

  alpha = results.params[0]
  # beta = results.params[1]

  return alpha