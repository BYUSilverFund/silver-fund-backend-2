import pytest
import numpy as np
import statsmodels.api as sm
from functions import portfolio_alpha, portfolio_beta

def test_portfolio_alpha():
  np.random.seed(0)
  X = [x for x in range(50)]
  y = [2*x for x in range(50)]

  X = sm.add_constant(X)

  model = sm.OLS(y, X)
  results = model.fit()

  true_alpha = results.params[0]

  test_alpha = portfolio_alpha(X,y)

  assert test_alpha == true_alpha

def test_portfolio_beta():
  np.random.seed(0)
  X = [x for x in range(50)]
  y = [2*x for x in range(50)]

  X = sm.add_constant(X)

  model = sm.OLS(y, X)
  results = model.fit()

  true_beta = results.params[1]

  test_beta = portfolio_beta(X,y)

  assert test_beta == true_beta
