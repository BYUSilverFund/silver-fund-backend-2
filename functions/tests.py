import pytest
import numpy as np
import statsmodels.api as sm
from functions import portfolio_alpha

def test_portfolio_alpha():
  np.random.seed(0)
  X = [x for x in range(50)]
  y = [2*x for x in range(50)]

  X = sm.add_constant(X)

  # Fit the OLS model
  model = sm.OLS(y, X)
  results = model.fit()

  true_alpha = results.params[0]

  test_alpha = portfolio_alpha(X,y)

  assert test_alpha == true_alpha
