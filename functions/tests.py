import pytest
import numpy as np
import statsmodels.api as sm
from functions import portfolio_alpha, portfolio_beta

def test_portfolio_alpha():
  np.random.seed(0)
  X = [x for x in range(50)]
  y = [x+1 for x in range(50)]

  true_alpha = 1

  test_alpha = round(portfolio_alpha(X,y))

  assert test_alpha == true_alpha

def test_portfolio_beta():
  np.random.seed(0)
  X = [x for x in range(50)]
  y = [2*x for x in range(50)]

  true_beta = 2

  test_beta = round(portfolio_beta(X,y))

  assert test_beta == true_beta
