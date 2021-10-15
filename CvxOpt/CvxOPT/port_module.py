import numpy as np
import scipy.sparse.linalg
import cvxpy as cp
import scipy.linalg as sl
import scipy.stats as st


def MVO(mu, Sigma, r_lambda=1, short=False, LN=None, LN_bound=1):
    """

    :param mu: mean vector
    :param Sigma: covariance matrix
    :param r_lambda: risk tolerance
    :param short: if shorting is allowed
    :param LN: n-norm
    :param LN_bound: upper constraint of n-norm
    :return:
    """
    n = len(mu)
    e = np.ones((n, 1))
    x = cp.Variable(n)

    objective = cp.Minimize(
        r_lambda * cp.quad_form(x, Sigma) - mu.T @ x
    )
    constraints = [
        e.T @ x == 1
    ]

    if not short:
        constraints += [
            x >= 0
        ]
    if LN is not None:
        constraints += [
            cp.norm(x, LN) <= LN_bound
        ]

    prob = cp.Problem(
        objective,
        constraints
    )
    assert prob.is_dcp()
    result = prob.solve()
    return result, prob.value, x.value


def RMVO(mu, Sigma, n_samples, r_lambda=1, short=False, option="box", con_lvl=0.95):
    """

    :param mu: ret vector
    :param Sigma: Cov matrix
    :param n_samples: n samples
    :param r_lambda: risk tolerance [1,10]
    :param short: bool
    :param option: "box" or "ellipse"
    :return:
    """
    n = len(mu)
    Theta = np.diag(np.diag(Sigma)) / n_samples
    Theta_root = sl.sqrtm(Theta)
    e = np.ones((n,))
    x = cp.Variable(n)

    if option == "box":
        delta = np.diagonal(Theta_root)

        z = con_lvl + (1 - con_lvl) / 2
        epsilon = st.norm.ppf(z)

        y = cp.Variable(n)
        constraint = [
            y <= x,
            y >= -x,
            e.T @ x == 1,
        ]

        obj = cp.Minimize(
            r_lambda * cp.quad_form(x, Sigma) - mu.T @ x + epsilon * delta.T @ y
        )

    else:
        epsilon = np.sqrt(st.chi2.ppf(con_lvl, n))

        constraint = [
            e.T @ x == 1,
        ]

        obj = cp.Minimize(
            r_lambda * cp.quad_form(x, Sigma) - mu.T @ x + epsilon * cp.norm(Theta_root @ x, 2),
        )

    if not short:
        constraint += [x >= 0]
    prob = cp.Problem(obj, constraint)
    assert prob.is_dcp()
    result = prob.solve()
    return result, prob.value, x.value


def ERC(mu, Sigma, kappa=10):
    n = len(mu)
    y = cp.Variable(n)

    term2 = 0
    for i in range(n):
        term2 += cp.log(y[i])

    obj = cp.Minimize(1 / 2 * cp.quad_form(y, Sigma) - kappa * term2)
    constraint = [
        y >= 0
    ]
    prob = cp.Problem(obj, constraint)
    assert prob.is_dcp()
    result = prob.solve()
    return result, prob.value, y.value / y.value.sum()
