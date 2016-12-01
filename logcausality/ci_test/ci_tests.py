#!/usr/bin/env python
# coding: utf-8

import numpy as np
from scipy.stats import norm

from .gauss import zstat

def ci_test_gauss(data_matrix, x, y, s, **kwargs):

    assert 'corr_matrix' in kwargs
    cm = kwargs['corr_matrix']
    n = data_matrix.shape[0]

    z = zstat(x, y, list(s), cm, n)
    p_val = 2.0 * norm.sf(np.absolute(z))
    return p_val


if __name__ == "__main__":
    import gauss_testdata
    dm = np.array(gauss_testdata.data)
    cm = np.corrcoef(dm.T)

    v = ci_test_gauss(dm, 0, 2, [1], corr_matrix = cm)
    print("x = 0, y = 2, z = [1] : {0} (R:0.787275)".format(v))
    v = ci_test_gauss(dm, 0, 2, [], corr_matrix = cm)
    print("x = 0, y = 2, z = Null : {0} (R:1.11125330537e-53)".format(v))
