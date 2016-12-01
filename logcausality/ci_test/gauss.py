#!/usr/bin/env python
# coding: utf-8

import numpy as np
import logging

_logger = logging.getLogger(__name__)

def zstat(x, y, s, cm, n):
    """float: Get Fisher's Z statistics."""
 
    def log_q1pm(r):
        return np.log1p(2 * r / (1 - r))

    r = pcor_order(x, y, s, cm)
    zstat = np.sqrt(n - len(s) - 3) * 0.5 * log_q1pm(r)
    if np.isnan(zstat):
        return 0
    else:
        return zstat


def pcor_order(x, y, s, cm):
    """float: Get partial correlation coefficient from correlation matrix."""
    if len(s) == 0:
        return cm[x, y]
    else:
        pim = np.linalg.pinv(cm[[x, y] + s, :][:, [x, y] + s])
        return -pim[0, 1] / np.sqrt(pim[0, 0] * pim[1, 1])


