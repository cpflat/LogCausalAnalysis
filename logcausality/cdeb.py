#!/usr/bin/env python
# coding: utf-8

"""Common functions for debugging (assuming to use in pdb interactive debug).
"""

import pdb
import cPickle as pickle


def fadd(string, fn):
    with open(fn, 'a') as f:
        f.write(string)


def pickle_dump(data, fn):
    with open(fn, 'w') as f:
        pickle.dump(data, f)


def pickle_load(fn):
    with open(fn, 'w') as f:
        ret = pickle.load(f)
    return ret

