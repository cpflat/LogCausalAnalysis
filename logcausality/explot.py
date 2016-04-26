#!/usr/bin/env python
# coding: utf-8

import numpy as np
from matplotlib.colors import LinearSegmentedColormap
import cPickle as pickle

def generate_cmap(colors):
    values = range(len(colors))

    vmax = np.ceil(np.max(values))
    color_list = []
    for v, c in zip(values, colors):
        color_list.append( ( v/ vmax, c) )
    return LinearSegmentedColormap.from_list('custom_cmap', color_list)


def dump(fn, datas):
    with open(fn, "w") as f:
        pickle.dump(datas, f)


def load(fn):
    with open(fn, "r") as f:
        r = pickle.load(f)
    return r

