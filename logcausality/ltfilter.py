#!/usr/bin/env python
# coding: utf-8

import sys
import os
import datetime
import logging
#import cPickle as pickle
import numpy as np

import config
#import calc
#import log_db
#import timelabel

_logger = logging.getLogger(__name__)


class IDFilter():

    def __init__(self, def_fn):
        self.fids = set()
        if isinstance(def_fn, list):
            for fn in def_fn:
                if os.path.exists(fn):
                    self._open_def(fn)
                else:
                    self._exception_notfound(def_fn)
        elif isinstance(def_fn, str):
            if os.path.exists(def_fn):
                self._open_def(def_fn)
            else:
                self._exception_notfound(def_fn)

    def _exception_notfound(self, def_fn):
        _logger.warning("Filter definition file {0} not found".format(fn))

    def _open_def(self, fn):
        with open(fn, 'r') as f:
            for line in f:
                line = line.strip("\n")
                if line == "":
                    pass
                elif line[0] == "#":
                    pass
                else:
                    self.fids.add(int(line))

    def isremoved(self, nid):
        return nid in self.fids


# To filter cyclic log

def interval(self, l_dt, threshold):
    # args
    #   l_dt : list of datetime.datetime
    #   threshold : threshold value for standard deviation
    # return
    #   interval(int) if the given l_dt have stable interval
    #   or return None

    l_interval = []
    prev_dt = None
    for dt in sorted(l_dt):
        if prev_dt is not None:
            diff = (dt - prev_dt).total_seconds()
            l_interval.append(diff)
        prev_dt = dt

    dist = np.array(l_interval)
    std = np.std(dist)
    if std < threshold:
        return int(np.median(dist))
    else:
        return None

