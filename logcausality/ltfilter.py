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


def filtered(conf, edict, l_filter):
    # edict : {eid : [datetime, ...]}
    if len(l_filter) == 0:
        return edict, evmap
    if "file" in l_filter:
        ff = ltfilter.IDFilter(conf.getlist("dag_filter", "filter_name"))
    if "periodic" in l_filter:
        per_th = conf.getfloat("dag_filter", "periodic_th")
        per_count = cont.getint("dag_filter", "periodic_count")
        per_term = config.str2dur(conf.getfloat("dag_filter", "periodic_th"))

    l_eid = []
    for eid, l_dt in edict.iteritems():
        if "file" in l_filter:
            if ff.isremoved(eid):
                l_eid.append(eid)
        if "periodic" in l_filter:
            temp = ltfilter.interval(l_dt, per_th, per_count, per_term)
            if temp is not None:
                l_eid.append(eid)
    return l_eid


# To filter periodic log

def interval(l_dt, threshold = 0.5, th_count = 3,
        th_term = datetime.timedelta(hours = 6)):
    # args
    #   l_dt : list of datetime.datetime
    #   threshold : threshold value for standard deviation
    #   th_term : required term length to appear periodically
    # return
    #   interval(int) if the given l_dt have stable interval
    #   or return None

    if len(l_dt) < th_count:
        # len(l_dt) < 2 : no interval will be found
        # len(l_dt) == 2 : only 1 interval that not seem periodic...
        return None
    l_interval = []
    prev_dt = None
    for dt in sorted(l_dt):
        if prev_dt is not None:
            diff = (dt - prev_dt).total_seconds()
            l_interval.append(diff)
        prev_dt = dt

    dist = np.array(l_interval)
    std = np.std(dist)
    mean = np.mean(dist)
    term = th_term.total_seconds()
    if mean == 0:
        # mean == 0 : multiple message in 1 time, not seem periodic
        return None
    if (std / mean) < threshold and mean * len(l_dt) > term:
        return int(np.median(dist))
    else:
        return None


