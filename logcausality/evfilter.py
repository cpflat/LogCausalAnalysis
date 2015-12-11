#!/usr/bin/env python
# coding: utf-8

import sys
import os
import datetime
import logging
#import cPickle as pickle
import numpy as np

import config
import dtutil
#import calc
#import log_db
#import timelabel

_logger = logging.getLogger(__name__.rpartition(".")[-1])


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
        ff = IDFilter(conf.getlist("filter", "filter_name"))
    per_count = conf.getint("filter", "periodic_count")
    per_term = conf.getdur("filter", "periodic_term")
    if "periodic" in l_filter:
        per_th = conf.getfloat("filter", "periodic_th")
    if "self-corr" in l_filter:
        corr_th = conf.getfloat("filter", "self_corr_th")
        corr_diff = [config.str2dur(diffstr) for diffstr
                     in conf.gettuple("filter", "self_corr_diff")]
        corr_bin = conf.getdur("filter", "self_corr_bin")

    l_eid = []
    for eid, l_dt in edict.iteritems():
        if "file" in l_filter:
            if ff.isremoved(eid):
                l_eid.append(eid)
                continue
        if periodic_term(l_dt, per_count, per_term):
            if "periodic" in l_filter:
                temp = interval(l_dt, per_th)
                if temp is not None:
                    l_eid.append(eid)
                    continue
            if "self-corr" in l_filter:
                corr = self_correlation(l_dt, corr_diff, corr_bin)
                if corr is not None and corr > corr_th:
                    l_eid.append(eid)
                    continue

    return l_eid


# To filter periodic log

def periodic_term(l_dt, count, term, verbose = False):
    if len(l_dt) < count:
        if verbose:
            print("Event appearance is too small, skip periodicity test")
        return False
    elif max(l_dt) - min(l_dt) < term:
        if verbose:
            print("Event appearing term is too short, skip periodicity test")
        return False
    else:
        return True


def interval(l_dt, threshold = 0.5, verbose = False):
    # args
    #   l_dt : list of datetime.datetime
    #   threshold : threshold value for standard deviation
    #   verbose : output calculating infomation to stdout
    # return
    #   interval(int) if the given l_dt have stable interval
    #   or return None

    if len(l_dt) <= 2:
    #    # len(l_dt) < 2 : no interval will be found
    #    # len(l_dt) == 2 : only 1 interval that not seem periodic...
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

    if verbose:
        print("std {0}, mean {1}, median {2}".format(std, mean,
                                                     np.median(dist)))

    if mean == 0:
        # mean == 0 : multiple message in 1 time, not seem periodic
        return None
    if (std / mean) < threshold:
        return int(np.median(dist))
    else:
        return None


def self_correlation(l_dt, l_diff, binsize):
    if binsize == datetime.timedelta(seconds = 1):
        data = l_dt
    else:
        top_dt = dtutil.adj_sep(min(l_dt), binsize)
        end_dt = dtutil.radj_sep(max(l_dt), binsize)
        l_label = dtutil.label(top_dt, end_dt, binsize)
        data = dtutil.discretize(l_dt, l_label, binarize = False)

    l_ret = []
    for diff in l_diff:
        binnum = int(diff.total_seconds() / binsize.total_seconds())
        if len(data) <= binnum * 2:
            pass
        else:
            data1 = data[:len(data) - binnum]
            data2 = data[binnum:]# + [0] * binnum
            assert len(data1) == len(data2)
            l_ret.append(np.corrcoef(np.array(data1), np.array(data2))[0, 1])

    if len(l_ret) > 0:
        return max(l_ret)
    else:
        return None


def test_filter(conf, area = "all", limit = 10):
    import log_db
    import log2event
    ld = log_db.LogData(conf)
    w_term = conf.getterm("dag", "whole_term")
    if w_term is None:
        w_term = ld.whole_term()
        print w_term
    term = conf.getdur("dag", "unit_term")
    diff = conf.getdur("dag", "unit_diff")
    dur = conf.getdur("dag", "stat_bin")

    l_filter = conf.gettuple("dag", "use_filter")
    print l_filter
    if "file" in l_filter:
        ff = IDFilter(conf.getlist("filter", "filter_name"))
    per_count = conf.getint("filter", "periodic_count")
    per_term = conf.getdur("filter", "periodic_term")
    if "periodic" in l_filter:
        per_th = conf.getfloat("filter", "periodic_th")
    if "self-corr" in l_filter:
        corr_th = conf.getfloat("filter", "self_corr_th")
        corr_diff = [config.str2dur(diffstr) for diffstr
                     in conf.gettuple("filter", "self_corr_diff")]
        corr_bin = conf.getdur("filter", "self_corr_bin")

    for top_dt, end_dt in dtutil.iter_term(w_term, term, diff):
        print("[Testing {0} - {1}]".format(top_dt, end_dt))
        s_eid = set()
        edict, evmap = log2event.log2event(conf, top_dt, end_dt, area)
        for eid, l_dt in edict.iteritems():
            ltgid, host = evmap.info(eid)
            print("Event {0} : ltgid {1} in host {2} ({3})".format(eid,
                    ltgid, host, len(l_dt)))
            ld.show_log_repr(limit = limit, ltgid = ltgid,
                    top_dt = top_dt, end_dt = end_dt, host = host, area = area)
            if "file" in l_filter:
                if ff.isremoved(eid):
                    print("found in definition file, removed")
                    s_eid.add(eid)
            if periodic_term(l_dt, per_count, per_term, verbose = True):
                if "periodic" in l_filter:
                    temp = interval(l_dt, per_th, verbose = True)
                    if temp is not None:
                        print("rule [periodic] safisfied, removed")
                        print("interval : {0}".format(temp))
                        s_eid.add(eid)
                if "self-corr" in l_filter:
                    corr = self_correlation(l_dt, corr_diff, corr_bin)
                    if corr is not None:
                        print("self-correlation : {0}".format(corr))
                        if corr > corr_th:
                            print("rule [self-corr] satisfied, removed")
                            s_eid.add(eid)
            print
        print("[Summary in term {0} - {1}]".format(top_dt, end_dt))
        print("  {0} events found, {1} events filtered, {2} remains".format(\
                len(edict), len(s_eid), len(edict) - len(s_eid)))
        print


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit("usage: {0} config (area)".format(sys.argv[0]))
    confname = sys.argv[1]
    if len(sys.argv) >= 3:
        area = sys.argv[2]
    else:
        area = "all"
    conf = config.open_config(confname)
    config.set_common_logging(conf, _logger, [])
    test_filter(conf, area)



