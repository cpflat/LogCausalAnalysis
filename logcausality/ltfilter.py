#!/usr/bin/env python
# coding: utf-8

import sys
import os
import datetime
import logging
import cPickle as pickle

import config
import calc
import log_db
import timelabel

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


class CalculateSelfCorr():

    def __init__(self, conf, fflag):
        self.ld = log_db.LogData(conf)
        self.filename = conf.get("filter_self_corr", "indata_filename")
        w_term = conf.getterm("filter_self_corr", "term")
        if w_term is None:
            self.top_dt, self.end_dt = self.ld.whole_term()
        else:
            self.top_dt, self.end_dt = w_term
        self.l_dur = [config.str2dur(str_dur) for str_dur
                in conf.getlist("filter_self_corr", "dur")]
        self.binsize = conf.getdur("filter_self_corr", "bin_size")
        self.th = conf.getfloat("filter_self_corr", "threshold")

        self.d_result = {}
        self.d_info = {}

        self.fflag = fflag
        if self.loaded():
            self.load()

    def loaded(self):
        return os.path.exists(self.filename) and not self.fflag

    def _add_result(self, val, ltgid, dur, cnt):
        self.d_result[(ltgid, dur)] = val
        self.d_info[ltgid] = cnt

    def calc_self_corr(self, ltgid):
        l_dt = [line.dt for line in self.ld.iter_lines(ltgid = ltgid,
                top_dt = self.top_dt, end_dt = self.end_dt)]
        cnt = len(l_dt)
        if cnt > 0:
            for dur in self.l_dur:
                ts = timelabel.TimeSeries(self.top_dt, self.end_dt,
                        self.binsize, 0)
                ts.countdata(l_dt)
                c = calc.self_corr(ts.data, dur, len(ts.label))
                self._add_result(c, ltgid, dur, cnt)
        else:
            _logger.info("no data for ltgid {0}".format(ltgid))

    def calc_all(self):
        for ltgid in self.ld.iter_ltgid():
            _logger.info("calculating ltgid {0}".format(ltgid))
            self.calc_self_corr(ltgid)
        self.dump()

    def show_result(self):
        buf = []
        for k, val in sorted(self.d_result.items(), key = lambda x: x[0][0]):
            ltgid, dur = k
            cnt = self.d_info[ltgid]
            buf.append("ltgid {0} : {1} in {2} (cnt {3})".format(
                ltgid, val, dur, cnt))
        return "\n".join(buf)

    def show_filtered(self, threshold = None):
        if threshold is None:
            threshold = self.th
        ret = set()
        for k, val in self.d_result.items():
            ltgid, dur = k
            if val > threshold:
                ret.add(ltgid)
        for ltgid in ret:
            print ltgid

    def load(self):
        with open(self.filename, 'r') as f:
            self.d_result, self.d_info = pickle.load(f)

    def dump(self):
        with open(self.filename, 'w') as f:
            obj = self.d_result, self.d_info
            pickle.dump(obj, f)


def mkfilter_self_corr(conf, fflag, threshold = None):
    sc = CalculateSelfCorr(conf, fflag)
    if sc.loaded():
        pass
    else:
        sc.calc_all()
    print sc.show_filtered(threshold)


def show_self_corr(conf):
    sc = CalculateSelfCorr(conf, False)
    if sc.loaded():
        print sc.show_result()


if __name__ == "__main__":
    
    import optparse
    usage = "usage: {0} [options] mode".format(sys.argv[0])
    op = optparse.OptionParser(usage)
    op.add_option("-c", "--config", action="store",
            dest="conf", type="string", default=config.DEFAULT_CONFIG_NAME,
            help="configuration file path")
    op.add_option("-f", action="store_true", dest="fflag",
            default=False, help="format indata and recalculate")
    op.add_option("-t", "--threshold", action="store", dest="threshold",
            type="float", default=None, help="Threshold")
    (options, args) = op.parse_args()
    if len(args) == 0: sys.exit(usage)

    conf = config.open_config(options.conf)
    config.set_common_logging(conf, _logger)

    if args[0] == "self-corr":
        mkfilter_self_corr(conf, options.fflag, options.threshold)
    elif args[0] == "show-self-corr":
        show_self_corr(conf) 

