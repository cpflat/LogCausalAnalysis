#!/usr/bin/env python
# coding: utf-8

import sys
import os
import datetime

import config
import calc
import log_db
import timelabel
from logging import getLogger

_config = config.common_config()
_logger = getLogger(__name__)


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
        _logger.warning("Filter definition file not found".format(fn))

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


class logger():

    def __init__(self, log_fn):
        self.fn = log_fn
        with open(self.fn, "a") as f:
            f.write("\n")
            f.write("#{0}\n".format(datetime.datetime.today()))

    def add(self, ltid, cnt, dur, val):
        with open(self.fn, "a") as f:
            f.write("ltid {0} : {1} in {2} (cnt {3})\n".format(
                ltid, val, dur, cnt))


def mkfilter_self_corr_ltid(ldb, ltid, top_dt, end_dt, l_dur):
    l_dt = [line.dt for line in ldb.generate(ltid=ltid)]
    cnt = len(l_dt)
    if cnt > 0:
        ret = []
        for dur in l_dur:
            ts = timelabel.TimeSeries(top_dt, end_dt, 
                    _config.getdur("filter", "corr_bin_size"), 0)
            ts.countdata(l_dt)
            c = calc.self_corr(ts.data, dur, len(ts.label))
            ret.append((c, dur))
        c, dur = max(ret)
        return c, dur, cnt
    else:
        return None, None, cnt


def mkfilter_self_corr_file(fn, lfn, top_dt, end_dt, l_dur, threshold):
    ret = []
    lg = logger(lfn)
    ldb = log_db.ldb_manager()
    with open(fn, "r") as f:
        for line in f:
            if line == "\n": continue
            ltid = int(line.rstrip("\n"))
            for dur in l_dur:
                c, dur, cnt = mkfilter_self_corr_ltid(ldb, ltid, 
                        top_dt, end_dt, dur)
            lg.add(ltid, cnt, dur, c)
            if c is None:
                pass
            elif c > threshold:
                ret.append(ltid)
    return ret


def mkfilter_self_corr(lfn, top_dt, end_dt, l_dur, threshold):
    ret = []
    lg = logger(lfn)
    ldb = log_db.ldb_manager()
    for ltline in ldb.lt:
        ltid = ltline.ltid
        c, dur, cnt = mkfilter_self_corr_ltid(ldb, ltid, top_dt, end_dt, l_dur)
        lg.add(ltid, cnt, dur, c)
        if c is None:
            pass
        elif c > threshold:
            ret.append(ltid)
    return ret


def mkfilter_log(lfn, threshold):
    ret = []
    with open(lfn, "r") as f:
        for line in f:
            if line == "\n":
                pass
            elif line[0] == "#":
                pass
            else:
                ltid = int(line.split(" ")[1])
                try:
                    val = float(line.split(" ")[3])
                except ValueError:
                    continue
                else:
                    if val >= threshold:
                        ret.append(ltid)
    return ret


if __name__ == "__main__":
    
    import optparse
    usage = "usage: {0} [options] mode\n".format(sys.argv[0]) + \
            "\tmode : [self-corr, ]"
    op = optparse.OptionParser(usage)
    op.add_option("-f", "--file", action="store", dest="filename",
            type="string", default=None,
            help="validate ltid in given file only")
    op.add_option("-i", "--ltid", action="store", dest="ltid", type="int",
            default=None, help="validate given ltid only")
    op.add_option("-l", "--log", action="store_true", dest="lflag",
            default=False, help="reconstruct filter file from log")
    op.add_option("-t", "--threshold", action="store", dest="threshold",
            type="float", default=None, help="Threshold")
    (options, args) = op.parse_args()
    if len(args) == 0: sys.exit(usage)

    top_dt, end_dt = _config.getterm("database", "term")
    lfn = _config.get("filter", "log_filename")
    l_dur = [
            datetime.timedelta(hours=1),
            datetime.timedelta(days=1)
    ]

    if args[0] == "self-corr":
        if options.threshold is None:
            threshold = _config.getfloat("filter", "corr_threshold")
        else:
            threshold = options.threshold

        if options.lflag:
            ret = mkfilter_log(lfn, threshold)
            for i in ret:
                print i
        elif options.ltid is not None:
            ldb = log_db.ldb_manager()
            ret, dur, cnt = mkfilter_self_corr_ltid(ldb, options.ltid,
                    top_dt, end_dt, l_dur)
            print "{0} in {1} (cnt {2})".format(ret, dur, cnt)
        elif options.filename is not None:
            ret = mkfilter_self_corr_file(options.filename, lfn,
                    top_dt, end_dt, l_dur, threshold)
            for i in ret:
                print i
        else:
            _logger.info("No options given, process all data in DB")
            ret = mkfilter_self_corr(lfn, top_dt, end_dt, l_dur, threshold)
            for i in ret:
                print i

