#!/usr/bin/env python
# coding: utf-8

import datetime
import logging
from itertools import combinations 
import numpy as np

import config
import dtutil
import log_db
import log2event

_logger = logging.getLogger(__name__.rpartition(".")[-1])


class CompareVariable():

    def __init__(self, conf):
        self.evmap = log2event.LogEventIDMap()
        self.d_var = {}
        self.d_eventvar = {} # key = eid, val = {var : cnt, ...}
        self.d_varevent = {} # key = var, val = set(eid, ...)
        self.d_result = None

    def add(self, line):
        eid = self.evmap.process_line(line)
        for vid, var in enumerate(line.var()):
            self._add_var(eid, line.lt.ltid, vid, var)

    def _add_var(self, eid, ltid, vid, var):
        key1 = eid
        if not self.d_var.has_key(key1):
            self.d_var[key1] = {}
        key2 = (ltid, vid)
        if not self.d_var[key1].has_key(key2):
            self.d_var[key1][key2] = {}
        key3 = var
        if not self.d_var[key1][key2].has_key(key3):
            self.d_var[key1][key2][key3] = 0
        self.d_var[key1][key2][key3] += 1

        if not self.d_eventvar.has_key(eid):
            self.d_eventvar[eid] = {}
        self.d_eventvar[eid][var] = self.d_eventvar[eid].get(var, 0) + 1

        self.d_varevent.setdefault(var, set()).add(eid)

    def _var_tfidf(self, var, eid1, eid2):
        tf1 = 1.0 * self.d_eventvar[eid1][var] / \
                sum(self.d_eventvar[eid1].values())
        tf2 = 1.0 * self.d_eventvar[eid2][var] / \
                sum(self.d_eventvar[eid2].values())
        tf = (tf1 + tf2) / 2.0
        idf = np.log2(1.0 * len(self.d_var.keys()) / len(self.d_varevent[var]))
        return tf * idf

    def _similarlity(self, eid1, ltid1, vid1, eid2, ltid2, vid2):
        set1 = set(self.d_var[eid1][(ltid1, vid1)].keys())
        set2 = set(self.d_var[eid2][(ltid2, vid2)].keys())
        whole = set1 | set2
        common = set1 & set2
        ret = sum([self._var_tfidf(var, eid1, eid2) for var in whole
                if var in common])
        return ret / len(whole)

    def process(self):
        self.d_result = {}
        for eid1, eid2 in combinations(self.d_var.keys(), 2):
            _logger.info("evaluating event {0} and event {1}".format(
                    eid1, eid2))
            d_sim = {}
            for ltid1, vid1 in self.d_var[eid1].keys():
                for ltid2, vid2 in self.d_var[eid2].keys():
                    key = (ltid1, vid1, ltid2, vid2)
                    d_sim[key] = self._similarlity(eid1, ltid1, vid1,
                            eid2, ltid2, vid2)
            key, val = max(d_sim.items(), key = lambda x: x[1])
            self.d_result[(eid1, eid2)] = (key, val)

    def show_result(self, limit = 100, wlimit = 5):

        def show_words(l_obj, title, limit, spl = ", "):
            buf = title + " : "
            if len(l_obj) > limit:
                buf += spl.join(l_obj[:5]) + "..."
            elif len(l_obj) > 0:
                buf += spl.join(l_obj)
            return buf

        l_buf = []
        cnt = 0
        for key, val in sorted(self.d_result.iteritems(),
                key = lambda x: x[1][1], reverse = True):
            eid1, eid2 = key
            ltid1, vid1, ltid2, vid2 = val[0]
            sim = val[1]
            l_buf.append("{0} in [LTID {1} variable {2}]".format(sim,
                    ltid1, vid1) + " [LTID {0} variable {1}]".format(
                    ltid2, vid2))
            set1 = set(self.d_var[eid1][(ltid1, vid1)].keys())
            set2 = set(self.d_var[eid2][(ltid2, vid2)].keys())
            common = set1 & set2
            uncommon1 = set1 - common
            uncommon2 = set2 - common
            l_buf.append(show_words(list(common), "common", wlimit))
            l_buf.append(show_words(list(uncommon1), "event1 only", wlimit))
            l_buf.append(show_words(list(uncommon2), "event2 only", wlimit))
            l_buf.append("")
            cnt += 1
            if cnt >= limit:
                break
        return "\n".join(l_buf)


def test(conf, area):
    start_dt = datetime.datetime.now()
    _logger.info("var_cls task start")

    ld = log_db.LogData(conf)
    cv = CompareVariable(conf)
    w_term = conf.getterm("dag", "whole_term")
    if w_term is None:
        w_term = ld.whole_term()
    term = datetime.timedelta(days = 1)
    diff = datetime.timedelta(days = 1)

    for top_dt, end_dt in dtutil.iter_term(w_term, term, diff):
        _logger.info("loading log data ({0} - {1})".format(top_dt, end_dt))
        for line in ld.iter_lines(top_dt = top_dt, end_dt = end_dt,
                area = area):
            cv.add(line)

    _logger.info("log data loading done")
    _logger.info("{0} events found".format(len(cv.evmap)))
    cv.process()
    _logger.info("event relation estimating done")
    print cv.show_result()

    end_dt = datetime.datetime.now()
    _logger.info("var_cls task done ({0})".format(end_dt - start_dt))


if __name__ == "__main__":
    usage = ""
    import optparse
    op = optparse.OptionParser(usage)
    op.add_option("-a", "--area", action="store",
            dest="area", type="string", default="all",
            help="target area")
    op.add_option("-c", "--config", action="store",
            dest="conf", type="string", default=config.DEFAULT_CONFIG_NAME,
            help="configuration file path")
    (options, args) = op.parse_args()
    conf = config.open_config(options.conf)
    config.set_common_logging(conf, _logger)
    area = options.area
    
    test(conf, area)


