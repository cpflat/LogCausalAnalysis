#!/usr/bin/env python
# coding: utf-8

import sys
import math
import scipy.spatial.distance
import cPickle as pickle

import config
import log_db
import log2event
import pc_log
import pcresult


class DAGComparison():
    """
    Attributes:
        w_evmap (log2event.EventDefinitionMap): Definition of "weid".
            "weid" is the identifiers of events in whole term
            of DAG generation, and must be distinguished with "eid".
        d_ev (Dict[Dict[int]]): Record of event appearance in DAGs.
            Its key is the status of DAGs (top_dt, end_dt, area).
            Its value is a dictionary of event appearance
            (key : weid, val : number of appearance).
    """

    def __init__(self, conf, area, reset = False):
        self.fn_header = "compare_graph_temp_" #TODO
        self.log_base = 2
        self.area = area
        self.fn = self.fn_header + "area"

        self.w_evmap = None
        self.d_ev = {}
        if reset:
            self._init_event_stat(conf)
        else:
            self._open(conf)

    def _open(self, conf):
        try:
            self.load()
        except IOError:
            self._init_event_stat(conf)
        assert self.w_evmap is not None

    def _init_event_stat(self, conf):
        ld = log_db.LogData(conf)
        w_top_dt, w_end_dt = pc_log.whole_term(conf, ld)
        gid_name = conf.get("dag", "event_gid")
        self.w_evmap = log2event.EventDefinitionMap(w_top_dt, w_end_dt,
                gid_name)

        src_dir = conf.get("dag", "output_dir")
        l_r = pcresult.results_in_area(conf, src_dir, self.area)
        for r in l_r:
            edict = {}
            for line in ld.iter_lines(top_dt = r.top_dt, end_dt = r.end_dt,
                    area = r.area):
                weid = self.w_evmap.process_line(line)
                edict[weid] = edict.get(weid, 0) + 1
            self.d_ev[(r.top_dt, r.end_dt, r.area)] = edict

    @staticmethod
    def tf(weid, edict):
        all_freq = sum(v for v in edict.values())        
        if edict.has_key(weid):
            freq = edict[weid]
            return 1.0 * freq / all_freq
        else:
            return 0.0

    def df(self, weid):
        ret = 0
        for edict in self.d_ev.values():
            if edict.has_key(weid):
                ret += 1
        return 1.0 * ret / len(self.d_ev)

    def tfidf(self, weid, edict):
        tf = self.tf(weid, edict)
        df = self.df(weid)
        return tf * math.log(1.0 / df, self.log_base)

    def data_for_r(self, r):
        return self.d_ev[(r.top_dt, r.end_dt, r.area)]

    def load(self, fn = None):
        if fn is None:
            fn = self.fn
        with open(self.fn, "r") as f:
            obj = pickle.load(f)
        self.d_ev, self.w_evmap = obj

    def dump(self):
        obj = (self.d_ev, self.w_evmap)
        with open(self.fn, "w") as f:
            pickle.dump(obj, f)


def _event_vector(l_weid, edict, dagc):
    ret = []
    for weid in l_weid:
        ret.append(dagc.tfidf(weid, edict))
    return ret


def _evv_distance(evv1, evv2):
    if sum(v != 0.0 for v in evv1) == 0:
        return 0.0
    elif sum(v != 0.0 for v in evv2) == 0:
        return 0.0
    else:
        return scipy.spatial.distance.cosine(evv1, evv2)


def search_similar_dag(conf, top_dt, end_dt, area):
    ld = log_db.LogData(conf)
    dagc = DAGComparison(conf, area)
    
    edict = {}
    for line in ld.iter_lines(top_dt = top_dt, end_dt = end_dt, area = area):
        weid = dagc.w_evmap.process_line(line)
        edict[weid] = edict.get(weid, 0) + 1
    l_weid = edict.keys()
    src_evv = _event_vector(l_weid, edict, dagc)

    src_dir = conf.get("dag", "output_dir")
    l_r = pcresult.results_in_area(conf, src_dir, area)
    result = []
    for r in l_r:
        if r.end_dt > top_dt and r.top_dt < end_dt:
            # ignore if common term included
            pass
        else:
            edict_r = dagc.data_for_r(r)
            r_evv = _event_vector(l_weid, edict_r, dagc)
            dist = _evv_distance(src_evv, r_evv)
            result.append((r, dist))

    return result


def test_dag_search(conf):
    src_dir = conf.get("dag", "output_dir")
    l_area = pcresult.result_areas(conf)
    for area in l_area:
        l_r = pcresult.results_in_area(conf, src_dir, area)
        result = []
        for r in l_r:
            result = search_similar_dag(conf, r.top_dt, r.end_dt, r.area)
            print r.cond_str()
            if len(result) > 10:
                result = sorted(result,
                        key = lambda x: x[1], reverse = True)[:10]
            for r_found, val in result:
                print val, r_found.cond_str() 
            print


def test_init_searchobj(conf):
    l_area = pcresult.result_areas(conf)
    for area in l_area:
        dagc = DAGComparison(conf, area)
        dagc.dump()


if __name__ == "__main__":
    usage = "usage: {0} [options]".format(sys.argv[0])
    import optparse
    op = optparse.OptionParser(usage)
    op.add_option("-i", "--init", action="store_true",
            dest="init", default=False,
            help="Only initialize comparizon object (for speedup searching)")
    op.add_option("-c", "--config", action="store",
            dest="conf", type="string", default=config.DEFAULT_CONFIG_NAME,
            help="configuration file path")
    (options, args) = op.parse_args()

    conf = config.open_config(options.conf)
    #config.set_common_logging(conf, _logger)
    if options.init:
        test_init_searchobj(conf)
    else:
        test_dag_search(conf)


