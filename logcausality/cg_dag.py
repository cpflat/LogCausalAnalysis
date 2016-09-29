#!/usr/bin/env python
# coding: utf-8

import sys

import common
import config
import pc_log
import pcresult


def similar_block_dag(conf, top_dt, end_dt, area, method, ignore_same = True):
    src_dir = conf.get("dag", "output_dir")
    ig_direction = conf.getboolean("search", "dag_ig_direction")
    wflag = conf.getboolean("search", "dag_weight")

    dur = conf.getdur("dag", "stat_bin")
    name = pc_log.thread_name(conf, top_dt, end_dt, dur, area) 
    if name in common.rep_dir(src_dir):
        r_temp = pcresult.PCOutput(conf).load(name)
    else:
        r_temp = pc_log.pc_log(conf, top_dt, end_dt, dur, area, dump = False)

    src_dir = conf.get("dag", "output_dir")
    l_r = pcresult.results_in_area(conf, src_dir, area)
    weight = None
    if wflag:
        weight = pcresult.EdgeTFIDF(l_r)

    result = []
    for r in l_r:
        if ignore_same and (r.end_dt > top_dt and r.top_dt < end_dt):
            # ignore if common term included
            pass
        else:
            if method == "dag_ed":
                dist = pcresult.graph_edit_distance(r_temp, r,
                        ig_direction, weight)
            elif method == "dag_mcs":
                dist = pcresult.mcs_size_ratio(r_temp, r,
                        ig_direction, weight)
            else:
                raise NotImplementedError
            result.append((r, dist))

    return result



