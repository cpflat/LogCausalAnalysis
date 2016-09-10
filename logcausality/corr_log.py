#!/usr/bin/env python
# coding: utf-8

import sys
import time
import datetime
import threading
import logging
import numpy as np
import cPickle as pickle
import networkx as nx
from itertools import combinations

import config
import fslib
import pc_log
import log_db
import log2event
import evfilter
import pcresult

_logger = logging.getLogger(__name__.rpartition(".")[-1])


def corr_graph(data, threshold):
    g = nx.DiGraph()
    l_nid = data.keys()
    for n1, n2 in combinations(l_nid, 2):
        t1 = data[n1]
        t2 = data[n2]
        corr = np.corrcoef(np.array(t1), np.array(t2))[0, 1]
        if corr >= threshold:
            g.add_edge(n1, n2)
            g.add_edge(n2, n1)
    return g


def corr_log(conf, top_dt, end_dt, dur, area, dump = True):

    _logger.info("job start ({0} - {1} in {2})".format(top_dt, end_dt, area))
    
    edict, evmap = pc_log.get_edict(conf, top_dt, end_dt, dur, area)

    _logger.info("{0} events found in given term of log data".format(
            len(edict)))
    if dump:
        tempfn = pc_log.thread_name(conf, top_dt, end_dt, dur, area) + ".temp"
        with open(tempfn, 'w') as f:
            pickle.dump((edict, evmap), f)

    if len(edict) >= 2:
        threshold = conf.getfloat("dag", "threshold_corr")
        data = log2event.event2stat(edict, top_dt, end_dt, dur)
        graph = corr_graph(data, threshold)
    else:
        _logger.info("insufficient events({0}), return empty dag".format(\
                len(edict)))
        graph = pc_input.empty_dag()

    output = pcresult.PCOutput(conf)
    output.make(graph, evmap, top_dt, end_dt, dur, area)
    if dump:
        output.dump()
        fslib.rm(tempfn)

    _logger.info("job done, output {0}".format(output.filename))
    return output


def corr_mthread(l_args, pal=1):

    start_dt = datetime.datetime.now()
    _logger.info("corr_log task start ({0} jobs)".format(len(l_args)))

    l_thread = [threading.Thread(name = pc_log.thread_name(*args),
        target = corr_log, args = args) for args in l_args]

    l_job = []
    while len(l_thread) > 0:
        if len(l_job) < pal:
            job = l_thread.pop(0)
            job.start()
            l_job.append(job)
        else:
            time.sleep(1)
            l_job = [j for j in l_job if j.is_alive()]
    else:
        for job in l_job:
            job.join()

    end_dt = datetime.datetime.now()
    _logger.info("corr_log task done ({0})".format(end_dt - start_dt))


if __name__ == "__main__":
    
    usage = "usage: {0} [options]".format(sys.argv[0])
    import optparse
    op = optparse.OptionParser(usage)
    op.add_option("-c", "--config", action="store",
            dest="conf", type="string", default=config.DEFAULT_CONFIG_NAME,
            help="configuration file path")
    op.add_option("-p", "--parallel", action="store", dest="pal", type="int",
            default=1, help="multithreading")
    op.add_option("--debug", action="store_true", dest="debug",
            default=False, help="set logging level to DEBUG")
    (options, args) = op.parse_args()

    conf = config.open_config(options.conf)
    lv = logging.DEBUG if options.debug else logging.INFO
    config.set_common_logging(conf, _logger, ["evfilter"], lv = lv)

    fslib.mkdir(conf.get("dag", "output_dir"))
    l_args = pc_log.pc_all_args(conf)

    corr_mthread(l_args, options.pal)


