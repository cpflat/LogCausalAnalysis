#!/usr/bin/env python
# coding: utf-8

import sys
import time
import datetime
import optparse
import threading
import logging
import logging.config

import config
import fslib
import log2event
import pc_input 
import pcresult

_config = config.common_config()
_logger = logging.getLogger(__name__)


def pc_log(dirname, top_dt, end_dt, dur, area, threshold):

    _logger.debug("job start ({0} - {1} in {2})".format(top_dt, end_dt, area))

    edict, evmap = log2event.log2event(top_dt, end_dt, dur, area)
    
    if len(edict) > 2:
        graph = pc_input.pc(edict, threshold)
    else:
        _logger.debug("insufficient events({0}), return empty dag".format(\
                len(edict)))
        graph = pc_input.empty_dag()

    output = pcresult.PCOutput(dirname)
    output.make(graph, evmap, top_dt, end_dt, dur, area, threshold)
    output.dump()
    _logger.debug("job done, output {0}".format(output.filename))
    return output


def thread_name(dirname, top_dt, end_dt, dur, area, threshold):
    l_header = []
    l_header.append(dirname)
    l_header.append("/")
    l_header.append(area)
    l_header.append("_")
    if end_dt - top_dt - datetime.timedelta(days = 1) < \
            datetime.timedelta(seconds = 2):
        l_header.append(top_dt.strftime("%Y%m%d"))
    else:
        l_header.append(top_dt.strftime("%Y%m%d_%H%M%S"))
    return "".join(l_header)


def pc_all_args(dirname, term_str, diff_str, dur_str):
    w_top_dt, w_end_dt = _config.getterm("dag", "whole_term")
    term = config.str2dur(term_str)
    diff = config.str2dur(diff_str)
    dur = config.str2dur(dur_str)
    threshold = _config.getfloat("dag", "threshold")
    l_args = []
    for area in _config.gettuple("dag", "area"):
        top_dt = w_top_dt
        while top_dt < w_end_dt:
            end_dt = top_dt + term
            l_args.append((dirname, top_dt, end_dt, dur, area, threshold))
            top_dt = top_dt + diff
    return l_args


def pc_mthread(l_args, pal=1):

    _logger.debug("task start ({0} jobs)".format(len(l_args)))

    l_thread = [threading.Thread(name = thread_name(*args),
        target = pc_log, args = args) for args in l_args]

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


def test_pc_log():
    #import logging
    #logger = logging.getLogger("pcalg")
    #ch = logging.StreamHandler()
    #ch.setLevel(logging.DEBUG)
    #logger.setLevel(logging.DEBUG)
    #logger.addHandler(ch)

    dirname = _config.get("dag", "default_output_dir")
    top_dt = datetime.datetime(2112, 9, 1, 0, 0, 0)
    end_dt = datetime.datetime(2112, 9, 2, 0, 0, 0)
    dur = datetime.timedelta(seconds = 1)
    area = "area1"
    threshold = 0.01
    ret = pc_log(dirname, top_dt, end_dt, dur, area, threshold)
    print(" > {0}".format(ret.filename))


if __name__ == "__main__":
    logging.config.fileConfig("logging.conf")
    default_term = _config.get("dag", "default_term")
    default_dur = _config.get("dag", "default_dur")
    default_diff = _config.get("dag", "default_diff")
    output_dir = _config.get("dag", "default_output_dir")

    usage = "usage: %s [options]" % sys.argv[0]
    op = optparse.OptionParser(usage)
    op.add_option("-t", "--term", action="store", dest="term", type="string",
            default=default_term, help="date term of each dataset")
    op.add_option("-d", "--duration", action="store", dest="dur", type="string",
            default=default_dur, help="bin size of event occurrance")
    op.add_option("--diff", action="store", dest="diff", type="string",
            default=default_diff, help="")
    op.add_option("-p", "--parallel", action="store", dest="pal", type="int",
            default=1, help="multithreading")
    op.add_option("-r", action="store_true", dest="rflag",
            default=False, help="using pcalg library in R")
    op.add_option("--dir", action="store", dest="dirname", type="string",
            default=output_dir, help="output directory name")
    op.add_option("--test", action="store_true", dest="testflag",
            default=False, help="test mode")
    (options, args) = op.parse_args()
    if options.testflag: test_pc_log(); exit()

    fslib.mkdir(options.dirname)
        
    l_args = pc_all_args(options.dirname,
            options.term, options.diff, options.dur)
    pc_mthread(l_args, options.pal)


