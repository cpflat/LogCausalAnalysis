#!/usr/bin/env python
# coding: utf-8

import sys
import datetime
import random

import config
import log_db
import logparser
import host_alias

TIMEFMT = "%Y-%m-%d %H:%M:%S"


def message(eid, t):
    return "{0} host{1} random_event".format(t.strftime(TIMEFMT), eid)


def generate_normal():
    pass


def poisson_process(top_dt, end_dt, lambd):
    def next_dt(dt):
        return dt + datetime.timedelta(seconds = 1) * int(
                24 * 60 * 60 * random.expovariate(lambd))

    temp_dt = top_dt
    temp_dt = next_dt(temp_dt)
    while temp_dt < end_dt:
        yield temp_dt
        temp_dt = next_dt(temp_dt)


def generate_poisson(top_dt, end_dt, lambd, event):
    l_event = []
    for eid in range(event):
        for t in poisson_process(top_dt, end_dt, lambd):
            l_event.append((eid, t))

    l_event.sort(key = lambda x: x[1])
    return l_event

def add_db(conf, l_event, verbose, reset_db):
    ld = log_db.LogData(conf, edit = True, reset_db = reset_db)
    ld.init_ltmanager()
    lp = logparser.LogParser(conf)
    ha = host_alias.HostAlias(conf)
    for eid, t in l_event:
        msg = message(eid, t)
        log_db.process_line(msg, ld, lp, ha)
        if verbose: print msg


if __name__ == "__main__":
    usage = "usage: {0} [options] top_dt end_dt".format(sys.argv[0])
    import optparse
    op = optparse.OptionParser(usage)
    op.add_option("-c", "--config", action="store",
            dest="conf", type="string", default=config.DEFAULT_CONFIG_NAME,
            help="configuration file path")
    op.add_option("-l", "--lambda", action="store",
            dest="lambd", type="int", default=10,
            help="the average of appearance per 1 day")
    op.add_option("-d", "--dist", action="store",
            dest="dist", type="string", default="poisson",
            help="distribution of random log")
    op.add_option("-e", "--event", action="store",
            dest="event", type="int", default=10,
            help="number of events")
    op.add_option("-r", action="store_true", dest="reset",
            default=False, help="reset_db")
    op.add_option("-v", "--verbose", action="store_true",
            dest="verbose", default=False,
            help="output messages to stdio")
    (options, args) = op.parse_args()
    conf = config.open_config(options.conf)

    #assert len(args) == 4
    top_dt = datetime.datetime.strptime(" ".join(args[0:2]), TIMEFMT)
    end_dt = datetime.datetime.strptime(" ".join(args[2:4]), TIMEFMT)

    if options.dist == "poisson":
        ev = generate_poisson(top_dt, end_dt, options.lambd, options.event)
        add_db(conf, ev, options.verbose, options.reset)

