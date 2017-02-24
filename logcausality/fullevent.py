#!/usr/bin/env python
# coding: utf-8

import sys
import cPickle as pickle
from collections import defaultdict

import config
import pc_log
import log2event


def connect(conf, fn):
    d_ret = defaultdict(list)
    l_args = pc_log.pc_all_args(conf)
    for args in l_args:
        top_dt = args[1]
        area = args[4]
        edict, evmap = logevent.get_edict(*args)
        for eid in evmap.iter_eid():
            evdef = evmap.info(eid)
            d_ret[evdef].append(edict[eid])

    for evdef, l_dt in d_ret.iteritems():
        print("{0} : {1} counts".format(evdef, len(l_dt)))

    with open(fn, "w")as f:
        obj = d_ret
        pickle.dump(obj, f)


def load(fn):
    """Load event dictionary from pickle dump file generated with connect().
    
    Args:
        fn (str) : filename of pickle dump file

    Returns:
        dict : key = log2event.EvDef, val = list[datetime.datetime]
    """

    with open(fn, "r") as f:
        d_ret = pickle.load(f)
    return d_ret


if __name__ == "__main__":
    usage = "usage: {0} [options] filename".format(sys.argv[0])
    import optparse
    op = optparse.OptionParser(usage)
    op.add_option("-c", "--config", action="store",
            dest="conf", type="string", default=config.DEFAULT_CONFIG_NAME,
            help="configuration file path")
    (options, args) = op.parse_args()

    conf = config.open_config(options.conf)

    if len(args) < 1:
        sys.exit("give me output filename")
    filename = args[0]
    connect(conf, filename)


