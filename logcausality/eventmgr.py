#!/usr/bin/env python
# coding: utf-8

import sys
import os
from collections import defaultdict

import common
import config
import log2event


def diff_event(conf1, conf2):
    d_set1 = defaultdict(set)
    d_set2 = defaultdict(set)
    d_diff = defaultdict(int)

    dirname = conf1.get("dag", "event_dir")
    for fp in common.rep_dir(dirname):
        fn = fp.split("/")[-1]
        edict1, evmap1 = log2event.load_edict(fp)
        for evdef in [evmap1.info(k) for k in edict1.keys()]:
            d_set1[fn].add(evdef)

    dirname = conf2.get("dag", "event_dir")
    for fp in common.rep_dir(dirname):
        fn = fp.split("/")[-1]
        edict2, evmap2 = log2event.load_edict(fp)
        for evdef in [evmap2.info(k) for k in edict2.keys()]:
            d_set2[fn].add(evdef)

    for k in d_set1.keys():
        if not d_set2.has_key(k):
            raise KeyError("{0} not found in event set 2".format(k))
        s1 = d_set1[k]
        s2 = d_set2[k]
        for ev in (s1 - s2):
            d_diff[ev] += 1

    for evdef, cnt in sorted(d_diff.iteritems(), key = lambda x: x[1],
            reverse = True):
        print cnt, evdef

    import log_db
    ld = log_db.LogData(conf1)
    print
    for evdef, cnt in sorted(d_diff.iteritems(), key = lambda x: x[1],
            reverse = True):
        print cnt, evdef
        evmap1.info_repr(ld, evmap1.get_eid(evdef))
        print


if __name__ == "__main__":
    usage = "usage: {0} [options] mode".format(sys.argv[0])
    import optparse
    op = optparse.OptionParser(usage)
    op.add_option("-c", "--config", action="store",
            dest="conf", type="string", default=config.DEFAULT_CONFIG_NAME,
            help="configuration file path")
    (options, args) = op.parse_args()

    conf = config.open_config(options.conf)

    if len(args) == 0:
        sys.exit(usage)
    mode = args.pop(0)
    if mode == "diff-event":
        if len(args) == 0:
            sys.exit(usage)
        conf2 = config.open_config(args[0])
        diff_event(conf, conf2)


