#!/usr/bin/env python
# coding: utf-8

import sys
import os
from collections import defaultdict

import common
import config
import log2event


def event_label(conf):
    import log_db
    ld = log_db.LogData(conf)
    import lt_label
    ll = lt_label.init_ltlabel(conf)

    d_group = defaultdict(int)
    dirname = conf.get("dag", "event_dir")
    for fp in common.rep_dir(dirname):
        fn = fp.split("/")[-1]
        edict, evmap = log2event.load_edict(fp)
        for evdef in [evmap.info(k) for k in edict.keys()]:
            gid = evdef.gid
            l_lt = ld.ltg_members(gid)
            group = ll.get_ltg_group(gid, l_lt)
            d_group[group] += 1
    return d_group


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

    return d_diff


def show_diff_event(conf1, conf2):
    d_diff = diff_event(conf1, conf2)
    import log2event
    import log_db
    ld = log_db.LogData(conf1)
    import lt_label
    ll = lt_label.init_ltlabel(conf1)

    for evdef, cnt in sorted(d_diff.iteritems(), key = lambda x: x[1],
            reverse = True):
        print cnt, evdef

    print
    for evdef, cnt in sorted(d_diff.iteritems(), key = lambda x: x[1],
            reverse = True):
        print cnt, log2event.EventDefinitionMap.get_str(evdef)
        print "  " + ld.show_ltgroup(evdef.gid)
        print


def show_diff_event_label(conf1, conf2):
    d_diff = diff_event(conf1, conf2)
    import log_db
    ld = log_db.LogData(conf1)
    import lt_label
    ll = lt_label.init_ltlabel(conf1)

    d_group = defaultdict(int)
    for evdef in d_diff.keys():
        gid = evdef.gid
        l_lt = ld.ltg_members(gid)
        group = ll.get_ltg_group(gid, l_lt)
        d_group[group] += 1

    d_group_all = event_label(conf1)

    if len(d_group) == 0:
        print "return empty, is the config order right?"
    for group, cnt in d_group.items():
        cnt_all = d_group_all[group]
        print group, cnt, "/", cnt_all


def diff_event_edge(conf1, conf2):
    d_set1 = defaultdict(set)
    d_set2 = defaultdict(set)
    d_diff = {}

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
        d_diff[k] = (s1 - s2)

    return d_diff


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
        show_diff_event(conf, conf2)
    elif mode == "diff-label":
        if len(args) == 0:
            sys.exit(usage)
        conf2 = config.open_config(args[0])
        show_diff_event_label(conf, conf2)


