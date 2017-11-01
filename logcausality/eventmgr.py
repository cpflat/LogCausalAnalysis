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


def get_dict_eventset(conf):
    d_set = defaultdict(set)
    dirname = conf.get("dag", "event_dir")
    for fp in common.rep_dir(dirname):
        fn = fp.split("/")[-1]
        temp_edict, temp_evmap = log2event.load_edict(fp)
        for k in temp_edict.keys():
            evdef = temp_evmap.info(k)
            d_set[fn].add(evdef)
    return d_set


def diff_event(conf1, conf2):

    d_set1 = get_dict_eventset(conf1)
    d_set2 = get_dict_eventset(conf2)
    d_diff = defaultdict(int)

    for k in d_set1.keys():
        if not d_set2.has_key(k):
            raise KeyError("{0} not found in event set 2".format(k))
        s1 = d_set1[k]
        s2 = d_set2[k]
        for ev in (s1 - s2):
            d_diff[ev] += 1

    return d_diff


def filter_info(conf_org, conf):
    d_org = get_dict_eventset(conf_org)
    d_set = get_dict_eventset(conf)

    d_rm = defaultdict(int)
    d_rp = defaultdict(int)
    type_rp = log2event.EventDefinitionMap.type_periodic_remainder
    for k in d_org.keys():
        if not d_set.has_key(k):
            raise KeyError("{0} not found in event set 2".format(k))
        s1 = d_org[k]
        s2 = d_set[k]
        s_rp = set()
        for ev in s2:
            if ev.type == type_rp:
                d_rp[ev] += 1
                s_rp.add((ev.gid, ev.host))
        for ev in (s1 - s2):
            if (ev.gid, ev.host) in s_rp:
                # replaced, not removed
                pass
            else:
                d_rm[ev] += 1

    return d_rm, d_rp


def filter_compare(conf_org, conf1, conf2):

    def removed(s_org, s_processed, s_replaced):
        s_rm = set()
        for ev in s_org:
            if ev in s_processed:
                # remaining
                pass
            elif ev in s_replaced:
                # replaced, not removed
                pass
            else:
                # removed
                s_rm.add(ev)
        return s_rm

    d_org = get_dict_eventset(conf_org)
    d_set1 = get_dict_eventset(conf1)
    d_set2 = get_dict_eventset(conf2)

    d_cnt_rm1_all = defaultdict(int)
    d_cnt_rm2_all = defaultdict(int)
    d_cnt_rmc = defaultdict(int)
    d_cnt_rm1 = defaultdict(int)
    d_cnt_rm2 = defaultdict(int)
    d_cnt_rp1 = defaultdict(int)
    d_cnt_rp2 = defaultdict(int)

    type_rp = log2event.EventDefinitionMap.type_periodic_remainder
    for k in d_org.keys():
        if not d_set1.has_key(k):
            raise KeyError("{0} not found in event set 1".format(k))
        if not d_set2.has_key(k):
            raise KeyError("{0} not found in event set 2".format(k))
        s_org = d_org[k]
        s1 = d_set1[k]
        s2 = d_set2[k]
        s_rp1 = set()
        s_rp2 = set()

        for ev in s1:
            if ev.type == type_rp:
                d_cnt_rp1[ev] += 1
                s_rp1.add((ev.gid, ev.host))
        for ev in s2:
            if ev.type == type_rp:
                d_cnt_rp2[ev] += 1
                s_rp2.add((ev.gid, ev.host))

        s1_rm = removed(s_org, s1, s_rp1)
        s2_rm = removed(s_org, s2, s_rp2)

        s_rm_common = s1_rm & s2_rm
        s_rm_onlyin1 = s1_rm - s2_rm
        s_rm_onlyin2 = s2_rm - s1_rm

        d_cnt_rm1_all[k] = len(s1_rm)
        d_cnt_rm2_all[k] = len(s2_rm)
        d_cnt_rmc[k] = len(s_rm_common)
        d_cnt_rm1[k] = len(s_rm_onlyin1)
        d_cnt_rm2[k] = len(s_rm_onlyin2)

    dset_sum = lambda d: sum([len(val) for val in d.values()])
    dcnt_sum = lambda d: sum(d.values())
    print("Original events: {0}".format(dset_sum(d_org)))
    print("Remaining events in Set 1: {0}".format(dset_sum(d_set1)))
    print("Remaining events in Set 2: {0}".format(dset_sum(d_set2)))
    print("Replaced in Set 1: {0}".format(dcnt_sum(d_cnt_rp1)))
    print("Replaced in Set 2: {0}".format(dcnt_sum(d_cnt_rp2)))
    print("Removed in Set 1: {0}".format(dcnt_sum(d_cnt_rm1_all)))
    print("Removed in Set 2: {0}".format(dcnt_sum(d_cnt_rm2_all)))
    print("Commonly removed: {0}".format(dcnt_sum(d_cnt_rmc)))
    print("Removed only in Set 1: {0}".format(dcnt_sum(d_cnt_rm1)))
    print("Removed only in Set 2: {0}".format(dcnt_sum(d_cnt_rm2)))


def show_diff_event(conf1, conf2):
    d_diff = diff_event(conf1, conf2)
    import log2event
    import log_db
    ld = log_db.LogData(conf1)
    import lt_label
    ll = lt_label.init_ltlabel(conf1)

#    cnt_rm_type = 0; cnt_rm_ev = 0
#    cnt_rp_type = 0; cnt_tp_ev = 0
#    type_rm = log2event.EventDefinitionMap.type_normal
#    type_rp = log2event.EventDefinitionMap.type_periodic_remainder
#    for evdef, cnt in d_diff.iteritems():
#        if evdef = 

    for evdef, cnt in sorted(d_diff.iteritems(), key = lambda x: x[1],
            reverse = True):
        print cnt, evdef

    print
    for evdef, cnt in sorted(d_diff.iteritems(), key = lambda x: x[1],
            reverse = True):
        print cnt, log2event.EventDefinitionMap.get_str(evdef)
        print "  " + ld.show_ltgroup(evdef.gid)
        print


#def diff_event_stat(conf1, conf2):
#    d_diff = diff_event(conf1, conf2)
#
#    s_ev1 = set()
#    dirname = conf1.get("dag", "event_dir")
#    for fp in common.rep_dir(dirname):
#        fn = fp.split("/")[-1]
#        edict1, evmap1 = log2event.load_edict(fp)
#        s_ev1 = s_ev1 | set([evmap1.info(eid) for eid in edict1.keys()])
#
#    s_ev2 = set()
#    dirname = conf2.get("dag", "event_dir")
#    for fp in common.rep_dir(dirname):
#        fn = fp.split("/")[-1]
#        edict2, evmap2 = log2event.load_edict(fp)
#        s_ev2 = s_ev2 | set([evmap2.info(eid) for eid in edict2.keys()])
#
#    s_diff1 = s_ev1 - s_ev2
#    s_diff2 = s_ev2 - s_ev1
#
#    rep_type = log2event.EventDefinitionMap.type_periodic_remainder
#    s_diff1_rep = [evdef for evdef in s_diff1 if evdef.type == rep_type]
#    s_diff2_rep = [evdef for evdef in s_diff2 if evdef.type == rep_type]
#
#    print("Set 1")
#    org = len(s_ev1)
#    print("Events: {0}".format(org))
#    num = len(s_diff1)
#    print("Not in Set1: {0} ({1})".format(num, 1.0 * num / org))
#    num = len(s_diff1) - len(s_diff1_rep)
#    print("Removed events: {0} ({1})".format(num, 1.0 * num / org))
#    num = len(s_diff1_rep)
#    print("Replaced events: {0} ({1})".format(num, 1.0 * num / org))
#    print("")
#    print("Set 2")
#    org = len(s_ev2)
#    print("Events: {0}".format(org))
#    num = len(s_diff2)
#    print("Edited events: {0} ({1})".format(num, 1.0 * num / org))
#    num = len(s_diff2) - len(s_diff2_rep)
#    print("Removed events: {0} ({1})".format(num, 1.0 * num / org))
#    num = len(s_diff2_rep)
#    print("Replaced events: {0} ({1})".format(num, 1.0 * num / org))


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
    #elif mode == "diff-event-stat":
    #    if len(args) == 0:
    #        sys.exit(usage)
    #    conf2 = config.open_config(args[0])
    #    diff_event_stat(conf, conf2)
    elif mode == "diff-filter":
        if len(args) < 3:
            sys.exit(("give me 3 config files: 1 for original events, "
                      "2 for filtered ones to compare"))
        conf_org = config.open_config(args[0])
        conf1 = config.open_config(args[1])
        conf2 = config.open_config(args[2])
        filter_compare(conf_org, conf1, conf2)
    elif mode == "diff-label":
        if len(args) == 0:
            sys.exit(usage)
        conf2 = config.open_config(args[0])
        show_diff_event_label(conf, conf2)


