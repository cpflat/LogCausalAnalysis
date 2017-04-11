#!/usr/bin/env python
# coding: utf-8

import sys
import os

import common
import config
import pcresult


def diff_event_type(conf):
    cnt = 0
    for r in pcresult.results(conf):
        dataset_flag = False
        for edge in r.graph.edges():
            types = [r.evmap.info(eid).type for eid in edge]
            if types[0] == types[1]:
                pass
            else:
                if not dataset_flag:
                    print("# {0}".format(r.filename.partition("/")[-1]))
                    dataset_flag = True
                if (edge[1], edge[0]) in r.graph.edges():
                    directed = False
                else:
                    directed = True
                r._print_edge(edge, directed)
                cnt += 1
    print
    print("Edges among different event type: {0}".format(cnt))


def related_filtered(conf):
    cnt = 0
    for r in pcresult.results(conf):
        dataset_flag = False
        for edge in r.graph.edges():
            types = [r.evmap.info(eid).type for eid in edge]
            if types[0] == 0 and types[1] == 0:
                pass
            else:
                if not dataset_flag:
                    print("# {0}".format(r.filename.partition("/")[-1]))
                    dataset_flag = True
                if (edge[1], edge[0]) in r.graph.edges():
                    directed = False
                else:
                    directed = True
                r._print_edge(edge, directed)
                cnt += 1
    print
    print("Edges related to filtered event: {0}".format(cnt))


def list_all_gid(conf):
    import log_db
    import lt_label
    ld = log_db.LogData(conf)
    ll = lt_label.init_ltlabel(conf)

    s_gid = set()
    for r in pcresult.results(conf):
        for edge in r.graph.edges():
            for gid in [r.evmap.info(eid).gid for eid in edge]:
                s_gid.add(gid)

    for gid in s_gid:
        l_ltline = ld.ltg_members(gid)
        print("gid {0} : {1} in {2}".format(gid,
            ll.get_ltg_label(gid, l_ltline)), ll.get_ltg_group(gid, l_ltline))


def search_gid(conf, gid):
    for r in pcresult.results(conf):
        for edge in r.graph.edges():
            for temp_gid in [r.evmap.info(eid).gid for eid in edge]:
                if temp_gid == gid:
                    print r.filename


def diff_event_edge(conf1, conf2):



if __name__ == "__main__":
    usage = """
usage: {0} [options] args...
    """.format(sys.argv[0]).strip()

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
    if mode == "diff-event-type":
        diff_event_type(conf)
    elif mode == "related-filtered":
        related_filtered(conf)
    elif mode == "list-all-gid":
        list_all_gid(conf)
    elif mode == "search-gid":
        if len(args) == 0:
            sys.exit("give me gid")
        gid = args[0]
        search_gid(conf, gid)
    else:
        raise NotImplementedError


