#!/usr/bin/env python
# coding: utf-8

import sys
import optparse

import config
import log_db
import lt_common

MAX_ARG_VARIETY = 10


def show_all(ldb):
    ldb.lt.show()


def show_lt(ldb):
    ldb.lt.show_all_lt()


def show_ltg(ldb, gid):
    if gid is None:
        ldb.lt.show_all_group()
    else:
        ldb.lt.show_group(gid)


def show_sort(ldb):
    buf = []
    for ltline in ldb.lt.table:
        buf.append(str(ltline))
    buf.sort()
    print "\n".join(buf)


def breakdown_ltid(ldb, ltid):
    d_args = {}
    for line in ldb.generate(ltid = ltid):
        for vid, arg in enumerate(line.args):
            d_var = d_args.setdefault(vid, {})
            d_var[arg] = d_var.get(arg, 0) + 1

    buf = []
    buf.append("LTID {0}> {1}".format(ltid, str(ldb.lt.table[ltid])))
    buf.append("")
    for vid, loc in enumerate(ldb.lt.table[ltid].variable_location()):
        buf.append("Variable {0} (word location : {1})".format(vid, loc))
        items = sorted(d_args[vid].items(), key=lambda x: x[1], reverse=True)
        var_variety = len(d_args[vid].keys())
        if var_variety > MAX_ARG_VARIETY:
            for item in items[:10]:
                buf.append("{0} : {1}".format(item[0], item[1]))
            buf.append("... {0} kinds of variable".format(var_variety))
        else:
            for item in items:
                buf.append("{0} : {1}".format(item[0], item[1]))
        buf.append("")
    return "\n".join(buf)


def merge_ltid(ldb, ltid1, ltid2, sym):
    print("merge following log templates...")
    print("ltid {0} : {1}".format(ltid1, str(ldb.lt.table[ltid1])))
    print("ltid {0} : {1}".format(ltid2, str(ldb.lt.table[ltid2])))
    print

    ltw1 = ldb.lt.table[ltid1].words
    cnt1 = ldb.lt.table[ltid1].cnt
    l_s = ldb.lt.table[ltid1].style
    ltw2 = ldb.lt.table[ltid2].words
    cnt2 = ldb.lt.table[ltid2].cnt
    if not len(ltw1) == len(ltw2):
        sys.exit("log template length is different, failed")
    new_ltw = lt_common.merge_lt(ltw1, ltw2, sym)

    ldb.lt.table.replace_lt(ltid1, new_ltw, l_s, cnt1 + cnt2)
    ldb.lt.table.remove_lt(ltid2)
    ldb.update(("ltid", ), (ltid2, ), ("ltid", ), (ltid1, ))

    ldb.lt.dump()
    ldb.commit()

    print("> new log template : ltid {0}".format(ltid1))
    print("ltid {0} : {1}".format(ltid1, str(ldb.lt.table[ltid1])))


def separate_ltid(ldb, ltid, vid, value):

    def remake_lt(ldb, ltid):
        new_lt = None
        cnt = 0
        for l_w in ldb.generate_wordlist(ltid = ltid):
            if new_lt is not None:
                new_lt = lt_common.merge_lt(new_lt, l_w)
            else:
                new_lt = l_w
            cnt += 1
        return new_lt, cnt

    print("separate following log template...")
    print("ltid {0} : {1}".format(ltid, str(ldb.lt.table[ltid])))
    print("new log template if variable {0} is {1}".format(vid, value))
    print

    l_lid = []
    for lid, lm in ldb.generate_with_id(ltid = ltid):
        if lm.args[vid] == value:
            l_lid.append(lid)
    new_ltid = ldb.lt.table.next_ltid()    
    for lid in l_lid:
        ldb.update_lid(lid, ("ltid", ), (new_ltid, ))

    l_s = ldb.lt.table[ltid].style
    ltw1, cnt1 = remake_lt(ldb, ltid)
    ldb.lt.table.replace_lt(ltid, ltw1, l_s, cnt1)

    ltw2, cnt2 = remake_lt(ldb, new_ltid)
    ret = ldb.lt.table.add_lt(ltw2, l_s, cnt2)
    assert ret == new_ltid

    ldb.lt.dump()
    ldb.commit()

    print("> new log templates : ltid {0}, ltid {1}".format(ltid, new_ltid))
    print("ltid {0} : {1}".format(ltid, str(ldb.lt.table[ltid])))
    print("ltid {0} : {1}".format(new_ltid, str(ldb.lt.table[new_ltid])))


if __name__ == "__main__":
    usage = "usage: %s [options] mode" % sys.argv[0]
    op = optparse.OptionParser(usage)
    op.add_option("-c", "--config", action="store",
            dest="conf", type="string", default=config.DEFAULT_CONFIG_NAME,
            help="configuration file")
    (options, args) = op.parse_args()
    if len(args) == 0:
        sys.exit(usage)
    mode = args[0]
    conf = config.open_config(options.conf)

    ldb = log_db.ldb_manager(conf)
    ldb.open_lt()
    if mode == "show":
        show_all(ldb)
    elif mode == "show-lt":
        show_lt(ldb)
    elif mode == "show-group":
        if len(args) <= 1:
            show_ltg(ldb, None)
        else:
            show_ltg(ldb, int(args[1]))
    elif mode == "show-sort":
        show_sort(ldb)
    elif mode == "breakdown":
        if len(args) <= 1:
            sys.exit("give me ltid, following \"{0}\"".format(mode))
        ltid = int(args[1])
        print breakdown_ltid(ldb, ltid)
    elif mode == "merge":
        if len(args) <= 2:
            sys.exit("give me 2 ltid, following \"{0}\"".format(mode))
        ltid1 = int(args[1])
        ltid2 = int(args[2])
        merge_ltid(ldb, ltid1, ltid2)
    elif mode == "separate":
        if len(args) <= 3:
            sys.exit("give me ltid, variable id and value, "
                    "following \"{0}\"".format(mode))
        ltid = int(args[1])
        vid = int(args[2])
        val = args[3]
        separate_ltid(ldb, ltid, vid, val)
        


