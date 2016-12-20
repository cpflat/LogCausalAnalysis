#!/usr/bin/env python
# coding: utf-8

"""Notice: after you edit log templates,
it is NOT reccommended to add other log messages,
especially with new log templates.
On current implementation, lt_edit will destroy
the structures of log template manager."""

import sys
import optparse

import config
import log_db
import lt_common


def export(ld):
    for ltline in ld.table:
        print ltline


def show_all(ld):
    print ld.show_all_ltgroup()


def show_lt(ld):
    print ld.show_all_lt()


def show_ltg(ld, gid):
    if gid is None:
        print ld.show_all_ltgroup()
    else:
        print ld.show_ltgroup(gid)


def show_sort(ld):
    buf = []
    for ltline in ld.iter_lt():
        buf.append(str(ltline))
    buf.sort()
    print "\n".join(buf)


def breakdown_ltid(ld, ltid, limit):
    d_args = {}
    for line in ld.iter_lines(ltid = ltid):
        for vid, arg in enumerate(line.var()):
            d_var = d_args.setdefault(vid, {})
            d_var[arg] = d_var.get(arg, 0) + 1

    buf = []
    
    buf.append("LTID {0}> {1}".format(ltid, str(ld.lt(ltid))))
    buf.append(" ".join(ld.lt(ltid).ltw))
    buf.append("")
    for vid, loc in enumerate(ld.lt(ltid).var_location()):
        buf.append("Variable {0} (word location : {1})".format(vid, loc))
        items = sorted(d_args[vid].items(), key=lambda x: x[1], reverse=True)
        var_variety = len(d_args[vid].keys())
        if var_variety > limit:
            for item in items[:limit]:
                buf.append("{0} : {1}".format(item[0], item[1]))
            buf.append("... {0} kinds of variable".format(var_variety))
        else:
            for item in items:
                buf.append("{0} : {1}".format(item[0], item[1]))
        buf.append("")
    return "\n".join(buf)


def _str_lt(ltid):
    return "ltid {0} : {1}".format(ltid, str(ld.lt(ltid)))


#def _update_tpl(ld, old_ltw, new_ltw):
#    table = ld.ltm.ltgen._table
#    if table.exists(old_ltw):
#        tid = table.get_tid(old_ltw)
#        state = ld.ltm.ltgen.update_table(new_ltw, tid, False)
#    else:
#        raise ValueError("No existing tpl, failed")


def merge_ltid(ld, ltid1, ltid2, sym):
    ld.init_ltmanager()
    print("merge following log templates...")
    print _str_lt(ltid1)
    print _str_lt(ltid2)
    print

    ltw1 = ld.lt(ltid1).ltw
    cnt1 = ld.lt(ltid1).cnt
    l_s = ld.lt(ltid1).lts
    ltw2 = ld.lt(ltid2).ltw
    cnt2 = ld.lt(ltid2).cnt
    if not len(ltw1) == len(ltw2):
        sys.exit("log template length is different, failed")
    new_ltw = lt_common.merge_lt(ltw1, ltw2, sym)

    ld.ltm.replace_lt(ltid1, new_ltw, l_s, cnt1 + cnt2)
    ld.ltm.remove_lt(ltid2)
    ld.db.update_log({"ltid" : ltid2}, {"ltid" : ltid1})

    ld.commit_db()

    print("> new log template : ltid {0}".format(ltid1))
    print _str_lt(ltid1)


def separate_ltid(ld, ltid, vid, value, sym):

    def remake_lt(ld, ltid):
        # use iter_words rather than iter_lines
        # because iter_lines needs ltline registered in table
        # but ltline on new_ltid is now on construction in this function...
        new_lt = None
        cnt = 0
        for l_w in ld.db.iter_words(ltid = ltid):
            if new_lt is None:
                new_lt = l_w
            else:
                new_lt = lt_common.merge_lt(new_lt, l_w, sym)
            cnt += 1
        print "result : " + str(new_lt)
        print 
        return new_lt, cnt

    ld.init_ltmanager()
    print("separate following log template...")
    print _str_lt(ltid)
    print("new log template if variable {0} is {1}".format(vid, value))
    print

    l_lid = [lm.lid for lm in ld.iter_lines(ltid = ltid)
            if lm.var()[vid] == value]
    new_ltid = ld.lttable.next_ltid()    
    for lid in l_lid:
        ld.db.update_log({"lid" : lid}, {"ltid" : new_ltid})

    l_s = ld.lt(ltid).lts
    ltw1, cnt1 = remake_lt(ld, ltid)
    ld.ltm.replace_lt(ltid, ltw1, l_s, cnt1)

    ltw2, cnt2 = remake_lt(ld, new_ltid)
    ltline = ld.ltm.add_lt(ltw2, l_s, cnt2)
    assert ltline.ltid == new_ltid

    ld.commit_db()

    print("> new log templates : ltid {0}, ltid {1}".format(ltid, new_ltid))
    print _str_lt(ltid)
    print _str_lt(new_ltid)


def fix_ltid(ld, ltid, vid, sym):
    ld.init_ltmanager()
    print("make variable (with no variety) into description word...")
    print _str_lt(ltid)
    
    variety = set()
    for lm in ld.iter_lines(ltid = ltid):
        variety.add(lm.var()[vid])
    assert len(variety) > 0
    if len(variety) == 1:
        print("confirmed that the given variable can be fixed (no variety)")
        fixed_word = variety.pop()
        print(fixed_word)
    else:
        print("the given variable can NOT be fixed (seems not stable)")
        print(variety)
        return

    ltobj = ld.lt(ltid)
    vloc = ltobj.var_location()[vid]
    new_ltw = ltobj.ltw[:]; new_ltw[vloc] = fixed_word
    l_s = ltobj.lts
    cnt = ltobj.cnt

    ld.ltm.replace_lt(ltid, new_ltw, l_s, cnt)
    ld.commit_db()
    print("> new log templates : ltid {0}".format(ltid))
    print _str_lt(ltid)


def free_ltid(ld, ltid, wid, sym):
    ld.init_ltmanager()
    print("make description word into variable (with no variety)...")
    print _str_lt(ltid)
 
    ltobj = ld.lt(ltid)
    if ltobj.ltw[wid] == sym:
        print("wid {0} seems a variable, failed")
        return
    else:
        print("confirmed that wid {0} is description word ({1})".format(
            wid, ltobj.ltw[wid]))

    new_ltw = ltobj.ltw[:]; new_ltw[wid] = sym
    l_s = ltobj.lts
    cnt = ltobj.cnt

    ld.ltm.replace_lt(ltid, new_ltw, l_s, cnt)
    ld.commit_db()
    print("> new log templates : ltid {0}".format(ltid))
    print _str_lt(ltid)


def search_stable_variable(ld, th = 1):
    ld.init_ltmanager()

    for ltobj in ld.iter_lt():
        ltid = ltobj.ltid
        d_args = {}
        for lm in ld.iter_lines(ltid = ltid):
            for vid, arg in enumerate(lm.var()):
                d_var = d_args.setdefault(vid, {})
                d_var[arg] = d_var.get(arg, 0) + 1
        for vid, loc in enumerate(ld.lt(ltid).var_location()):
            var_variety = len(d_args[vid].keys())
            if var_variety <= th:
                print("{0} {1}".format(ltobj.ltid, ltobj))
                print("variable {0} (word location {1}): {2}".format(
                        vid, loc, d_args[vid]))


if __name__ == "__main__":
    
    usage = """
usage: {0} [options] args...
args:
  show : show all ltgroups
  show-lt : show all log template without grouping
  show-group LTGID : show log template group which has given LTGID
  breakdown LTID : show variables appeared in log instances of given LTID
  merge LTID1 LTID2 : merge log data with LTID1 and LTID2
  separate LTID VID VALUE : make new log template with log data
                            that have given variable VALUE in place VID
    """.format(sys.argv[0]).strip()
    
    op = optparse.OptionParser(usage)
    op.add_option("-c", "--config", action="store",
            dest="conf", type="string", default=config.DEFAULT_CONFIG_NAME,
            help="configuration file")
    op.add_option("-l", "--limit", action="store",
            dest="show_limit", type="int", default=10,
            help="Limitation rows to show source log data")
    (options, args) = op.parse_args()
    if len(args) == 0:
        sys.exit(usage)
    mode = args[0]
    conf = config.open_config(options.conf)

    ld = log_db.LogData(conf, edit = True)
    if mode == "export":
        export(ld)
    elif mode == "show":
        show_all(ld)
    elif mode == "show-lt":
        show_lt(ld)
    elif mode == "show-group":
        if len(args) <= 1:
            show_ltg(ld, None)
        else:
            show_ltg(ld, int(args[1]))
    elif mode == "show-sort":
        show_sort(ld)
    elif mode == "breakdown":
        if len(args) <= 1:
            sys.exit("give me ltid, following \"{0}\"".format(mode))
        ltid = int(args[1])
        print breakdown_ltid(ld, ltid, options.show_limit)
    elif mode == "merge":
        if len(args) <= 2:
            sys.exit("give me 2 ltid, following \"{0}\"".format(mode))
        ltid1 = int(args[1])
        ltid2 = int(args[2])
        sym = conf.get("log_template", "variable_symbol")
        merge_ltid(ld, ltid1, ltid2, sym)
    elif mode == "separate":
        if len(args) <= 3:
            sys.exit("give me ltid, variable id and value, "
                    "following \"{0}\"".format(mode))
        ltid = int(args[1])
        vid = int(args[2])
        val = args[3]
        sym = conf.get("log_template", "variable_symbol")
        separate_ltid(ld, ltid, vid, val, sym)
    elif mode == "fix":
        if len(args) <= 2:
            sys.exit("give me ltid and variable id to fix, "
                    "following \"{0}\"".format(mode))
        ltid = int(args[1])
        vid = int(args[2])
        sym = conf.get("log_template", "variable_symbol")
        fix_ltid(ld, ltid, vid, sym)
    elif mode == "free":
        if len(args) <= 2:
            sys.exit("give me ltid and word id to free, "
                    "following \"{0}\"".format(mode))
        ltid = int(args[1])
        wid = int(args[2])
        sym = conf.get("log_template", "variable_symbol")
        free_ltid(ld, ltid, wid, sym)
    elif mode == "search-stable":
        if len(args) >= 2:
            th = int(args[1])
        else:
            th = 1
        search_stable_variable(ld, th)

