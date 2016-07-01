#!/usr/bin/env python
# coding: utf-8

import os
import cPickle as pickle


class LTManager(object):
    """
    A log template manager. This class define log templates from messages.
    In addition, this class update log template table on memory and DB.

    Log template generation process can be classified to following 2 types.
        Grouping : Classify messages into groups, and generate template from
                the common parts of group members.
        Templating : Estimate log template from messages by classifying words,
                and make groups that have common template.

    Attributes:
        #TODO
    
    """

    # adding lt to db (ltgen do not add)

    def __init__(self, conf, db, table, reset_db, lt_alg, ltg_alg, post_alg):
        self.reset_db = reset_db
        self.conf = conf
        self.sym = conf.get("log_template", "variable_symbol")
        self.filename = conf.get("log_template", "indata_filename")

        self.db = db # log_db.LogDB
        self.table = table # LTTable
        #self.ltgroup = self._init_ltgroup(ltg_alg) # LTGroup
        self.ltgen = None
        self.ltspl = None
        self.ltgroup = None

    def _set_ltgen(self, ltgen):
        self.ltgen = ltgen

    def _set_ltgroup(self, ltgroup):
        self.ltgroup = ltgroup
        if not self.reset_db:
            self.ltgroup.restore_ltg(self.db, self.table)

    def process_line(self, l_w, l_s):
        ltline, added_flag = self.ltgen.process_line(l_w, l_s)
        return ltline
        # return ltline object
        # if ltline is None, it means lt not found in pre-defined table
        #raise NotImplementedError

    def add_lt(self, l_w, l_s, cnt = 1):
        # add new lt to db and table
        ltid = self.table.next_ltid()
        ltline = LogTemplate(ltid, None, l_w, l_s, cnt, self.sym)
        ltgid = self.ltgroup.add(ltline)
        ltline.ltgid = ltgid
        self.table.add_lt(ltline)
        self.db.add_lt(ltline)
        return ltline

    def replace_lt(self, ltid, l_w, l_s = None, cnt = None):
        self.table[ltid].replace(l_w, l_s, cnt)
        self.db.update_lt(ltid, l_w, l_s, cnt)
    
    def replace_and_count_lt(self, ltid, l_w, l_s = None):
        cnt = self.table[ltid].count()
        self.table[ltid].replace(l_w, l_s, None)
        self.db.update_lt(ltid, l_w, l_s, cnt)

    def count_lt(self, ltid):
        cnt = self.table[ltid].count()
        self.db.update_lt(ltid, None, None, cnt)

    def remove_lt(self, ltid):
        self.table.remove_lt(ltid)
        self.db.remove_lt(ltid)

    def remake_ltg(self):
        self.db.reset_ltg()
        self.ltgroup.init_dict()
        temp_table = self.table
        self.ltgroup.table = LTTable(self.sym)

        for ltline in temp_table:
            ltgid = self.ltgroup.add(ltline)
            ltline.ltgid = ltgid
            self.ltgroup.table.add_lt(ltline)
            self.db.add_ltg(ltline.ltid, ltgid)
        assert self.ltgroup.table.ltdict == temp_table.ltdict

    def load(self):
        with open(self.filename, 'r') as f:
            obj = pickle.load(f)
        ltgen_data, ltgroup_data = obj
        self.ltgen.load(ltgen_data)
        self.ltgroup.load(ltgroup_data)

    def dump(self):
        ltgen_data = self.ltgen.dumpobj()
        #ltspl_data = None
        ltgroup_data = self.ltgroup.dumpobj()
        obj = (ltgen_data, ltgroup_data)
        with open(self.filename, 'w') as f:
            pickle.dump(obj, f)

    #def _load_pickle(self):
    #    with open(self.filename, 'r') as f:
    #        return pickle.load(f)

    #def _dump_pickle(self, obj):
    #    with open(self.filename, 'w') as f:
    #        pickle.dump(obj, f)


class LTTable():

    def __init__(self, sym):
        self.ltdict = {}
        self.sym = sym

    def __iter__(self):
        return self._generator()

    def _generator(self):
        for ltid in self.ltdict.keys():
            yield self.ltdict[ltid]
    
    def __len__(self):
        return len(self.ltdict)

    def __getitem__(self, key):
        assert isinstance(key, int)
        if not self.ltdict.has_key(key):
            raise IndexError("list index out of range")
        return self.ltdict[key]
    
    def next_ltid(self):
        cnt = 0
        while self.ltdict.has_key(cnt):
            cnt += 1 
        else:
            return cnt
    
    def restore_lt(self, ltid, ltgid, ltw, lts, count):
        assert not self.ltdict.has_key(ltid)
        self.ltdict[ltid] = LogTemplate(ltid, ltgid, ltw, lts, count, self.sym)

    def add_lt(self, ltline):
        assert not self.ltdict.has_key(ltline.ltid)
        self.ltdict[ltline.ltid] = ltline

    def remove_lt(self, ltid):
        self.ltdict.pop(ltid)


class LogTemplate():

    def __init__(self, ltid, ltgid, ltw, lts, count, sym):
        self.ltid = ltid
        self.ltgid = ltgid
        self.ltw = ltw
        self.lts = lts
        self.cnt = count
        self.sym = sym

    def __str__(self):
        return self.restore_message(self.ltw)

    def var(self, l_w):
        return [w_org for w_org, w_lt in zip(l_w, self.ltw)
                if w_lt == self.sym]

    def var_location(self):
        return [i for i, w_lt in enumerate(self.ltw) if w_lt == self.sym]

    def restore_message(self, l_w):
        if self.lts is None:
            return "".join(l_w)
        else:
            return "".join([s + w for w, s in zip(l_w + [""], self.lts)])
    
    def count(self):
        self.cnt += 1
        return self.cnt

    def replace(self, l_w, l_s = None, count = None):
        self.ltw = l_w
        if l_s is not None:
            self.lts = l_s
        if count is not None:
            self.cnt = count


class LTGroup(object):

    # usually used as super class of other ltgroup
    # If used directly, this class will work as a dummy
    # (ltgid is always same as ltid)

    def __init__(self):
        self.init_dict()

    def init_dict(self):
        self.d_group = {} # key : groupid, val : [ltline, ...]
        self.d_rgroup = {} # key : ltid, val : groupid

    def _next_groupid(self):
        cnt = 0
        while self.d_group.has_key(cnt):
            cnt += 1
        else:
            return cnt

    def add(self, ltline):
        gid = ltline.ltid
        self.add_ltid(gid, ltline)
        return gid

    def add_ltid(self, gid, ltline):
        self.d_group.setdefault(gid, []).append(ltline)
        self.d_rgroup[ltline.ltid] = gid

    def restore_ltg(self, db, table):
        for ltid, ltgid in db.iter_ltg_def():
            self.d_group.setdefault(ltgid, []).append(table[ltid])
            self.d_rgroup[ltid] = ltgid

    def load(self, loadobj):
        pass

    def dumpobj(self, dumpobj):
        return None


class LTSearchTree():
    # Search tree for un-incremental lt generation algorithms

    def __init__(self, sym):
        self.sym = sym
        self.root = self._new_node()

    def __str__(self):
        l_buf = []

        def print_children(point, depth):
            word = point.word
            if word is None: word = "**"
            buf = "-" * depth + " {0}".format(point.word)
            if point.end is not None:
                buf += "  <-- ltid {0}".format(point.end)
            l_buf.append(buf)
            for word in point.windex.keys():
                print_children(point.windex[word], depth + 1)
            if point.wild is not None:
                print_children(point.wild, depth + 1)

        point = self.root
        l_buf.append("<head of log template search tree>")
        for word in point.windex.keys():
            print_children(point.windex[word], 1)
        if point.wild is not None:
            print_children(point.wild, 1)
        return "\n".join(l_buf)

    @staticmethod
    def _new_node(parent = None, word = None):
        return LTSearchTreeNode(parent, word)

    def add(self, ltid, ltwords):
        point = self.root
        for w in ltwords:
            if w == self.sym:
                if point.wild is None:
                    point.wild = self._new_node(point, w)
                point = point.wild
            else:
                if not point.windex.has_key(w):
                    point.windex[w] = self._new_node(point, w)
                point = point.windex[w]
        else:
            point.set_ltid(ltid)

    def _trace(self, ltwords):
        point = self.root
        temp_ltwords = ltwords[:]
        check_points = []
            # points with 2 candidates to go, for example "word" and "**"
            # [(node, len(left_words)), ...]
            # use as stack
        while True:
            w = temp_ltwords.pop(0)
            if w == self.sym:
                if point.wild is None:
                    return None
                else:
                    point = point.wild
            elif point.windex.has_key(w):
                if point.wild is not None:
                    check_points.append((point, len(temp_ltwords)))
                point = point.windex[w]
            elif point.wild is not None:
                point = point.wild
            else:
                if len(check_points) == 0:
                    return None
                else:
                    p, left_wlen = check_points.pop(-1)
                    temp_ltwords = ltwords[-left_wlen+1:]
                        # +1 : for one **(wild) node
                    point = p.wild

            if len(temp_ltwords) == 0:
                if point.end is None:
                    if len(check_points) == 0:
                        return None
                    else:
                        p, left_wlen = check_points.pop(-1)
                        temp_ltwords = ltwords[-left_wlen+1:]
                        point = p.wild
                else:
                    return point

    def remove(self, ltid, ltwords):
        node = self._trace(ltwords)
        if node is None:
            _logger.warning(
                    "LTSearchTree : Failed to remove ltid {0}".format(ltid))
        point.remove_ltid(ltid)
        while point.unnecessary():
            w = point.word
            point = point.parent
            if w is None:
                point.wild = None
            else:
                point.wdict.pop(w)
        else:
            if self.root is None:
                self.root = self._new_node()

    def search(self, ltwords):
        node = self._trace(ltwords)
        if node is None:
            return None
        else:
            return node.get_ltid()


class LTSearchTreeNode():

    def __init__(self, parent, word):
        self.windex = {}
        self.wild = None
        self.end = None
        self.parent = parent # for reverse search to remove
        self.word = word

    def child(self, word = None):
        if word is None:
            # wildcard
            return self.wild
        elif self.windex.has_key(word):
            return self.windex[word]
        else:
            return None

    def current_point(self):
        buf = []
        point = self
        while point.parent is not None:
            buf = [point.word] + buf
            point = point.parent
        print " ".join(buf)

    def set_ltid(self, ltid):
        self.end = ltid

    def remove_ltid(self, ltid):
        assert self.end == ltid
        self.end = None

    def get_ltid(self):
        return self.end

    def unnecessary(self):
        return (len(self.windex) == 0) and \
                (self.wild is None) and \
                (self.end is None)


def init_ltmanager(conf, db, table, reset_db):
    """Initializing ltmanager by loading argument parameters."""
    lt_alg = conf.get("log_template", "lt_alg")
    ltg_alg = conf.get("log_template", "ltgroup_alg")
    post_alg = "" #TODO
    # ltg_alg : used in lt_common.LTManager._init_ltgroup
    ltm = LTManager(conf, db, table, reset_db,
            lt_alg, ltg_alg, post_alg)

    if lt_alg == "shiso":
        import lt_shiso
        ltgen = lt_shiso.LTGen(ltm, table,
                threshold = conf.getfloat(
                    "log_template_shiso", "ltgen_threshold"),
                max_child = conf.getint(
                    "log_template_shiso", "ltgen_max_child")
                )
        #TODO ltgen should not use ltm...
    #elif lt_alg == "va":
    #    import lt_va
    #    ltm = lt_va.LTManager(conf, self.db, self.table,
    #            self._reset_db, ltg_alg)
    #elif lt_alg == "import":
    #    import lt_import
    #    ltm = lt_import.LTManager(conf, self.db, self.table,
    #            self._reset_db, ltg_alg)
    else:
        raise ValueError("lt_alg({0}) invalid".format(lt_alg))
    ltm._set_ltgen(ltgen)

    if ltg_alg == "shiso":
        import lt_shiso
        ltgroup = lt_shiso.LTGroupSHISO(table,
                ngram_length = conf.getint(
                    "log_template_shiso", "ltgroup_ngram_length"),
                th_lookup = conf.getfloat(
                    "log_template_shiso", "ltgroup_th_lookup"),
                th_distance = conf.getfloat(
                    "log_template_shiso", "ltgroup_th_distance"),
                mem_ngram = conf.getboolean(
                    "log_template_shiso", "ltgroup_mem_ngram")
                )
    elif ltg_alg == "none":
        ltgroup = LTGroup()
    else:
        raise ValueError("ltgroup_alg({0}) invalid".format(ltg_alg))
    ltm._set_ltgroup(ltgroup)

    if os.path.exists(ltm.filename) and not reset_db:
        ltm.load()

    return ltm


def merge_lt(m1, m2, sym):
    #return common area of log message (to be log template)
    ret = []
    for w1, w2 in zip(m1, m2):
        if w1 == w2:
            ret.append(w1)
        else:
            ret.append(sym)
    return ret


