#!/usr/bin/env python
# coding: utf-8

import os
import cPickle as pickle


class LTManager(object):
    # adding lt to db (ltgen do not add)

    def __init__(self, conf, db, table, reset_db, ltg_alg):
        self.reset_db = reset_db
        self.conf = conf
        self.sym = conf.get("log_template", "variable_symbol")
        self.filename = conf.get("log_template", "indata_filename")

        self.db = db # log_db.LogDB
        self.table = table # LTTable
        self.ltgroup = self._init_ltgroup(ltg_alg) # LTGroup

        if os.path.exists(self.filename) and not reset_db:
            self.load()

    def _init_ltgroup(self, ltg_alg):
        if ltg_alg == "shiso":
            import lt_shiso
            ltgroup = lt_shiso.LTGroupSHISO(self.table,
                    ngram_length = self.conf.getint(
                        "log_template_shiso", "ltgroup_ngram_length"),
                    th_lookup = self.conf.getfloat(
                        "log_template_shiso", "ltgroup_th_lookup"),
                    th_distance = self.conf.getfloat(
                        "log_template_shiso", "ltgroup_th_distance"),
                    mem_ngram = self.conf.getboolean(
                        "log_template_shiso", "ltgroup_mem_ngram")
                    )
        elif ltg_alg == "none":
            ltgroup = LTGroup()
        else:
            raise ValueError("ltgroup_alg({0}) invalid".format(ltg_alg))
        if not self.reset_db:
            ltgroup.restore_ltg(self.db, self.table)
        return ltgroup

    def process_line(self, l_w, l_s):
        # return ltline object
        # if ltline is None, it means lt not found in pre-defined table
        raise NotImplementedError

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
        pass

    def dump(self):
        pass

    def _load_pickle(self):
        with open(self.filename, 'r') as f:
            return pickle.load(f)

    def _dump_pickle(self, obj):
        with open(self.filename, 'w') as f:
            pickle.dump(obj, f)


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
        for w in ltwords:
            if w == self.sym:
                if point.wild is None:
                    return None
                else:
                    point = point.wild
            elif point.windex.has_key(w):
                point = point.windex[w]
            elif point.wild is not None:
                point = point.wild
            else:
                return None
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


def merge_lt(m1, m2, sym):
    #return common area of log message (to be log template)
    ret = []
    for w1, w2 in zip(m1, m2):
        if w1 == w2:
            ret.append(w1)
        else:
            ret.append(sym)
    return ret

