#!/usr/bin/env python
# coding: utf-8

import os
import logging
import cPickle as pickle

import config
import fslib
import logparser
#import logheader
#import logsplitter

_logger = logging.getLogger(__name__)

class LTLine():

    __module__ = os.path.splitext(os.path.basename(__file__))[0]

    def __init__(self, ltid, words, style, cnt, sym):
        self.ltid = ltid
        self.words = words
        self.style = style # None if conf(log_template.sym_ignore) is False
        # s[0], w[0], s[1]..., w[n], s[n+1]
        self.cnt = cnt
        self.sym = sym

    def __str__(self):
        return self.restore_words(self.words)

    def len_variable(self):
        return sum(1 for w in self.words if w == self.sym)
    
    def get_variable(self, l_w):
        ret = []
        for e in zip(self.words, l_w):
            if e[0] == self.sym:
                ret.append(e[1])
        return ret            

    def variable_location(self):
        return [cnt for cnt, w in enumerate(self.words)\
                if w == self.sym]

    def restore_words(self, l_w):
        if self.style is None:
            return "".join(l_w)
        else:
            return "".join([s + w for w, s in zip(l_w, self.style)])

    def restore_args(self, args):
        buf = self.__str__()
        for arg in args:
            buf = buf.replace(self.sym, arg, 1)
        return buf

    def count(self):
        self.cnt += 1

    def replace(self, l_w, l_s = None, count = None):
        self.words = l_w
        if l_s is not None:
            self.style = l_s
        if count is not None:
            self.cnt = count


class LTTable():

    __module__ = os.path.splitext(os.path.basename(__file__))[0]

    def __init__(self, sym):
        self.sym = sym
        self.ltdict = {}

    def __iter__(self):
        return self._generator()

    def _generator(self):
        for ltid in self.ltdict.keys():
            yield self.ltdict[ltid]

    def __len__(self):
        return len(self.ltdict.keys())

    def __getitem__(self, key):
        assert isinstance(key, int)
        if not self.ltdict.has_key(key):
            raise IndexError("list index out of range")
        return self.ltdict[key]

    def __setitem__(self, key, value):
        sys.stderr.write(\
                "Warning : LogTemplate NOT allows using __setitem__\n")
        return None

    def next_ltid(self):
        cnt = 0
        while self.ltdict.has_key(cnt):
            cnt += 1 
        else:
            return cnt

    def add_lt(self, ltwords, style, cnt = 1):
        ltid = self.next_ltid()
        assert not self.ltdict.has_key(ltid)
        self.ltdict[ltid] = LTLine(ltid, ltwords, style, cnt, self.sym)
        return ltid

    def count_lt(self, ltid):
        self.ltdict[ltid].count()

    def replace_lt(self, ltid, l_w, l_s = None, count = None):
        self.ltdict[ltid].replace(l_w, l_s, count)

    def remove_lt(self, ltid):
        self.ltdict.pop(ltid)


class LTManager(object):

    __module__ = os.path.splitext(os.path.basename(__file__))[0]

    def __init__(self, conf, filename = None):
        self.conf = conf
        if filename is None:
            self.filename = self.conf.get("log_template", "db_filename")
        else:
            self.filename = filename
        self.sym = self.conf.get("log_template", "variable_symbol")
        self.table = LTTable(self.sym)
        self.ltgroup = None # LTGroup

    @staticmethod
    def _str_ltline(line):
        return " ".join((str(line.ltid), str(line),
                "({0})".format(line.cnt)))

    def _str_group(self, gid):
        length = len(self.ltgroup.get_lt(gid))
        cnt = self.ltgroup.get_count(gid)
        rep = self.ltgroup.rep(gid)
        return "[ltgroup {0} ({1}, {2}) {3}]".format(gid, length, cnt, rep)

    def show(self):
        if self.ltgroup is None:
            for ltline in self.table:
                self._str_ltline(ltline)
        else:
            for gid in self.ltgroup:
                self.show_group(gid)

    def show_group(self, gid):
        buf = []
        for ltline in self.ltgroup.get_lt(gid):
            buf.append(self._str_ltline(ltline))
        print self._str_group(gid)
        print "\n".join(buf)

    def show_all_lt(self):
        for ltline in self.table:
            print self._str_ltline(ltline)

    def show_all_group(self):
        if self.ltgroup is None:
            _logger.warning("ltgroup not used")
        for gid in self.ltgroup:
            print self._str_group(gid)

    def process_line(self, l_w, l_s):
        # return ltline
        raise NotImplementedError

    def process_dataset(self, targets):
        # for testing to construct log template table
        for fp in fslib.rep_dir(targets):
            with open(fp, 'r') as f:
                for line in f:
                    line = line.rstrip("\n")
                    _logger.debug("line > {0}".format(line))
                    dt, host, l_w, l_s = logparser.process_line(line)
                    if l_w is None: continue
                    self.process_line(l_w, l_s)
        self.dump()

    def load(self, fn = None):
        if not fn: fn = self.filename
        with open(fn, 'r') as f:
            d = pickle.load(f)
        self.__dict__.update(d)
        return self

    def dump(self, fn = None):
        if not fn: fn = self.filename
        with open(fn, 'w') as f:
            pickle.dump(self.__dict__, f)


class LTGroup(object):

    __module__ = os.path.splitext(os.path.basename(__file__))[0]

    def __init__(self, table):
        self.table = table # LTTable, readonly
        self.d_group = {} # key : groupid, val : [ltline, ...]
        self.d_rgroup = {} # key : ltid, val : groupid

    def __iter__(self):
        return self._generator()

    def _generator(self):
        for gid in self.d_group.keys():
            yield gid

    def __getitem__(self, gid):
        assert isinstance(gid, int)
        if not self.d_group.has_key(gid):
            raise IndexError("list index out of range")
        return self.d_group[gid]

    def get_lt(self, gid):
        return self.d_group[gid]

    def get_gid(self, ltid):
        return self.d_rgroup[ltid]

    def get_count(self, gid):
        return sum([ltline.cnt for ltline in self.d_group[gid]])

    def rep(self, gid):
        # return representative lt in given group
        # lt with smallest id in lts of shortest length
        min_len = None
        l_ltline = []
        for ltline in self.d_group[gid]:
            if min_len is None:
                min_len = len(ltline.words)
                l_ltline.append(ltline)
            elif len(ltline.words) == min_len:
                l_ltline.append(ltline)
            elif len(lt.words) < min_len:
                min_len = len(ltline.words)
                l_ltline = [ltline]
        return min(l_ltline, key=lambda ltline: ltline.ltid)

    def iteritems(self):
        for gid in self.d_group.keys():
            yield gid, self.d_group[gid]

    def _next_groupid(self):
        cnt = 0
        while self.d_group.has_key(cnt):
            cnt += 1
        else:
            return cnt

    def add_ltid(self, gid, ltline):
        self.d_group.setdefault(gid, []).append(ltline)
        self.d_rgroup[ltline.ltid] = gid


class LTSearchTreeNode():

    __module__ = os.path.splitext(os.path.basename(__file__))[0]

    def __init__(self, parent, word):
        self.windex = {}
        self.wild = None
        self.end = None
        self.parent = parent # for reverse search to remove
        self.word = word

    def child(self, word):
        if word is None:
            # wildcard
            return self.wild
        elif self.windex.has_key(word):
            return self.windex(word)
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


class LTSearchTree():

    __module__ = os.path.splitext(os.path.basename(__file__))[0]

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


def merge_lt(m1, m2, sym):
    #return common area of log message (to be log template)
    ret = []
    for w1, w2 in zip(m1, m2):
        if w1 == w2:
            ret.append(w1)
        else:
            ret.append(sym)
    return ret

