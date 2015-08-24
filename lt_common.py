#!/usr/bin/env python
# coding: utf-8

import os
import logging
import cPickle as pickle

import config
import fslib
import logheader
import logsplitter

_config = config.common_config()
_logger = logging.getLogger(__name__)
VARIABLE_SYMBOL = _config.get("log_template", "variable_symbol")

class LTLine():

    __module__ = os.path.splitext(os.path.basename(__file__))[0]
    sym = VARIABLE_SYMBOL

    def __init__(self, ltid, words, style, cnt):
        self.ltid = ltid
        self.words = words
        self.style = style  # s[0], w[0], s[1]..., w[n], s[n+1]
        self.cnt = cnt

    def __str__(self): 
        return "".join([s + w for w, s in zip(self.words + [""], self.style)])

    def count(self):
        self.cnt += 1
    
    def replace(self, l_w, l_s = None, count = None):
        self.words = l_w
        if l_s is not None:
            self.style = l_s
        if count is not None:
            self.cnt = count

    def len_variable(self):
        return sum(1 for w in self.words if w == self.sym)

    def variable_location(self):
        return [cnt for cnt, w in enumerate(self.words)\
                if w == self.sym]

    def get_variable(self, l_w):
        ret = []
        for e in zip(self.words, l_w):
            if e[0] == self.sym:
                ret.append(e[1])
        return ret            

    def restore(self, args):
        buf = self.__str__()
        for arg in args:
            buf = buf.replace(self.sym, arg, 1)
        return buf

    def restore_wordlist(self, args):
        l_arg = args[:]
        ret = []
        for w in self.words:
            if w == self.sym:
                ret.append(l_arg.pop(0))
            else:
                ret.append(w)
        return ret


class LTTable():

    __module__ = os.path.splitext(os.path.basename(__file__))[0]

    def __init__(self):
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
        self.ltdict[ltid] = LTLine(ltid, ltwords, style, cnt)
        return ltid

    def count_lt(self, ltid):
        self.ltdict[ltid].count()

    def replace_lt(self, ltid, l_w, l_s = None, count = None):
        self.ltdict[ltid].replace(l_w, l_s, count)

    def remove_lt(self, ltid):
        self.ltdict.pop(ltid)


class LTManager(object):

    __module__ = os.path.splitext(os.path.basename(__file__))[0]
    default_fn = _config.get("log_template", "db_filename")

    def __init__(self, filename = None):
        if filename is None:
            self.filename = self.default_fn
        else:
            self.filename = filename
        self.table = LTTable()

    def show(self):
        for line in self.table:
            print line.ltid, str(line), "({0})".format(line.cnt)

    def process_line(self, l_w, l_s):
        # retrun ltline
        raise NotImplementedError

    def process_dataset(self, targets):
        # for testing to construct log template table
        for fp in fslib.rep_dir(targets):
            with open(fp, 'r') as f:
                for line in f:
                    _logger.debug("line > {0}".format(line.rstrip("\n")))
                    message, info = logheader.split_header(line.rstrip("\n"))
                    if message is None: continue
                    l_w, l_s = logsplitter.split(message)
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

    sym = VARIABLE_SYMBOL
    
    def __init__(self):
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


def merge_lt(m1, m2):
    #return common area of log message (to be log template)
    ret = []
    for w1, w2 in zip(m1, m2):
        if w1 == w2:
            ret.append(w1)
        else:
            ret.append(VARIABLE_SYMBOL)
    return ret

