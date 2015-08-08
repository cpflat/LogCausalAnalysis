#!/usr/bin/env python
# coding: utf-8

import sys
import os
import cPickle as pickle

import config
import fslib
import logheader
import logsplitter

_config = config.common_config()
#TEMPLATE_GROUP_DEF = config.get("log_template", "group_def_filename")
VARIABLE_SYMBOL = _config.get("log_template", "variable_symbol")


class LogTemplateLine():

    __module__ = os.path.splitext(os.path.basename(__file__))[0]

    def __init__(self, ltid, words, style, cnt):
        self.ltid = ltid
        self.pid = ltid # to be deleted
        self.words = words
        self.style = style  # s[0], w[0], s[1]..., w[n], s[n+1]
        self.cnt = cnt

    def __str__(self): 
        return "".join([s + w for w, s in zip(self.words + [""], self.style)])

    def count(self):
        self.cnt += 1

    def len_variable(self):
        return sum(1 for w in self.words if w == VARIABLE_SYMBOL)

    def variable_location(self):
        return [cnt for cnt, w in enumerate(self.words)\
                if w == VARIABLE_SYMBOL]

    def replace(self, l_w, l_s):
        self.words = l_w
        self.style = l_s

    def get_variable(self, l_w):
        ret = []
        for e in zip(self.words, l_w):
            if e[0] == VARIABLE_SYMBOL:
                ret.append(e[1])
        return ret            

    def restore(self, args):
        buf = self.__str__()
        for arg in args:
            buf = buf.replace(VARIABLE_SYMBOL, arg, 1)
        return buf


class LogTemplateSearchTreeBranch():

    __module__ = os.path.splitext(os.path.basename(__file__))[0]

    def __init__(self, parent = None, word = None):
        self.windex = {}
        self.wild = None
        self.end = None

        # for reverse search
        self.parent = parent
        self.word = word  # word is None : for wild

    def unnecessary(self):
        return (len(self.windex) == 0) and \
                (self.wild is None) and \
                (self.end is None)


class LogTemplateSearchTree():

    __module__ = os.path.splitext(os.path.basename(__file__))[0]
    
    def __init__(self):
        self.tree = self._new_branch()

    @staticmethod
    def _new_branch(parent = None):
        return LogTemplateSearchTreeBranch(parent)

    def add(self, ltid, lt):
        point = self.tree
        for w in lt:
            if w == VARIABLE_SYMBOL:
                if point.wild is None:
                    point.wild = self._new_branch(point)
                point = point.wild
            else:
                if not point.windex.has_key(w):
                    point.windex[w] = self._new_branch(point)
                point = point.windex[w]
        else:
            point.end = ltid

    def remove(self, ltid, lt):
        point = self.tree
        for w in lt:
            if w in point.windex.keys():
                point = point.windex[w]
            elif point.wild is not None:
                point = point.wild
            else:
                # failed to find lt
                return None
        else:
            assert point.end == ltid 
            point.end = None 
            while point.unnecessary():
                w = point.word
                point = point.parent
                if w is None:
                    point.wild = None
                else:
                    point.wdict.pop(w)
            else:
                if self.tree is None:
                    self.tree = self._new_branch()

    def lt_exists(self, lt):
        point = self.tree
        for w in lt:
            if w == VARIABLE_SYMBOL:
                if point.wild is None:
                    return None
                point = point.wild
            else:
                if not point.windex.has_key(w):
                    return None
                point = point.windex[w]
        else:
            return point.end

    def search(self, l_w):
        point = self.tree
        for w in l_w:
            if w in point.windex.keys():
                point = point.windex[w]
            elif point.wild is not None:
                point = point.wild
            else:
                # search failure (unknown lt, or doubling lt)
                return None
        else:
            #if point.end is None:
            #    import pdb; pdb.set_trace()
            return point.end


class LogTemplateGroupDef():

    __module__ = os.path.splitext(os.path.basename(__file__))[0]

    def __init__(self, ltins, filename = None):
        self.lt = ltins
        self.d_lt = {}  # key : lt, val : (ltg, state)
        self.d_ltg = {}  # key : ltg, val : d_state(key : state, val : lt)
        self.d_ltg_desc = {} # key : ltg, val : description
        self.description_check_flag = False
        self.load_def(filename)

    def _add(self, ltid, ltgid, state):
        self.d_lt[ltid] = (ltgid, state)
        if not self.d_ltg.has_key(ltgid):
            self.d_ltg[ltgid] = {}
        self.d_ltg[ltgid].setdefault(state, []).append(ltid)

    def _set_description(self, ltgid, description):
        self.d_ltg_desc[ltgid] = description

    def load_def(self, filename):
        ltgid = None
        with open(filename, 'r') as f:
            for l in f:
                line = l.rstrip("\n")
                if line == "\n":
                    pass
                elif line[0] == "#":
                    command, arg = line.strip("#").partition(" ")
                    if command == "ltgroup":
                        if ltgid is None:
                            ltgid = 0
                        else:
                            ltgid += 1
                        self._set_description(ltgid, arg)
                        state = 0
                    elif command == "state":
                        state = int(arg)
                else:
                    ltid = int(line.split()[0])
                    self._add(ltid, ltgid)

    def ltid_info(self, ltid):
        return self.d_lt[ltid]

    def ltg_info(self, ltgid):
        return self.d_ltg[ltgid]  # dict(key : state, val : ltid)

    def ltg_state_info(self, ltgid, state):
        return self.d_ltg[ltgid][state]  # ltid

    def ltg_desc(self, ltgid):
        return self.d_ltg_desc[ltgid]


class LogTemplate():

    __module__ = os.path.splitext(os.path.basename(__file__))[0]

    def __init__(self, filename = None):
        self.ltdict = {}
        self.treem = LogTemplateSearchTree()
        self.ltlen = 0
        if filename is None:
            self.filename = _config.get("log_template", "db_filename")
        else:
            self.filename = filename

    def __iter__(self):
        return self._generator()

    def _generator(self):
        for ltid in range(self.ltlen):
            yield self.ltdict[ltid]

    def __len__(self):
        return self.ltlen

    def __getitem__(self, key):
        if key >= self.ltlen:
            raise IndexError("list index out of range")
            sys.stderr.write("")
        return self.ltdict[key]

    def __setitem__(self, key, value):
        sys.stderr.write(\
                "Warning : LogTemplate NOT allows using __setitem__\n")
        return None

    def add(self, ltwords, style, cnt):
        # words : description words and mask (astarisk)
        ltid = self.ltlen
        assert not self.ltdict.has_key(ltid)
        self.ltdict[ltid] = LogTemplateLine(ltid, ltwords, style, cnt)
        self.treem.add(ltid, ltwords)
        self.ltlen += 1
        return ltid

    def count(self, ltid):
        self.ltdict[ltid].count()

    # add or count
    def read_lt(self, ltwords, style):
        ltid = self.treem.lt_exists(ltwords)
        if ltid is not None:
            self.count(ltid)
            return False
        else:
            self.add(ltwords, style, 1)
            return True

    def replace(self, ltid, ltwords, style):
        self.ltdict[ltid].replace(ltwords, style)
        self.treem.remove(ltid, ltwords)
        self.treem.add(ltid, ltwords)

    # return ltid, or None if no lt found
    def search(self, words, style = None):
        return self.treem.search(words)

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


def init_lt(filename = None):
    return LogTemplate(filename)


def open_lt(filename = None):
    return LogTemplate(filename).load(filename)


def import_lt(ltins, filename):
    if ltins is None:
        ltins = init_lt()
    with open(filename, 'r') as f:
        for line in f:
            l_w, l_s = logsplitter.split(line)
            ltins.add(l_w, l_s, None)
    ltins.dump()    


def export_lt(ltins, filename):
    if ltins is None:
        ltins = open_lt()
    buf = []
    for lt in ltins:
        buf.append(str(lt))
    with open(filename, 'w') as f:
        f.write("\n".join(buf))


def count(ltins, targets):
    if ltins is None:
        ltins = open_lt()
    cntdict = {}
    for fn in fslib.rep_dir(targets):
        with open(fn, 'r') as f:
            for line in f:
                message, info = logheader.split_header(line)
                if message is None: continue
                l_w, l_s = logsplitter.split(message)
                ltid = ltins.search(l_w)
                cntdict[ltid] = cntdict.get(ltid, 0) + 1
    for ltid, cnt in cntdict.iteritems():
        ltins[ltid].cnt = cnt
    ltins.dump()


def validate(ltins, targets):
    if ltins is None:
        ltins = open_lt()
    for fn in fslib.rep_dir(targets):
        with open(fn, 'r') as f:
            for line in f:
                message, info = logheader.split_header(line)
                if message is None: continue
                l_w, l_s = logsplitter.split(message)
                #ltid = ltins.search(l_w, l_s)
                ltid = ltins.search(l_w)
                if ltid is None:
                    #import pdb; pdb.set_trace()
                    print("No template found for following log")
                    print("line : {0}".format(line))


def validate_style(ltins, targets):
    if ltins is None:
        ltins = open_lt()
    for fn in fslib.rep_dir(targets):
        with open(fn, 'r') as f:
            for line in f:
                message, info = logheader.split_header(line)
                if message is None: continue
                l_w, l_s = logsplitter.split(message)
                ltid = ltins.search(l_w, l_s)
                if not ltins[ltid].style == l_s:
                    print("a log have wrong symbol style")
                    print("ltid {0} : {1}".format(ltid, str(ltins[ltid])))
                    print("line : {0}".format(line))


def list_ltid():
    for lt in open_lt():
        print lt.ltid, str(lt), "({0})".format(lt.cnt)


if __name__ == "__main__":
    if len(sys.argv) <= 1:
        list_ltid()
    else:
        mode = sys.argv[1]
        if mode == "import":
            if len(sys.argv) < 3:
                sys.exit("usage: {0} import fn".format(sys.argv[0]))
            import_lt(None, sys.argv[2])
        elif mode == "export":
            if len(sys.argv) < 3:
                sys.exit("usage: {0} export fn".format(sys.argv[0]))
            export_lt(None, sys.argv[2])
        elif mode == "count":
            if len(sys.argv) < 3:
                sys.exit("usage: {0} count targets".format(sys.argv[0]))
            count(None, sys.argv[2:])
        elif mode == "validate":
            if len(sys.argv) < 3:
                sys.exit("usage: {0} validate targets".format(sys.argv[0]))
            validate(None, sys.argv[2:])
        elif mode == "vstyle":
            if len(sys.argv) < 3:
                sys.exit("usage: {0} vstyle targets".format(sys.argv[0]))
            validate_style(None, sys.argv[2:])
        else:
            sys.exit("argument error")



