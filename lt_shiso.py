#!/usr/bin/env python
# coding: utf-8

"""
A log template generation algorithm proposed in [1].
[1] Masayoshi Mizutani. Incremental Mining of System Log Format.
in IEEE 10th International Conference on Services Computing, pp. 595–602, 2013.
"""

import sys
import os
import logging
import numpy

import config
import lt_common

_config = config.common_config()
_logger = logging.getLogger(__name__)

class LTManager(lt_common.LTManager):
    
    __module__ = os.path.splitext(os.path.basename(__file__))[0]
    
    def __init__(self, filename = None):
        super(LTManager, self).__init__(filename)
        self.ltgen = None
        self.ltgroup = None

        self.ltgen_threshold = 0.9
        self.ltgen_max_child = 4

        self.ltgroup_nglength = 3
        self.ltgroup_th_lookup = 0.3
        self.ltgroup_th_distance = 0.85
        self.ltgroup_mem_ngram = True

    def _init_ltgen(self):
        self.ltgen = LTGen(self.table,
                threshold = self.ltgen_threshold,
                max_child = self.ltgen_max_child
                )
        
    def _init_ltgroup(self):
        self.ltgroup = LTGroup(self.table,
                ngram_length = self.ltgroup_nglength,
                th_lookup = self.ltgroup_th_lookup,
                th_distance = self.ltgroup_th_distance,
                mem_ngram = self.ltgroup_mem_ngram
                )

    def set_param_ltgen(self, threshold = None, max_child = None):
        if threshold is not None:
            self.ltgen_threshold = threshold
        if max_child is not None:
            self.ltgen_max_child = max_child

    def set_param_ltgroup(self, ngram_length = None, th_lookup = None,
            th_distance = None, mem_ngram = None):
        if ngram_length is not None:
            self.ltgroup_nglength = ngram_length
        if th_lookup is not None:
            self.ltgroup_th_lookup = th_lookup
        if th_distance is not None:
            self.ltgroup_th_distance = th_distance
        if mem_ngram is not None:
            self.ltgroup_mem_ngram = mem_ngram

    def process_line(self, l_w, l_s):
        if self.ltgen is None:
            self._init_ltgen()
            self._init_ltgroup()
        ltline, added_flag = self.ltgen.process_line(l_w, l_s)
        if added_flag:
            self.ltgroup.add(ltline)
        return ltline

    def show(self):

        def print_line(line):
            print line.ltid, str(line), "({0})".format(line.cnt)

        done_ltid = []
        for gid, l_ltline in self.ltgroup.iteritems():
            print "[log template group {0}]".format(gid)
            for ltline in l_ltline:
                print_line(ltline)
                done_ltid.append(ltline.ltid)

        print "[log template with no group]"
        for line in self.table:
            if not line.ltid in done_ltid:
                print_line(line)

class LTGenNode():

    __module__ = os.path.splitext(os.path.basename(__file__))[0]
    
    def __init__(self):
        self.l_child = []
        self.lt = None

    def __len__(self):
        return len(self.l_child)

    def __iter__(self):
        return self.l_child.__iter__()

    def join(self, node):
        self.l_child.append(node)


class LTGen():

    __module__ = os.path.splitext(os.path.basename(__file__))[0]
    sym = _config.get("log_template", "variable_symbol")
    
    def __init__(self, table, threshold, max_child):
        self.table = table
        self.n_root = self._new_node()
        self.threshold = threshold
        self.max_child = max_child

    def _new_node(self, l_w = None, l_s = None):
        n = LTGenNode()
        if l_w is not None:
            ltid = self.table.add_lt(l_w, l_s)
            n.lt = self.table[ltid]
            #_logger.debug("added as ltid {0}".format(ltid))
        return n

    def process_line(self, l_w, l_s):
        n_parent = self.n_root
        while True:
            for n_child in n_parent:
            #    _logger.debug(
            #            "comparing with ltid {0}".format(n_child.lt.ltid))
                nc_lt = n_child.lt.words
                sr = self.seq_ratio(nc_lt, l_w)
            #    _logger.debug("seq_ratio : {0}".format(sr))
                if sr >= self.threshold:
            #        _logger.debug(
            #                "merged with ltid {0}".format(n_child.lt.ltid))
                    new_lt = lt_common.merge_lt(nc_lt, l_w)
                    if not new_lt == nc_lt:
                        n_child.lt.replace(new_lt, l_s)
            #            _logger.debug(
            #                    "ltid {0} replaced".format(n_child.lt.ltid))
            #            _logger.debug("-> {0}".format(str(n_child.lt)))
                    n_child.lt.count()
                    return n_child.lt, False
            #    else:
            #        if self.equal(nc_lt, l_w):
            #            _logger.warning(
            #                "comparing same line, but seqratio is small...")
            else:
                if len(n_parent) < self.max_child:
            #        _logger.debug("no node to be merged, add new node")
                    n = self._new_node(l_w, l_s)
                    n_parent.join(n)
                    return n.lt, True
                else:
            #        _logger.debug("children : {0}".format(
            #                [e.lt.ltid for e in n_parent.l_child]))
                    l_sim = [(edit_distance(n_child.lt.words, l_w),
                            n_child) for n_child in n_parent]
                    n_parent = max(l_sim, key=lambda x: x[0])[1]
            #        _logger.debug("go down to node(ltid {0})".format(
            #                n_parent.lt.ltid))

    def seq_ratio(self, m1, m2):

        #def c_coordinate(w):
        #    # retrun vector of characters of word
        #    # 4 demension : upper-case, lower-case, digit, symbol(others)
        #    l_cnt = [0.0 for i in range(4)]
        #    for c in w:
        #        if c.islower():
        #            l_cnt[0] += 1.0
        #        elif c.isupper():
        #            l_cnt[1] += 1.0
        #        elif c.isdigit():
        #            l_cnt[2] += 1.0
        #        else:
        #            l_cnt[3] += 1.0
        #    deno = numpy.linalg.norm(l_cnt)
        #    return [1.0 * e / deno for e in l_cnt]

        def c_coordinate(w):
            l_cnt = [0.0 for i in range(26 + 26 + 2)] # A-Z, a-z, digit, symbol
            for c in w:
                if c.isupper():
                    ind = ord(c) - 65
                    l_cnt[ind] += 1.0
                elif c.islower():
                    ind = ord(c) - 97
                    l_cnt[ind + 26] += 1.0
                elif c.isdigit():
                    l_cnt[-2] += 1.0
                else:
                    l_cnt[-1] += 1.0
            deno = numpy.linalg.norm(l_cnt)
            return [1.0 * e / deno for e in l_cnt]

        if len(m1) == len(m2):
            length = len(m1)
            if length == 0:
                return 1.0

            sum_dist = 0.0
            for w1, w2 in zip(m1, m2):
                if w1 == self.sym or w2 == self.sym:
                    pass
                else:
                    c_w1 = c_coordinate(w1)
                    c_w2 = c_coordinate(w2)
                    dist = sum([numpy.power(e1 - e2, 2)
                            for e1, e2 in zip(c_w1, c_w2)])
                    sum_dist += dist
            return 1.0 - (sum_dist / (2.0 * length))
        else:
            return 0.0

    #def similarity(self, m1, m2):
        # return levenshtein distance that allows wildcard

        #table = [ [0] * (len(m2) + 1) for i in range(len(m1) + 1) ]

        #for i in range(len(m1) + 1):
        #    table[i][0] = i

        #for j in range(len(m2) + 1):
        #    table[0][j] = j

        #for i in range(1, len(m1) + 1):
        #    for j in range(1, len(m2) + 1):
        #        if (m1[i - 1] == m2[j - 1]) or \
        #                m1[i - 1] == self.sym or m2[j - 1] == self.sym:
        #            cost = 0
        #        else:
        #            cost = 1
        #        table[i][j] = min(table[i - 1][j] + 1, table[i][ j - 1] + 1, \
        #                table[i - 1][j - 1] + cost)
        #return table[-1][-1]

    def equal(self, m1, m2):
        if len(m1) == len(m2):
            for w1, w2 in zip(m1, m2):
                if w1 == w2 or w1 == self.sym or w2 == self.sym:
                    pass
                else:
                    return False
            else:
                return True
        else:
            return False

    def merge_lt(self, m1, m2):
        #return common area of log message (to be log template)
        ret = []
        for w1, w2 in zip(m1, m2):
            if w1 == w2:
                ret.append(w1)
            else:
                ret.append(self.sym)
        return ret


class LTGroup():

    __module__ = os.path.splitext(os.path.basename(__file__))[0]
    sym = _config.get("log_template", "variable_symbol")

    def __init__(self, table, ngram_length = 3,
            th_lookup = 0.3, th_distance = 0.85, mem_ngram = True):
        self.table = table
        self.d_group = {} # key : groupid, val : [ltline, ...]
        self.d_vgroup = {} # key : ltid, val : groupid
        self.ngram_length = ngram_length
        self.th_lookup = th_lookup
        self.th_distance = th_distance
        self.mem_ngram = mem_ngram
        if self.mem_ngram:
            self.d_ngram = {} # key : ltid, val : [str, ...]

    def __iter__(self):
        return self._generator()

    def _generator(self):
        for gid in self.d_group.keys():
            yield self.d_group[gid]

    def iteritems(self):
        for gid in self.d_group.keys():
            yield gid, self.d_group[gid]

    def _next_groupid(self):
        cnt = 0
        while self.d_group.has_key(cnt):
            cnt += 1
        else:
            return cnt

    def add(self, lt_new):
        _logger.debug("group search for ltid {0}".format(lt_new.ltid))

        r_max = 0.0
        lt_max = None
        l_cnt = [0 for i in self.table]
        l_ng1 = self._get_ngram(lt_new)
        for ng in l_ng1:
            for lt_temp, l_ng2 in self._lookup_ngram(ng, lt_new.ltid):
                l_cnt[lt_temp.ltid] += 1
                r = 2.0 * l_cnt[lt_temp.ltid] / (len(l_ng1) + len(l_ng2)) 
                if r > r_max:
                    r_max = r
                    lt_max = lt_temp
        _logger.debug("ngram co-occurrance map : {0}".format(l_cnt))
        _logger.debug("r_max : {0}".format(r_max))
        if r_max > self.th_lookup:
            assert lt_max is not None, "bad threshold for lt group lookup"
            _logger.debug("lt_max ltid : {0}".format(lt_max.ltid))
            ltw2 = lt_max.words
            d = 2.0 * edit_distance(lt_new.words, lt_max.words) / \
                    (len(lt_new.words) + len(lt_max.words))
            _logger.debug("edit distance ratio : {0}".format(d))
            if d < self.th_distance:
                gid = self.mk_group(lt_new, lt_max)
                _logger.debug("smaller than threshold")
                _logger.debug("merge it (gid {0})".format(gid))
                _logger.debug("gid {0} : {1}".format(gid, \
                        [ltline.ltid for ltline in self.d_group[gid]]))
                return gid
        return None

    def mk_group(self, lt_new, lt_old):

        def add_ltid(groupid, ltline):
            self.d_group.setdefault(groupid, []).append(ltline)
            self.d_vgroup[ltline.ltid] = groupid

        assert not self.d_vgroup.has_key(lt_new.ltid)
        if self.d_vgroup.has_key(lt_old.ltid):
            groupid = self.d_vgroup[lt_old.ltid]
            add_ltid(groupid, lt_new)
        else:
            groupid = self._next_groupid()
            add_ltid(groupid, lt_old)
            add_ltid(groupid, lt_new)
        return groupid

    def _get_ngram(self, ltline):

        def ngram(ltw, length):
            return [ltw[i:i+length] for i in range(len(ltw) - length)]
            
        if self.mem_ngram:
            if not self.d_ngram.has_key(ltline.ltid):
                self.d_ngram[ltline.ltid] = \
                        ngram(ltline.words, self.ngram_length)
            return self.d_ngram[ltline.ltid]
        else:
            return ngram(ltline.words, self.ngram_length)

    def _lookup_ngram(self, ng, ltid):
        ret = []
        for ltline in self.table:
            if ng in self._get_ngram(ltline) and not ltline.ltid == ltid:
                ret.append((ltline, ng))
        return ret


def edit_distance(m1, m2):
    # return levenshtein distance that allows wildcard
    sym = lt_common.VARIABLE_SYMBOL

    table = [ [0] * (len(m2) + 1) for i in range(len(m1) + 1) ]

    for i in range(len(m1) + 1):
        table[i][0] = i

    for j in range(len(m2) + 1):
        table[0][j] = j

    for i in range(1, len(m1) + 1):
        for j in range(1, len(m2) + 1):
            if (m1[i - 1] == m2[j - 1]) or \
                    m1[i - 1] == sym or m2[j - 1] == sym:
                cost = 0
            else:
                cost = 1
            table[i][j] = min(table[i - 1][j] + 1, table[i][ j - 1] + 1, \
                    table[i - 1][j - 1] + cost)
    return table[-1][-1]


def test_make():
    ltm = LTManager(None)
    ltm.set_param(0.9, 4)
    ltm.process_dataset("test.temp")
    ltm.show()


if __name__ == "__main__":
    #logger_super = logging.getLogger("lt_common")
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    #logger_super.setLevel(logging.DEBUG)
    #logger_super.addHandler(ch)
    _logger.setLevel(logging.DEBUG)
    _logger.addHandler(ch)
    #test_make()

    if len(sys.argv) < 2:
        sys.exit("usage : {0} targets".format(sys.argv[0]))
    ltm = LTManager(None)
    ltm.set_param_ltgen(0.9, 4)
    ltm.process_dataset(sys.argv[1:])
    ltm.show()


