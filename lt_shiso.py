#!/usr/bin/env python
# coding: utf-8

"""
A log template generation algorithm proposed in [1].
[1] Masayoshi Mizutani. Incremental Mining of System Log Format.
in IEEE 10th International Conference on Services Computing, pp. 595–602, 2013.
"""

import sys
import logging
import numpy

import config
import lt_common

_config = config.common_config()
_logger = logging.getLogger(__name__)

class LTManager(lt_common.LTManager):
    
    def __init__(self, filename, threshold, max_child):
        super(LTManager, self).__init__(filename)
        self.ltgen = LTGen(self.table, threshold, max_child)

    def process_line(self, l_w, l_s):
        self.ltgen.process_line(l_w, l_s)


class Node():

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

    sym = _config.get("log_template", "variable_symbol")
    
    def __init__(self, table, threshold, max_child):
        self.table = table
        self.n_root = self._new_node()
        self.threshold = threshold
        self.max_child = max_child

    def _new_node(self, l_w = None, l_s = None):
        n = Node()
        if l_w is not None:
            ltid = self.table.add_lt(l_w, l_s)
            n.lt = self.table[ltid]
            _logger.debug("added as ltid {0}".format(ltid))
        return n

    def process_line(self, l_w, l_s):
        n_parent = self.n_root
        while True:
            for n_child in n_parent:
                _logger.debug(
                        "comparing with ltid {0}".format(n_child.lt.ltid))
                nc_lt = n_child.lt.words
                sr = self.seq_ratio(nc_lt, l_w)
                _logger.debug("seq_ratio : {0}".format(sr))
                if sr >= self.threshold:
                    _logger.debug(
                            "merged with ltid {0}".format(n_child.lt.ltid))
                    new_lt = self.merge_lt(nc_lt, l_w)
                    if not new_lt == nc_lt:
                        n_child.lt.replace(new_lt, l_s)
                        _logger.debug(
                                "ltid {0} replaced".format(n_child.lt.ltid))
                        _logger.debug("-> {0}".format(str(n_child.lt)))
                    n_child.lt.count()
                    return n_child.lt.ltid
                else:
                    if self.equal(nc_lt, l_w):
                        _logger.warning(
                            "comparing same line, but seqratio is small...")
                        #import pdb; pdb.set_trace()
            else:
                if len(n_parent) < self.max_child:
                    _logger.debug("no node to be merged, add new node")
                    n = self._new_node(l_w, l_s)
                    n_parent.join(n)
                    return n.lt.ltid
                else:
                    _logger.debug("children : {0}".format(
                            [e.lt.ltid for e in n_parent.l_child]))
                    l_sim = [(self.similarity(n_child.lt.words, l_w),
                            n_child) for n_child in n_parent]
                    n_parent = max(l_sim, key=lambda x: x[0])[1]
                    _logger.debug("go down to node(ltid {0})".format(
                            n_parent.lt.ltid))

    def seq_ratio(self, m1, m2):

        def c_coordinate(w):
            # retrun vector of characters of word
            # 4 demension : upper-case, lower-case, digit, symbol(others)
            l_cnt = [0.0 for i in range(4)]
            for c in w:
                if c.islower():
                    l_cnt[0] += 1.0
                elif c.isupper():
                    l_cnt[1] += 1.0
                elif c.isdigit():
                    l_cnt[2] += 1.0
                else:
                    l_cnt[3] += 1.0
            deno = numpy.linalg.norm(l_cnt)
            return [1.0 * e / deno for e in l_cnt]

        if len(m1) == len(m2):
            length = len(m1)
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

    def similarity(self, m1, m2):
        # return levenshtein distance that allows wildcard

        table = [ [0] * (len(m2) + 1) for i in range(len(m1) + 1) ]

        for i in range(len(m1) + 1):
            table[i][0] = i

        for j in range(len(m2) + 1):
            table[0][j] = j

        for i in range(1, len(m1) + 1):
            for j in range(1, len(m2) + 1):
                if (m1[i - 1] == m2[j - 1]) or \
                        m1[i - 1] == self.sym or m2[j - 1] == self.sym:
                    cost = 0
                else:
                    cost = 1
                table[i][j] = min(table[i - 1][j] + 1, table[i][ j - 1] + 1, \
                        table[i - 1][j - 1] + cost)
        return table[-1][-1]

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


def test_make():
    ltm = LTManager(None, 0.9, 4)
    ltm.process_dataset("test.temp")
    ltm.show()


if __name__ == "__main__":
    #logger_super = logging.getLogger("lt_common")
    #ch = logging.StreamHandler()
    #ch.setLevel(logging.DEBUG)
    #logger_super.setLevel(logging.DEBUG)
    #logger_super.addHandler(ch)
    #_logger.setLevel(logging.DEBUG)
    #_logger.addHandler(ch)
    #test_make()

    if len(sys.argv) < 2:
        sys.exit("usage : {0} targets".format(sys.argv[0]))
    ltgen = LTGen(None, 0.9, 4)
    ltgen.generate_ltset(sys.argv[1:])









