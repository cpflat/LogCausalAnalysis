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
#import scipy.spatial.distance

import lt_generate_common

_logger = logging.getLogger(__name__)


class Node():

    def __init__(self):
        self.l_child = []
        self.ltid = None

    def __len__(self):
        return len(self.l_child)

    def __iter__(self):
        return self.l_child.__iter__()

    def join(self, node):
        self.l_child.append(node)


class LTGen(lt_generate_common.LTGen):

    def __init__(self, ltins, threshold, max_child):
        super(LTGen, self).__init__(ltins)
        self.n_root = self._new_node()
        self.threshold = threshold
        self.max_child = max_child

    def _new_node(self, l_w = None, l_s = None):
        n = Node()
        if l_w is not None:
            n.ltid = self._add(l_w, l_s)
            _logger.debug("added as ltid {0}".format(n.ltid))
            _logger.debug("-> {0}".format(self._get_lt(n.ltid)))
        return n

    def process_line(self, l_w, l_s):
        n_parent = self.n_root
        while True:
            for n_child in n_parent:
                _logger.debug("comparing with ltid {0}".format(n_child.ltid))
                nc_lt = self._get_lt(n_child.ltid).words
                sr = self.seq_ratio(nc_lt, l_w)
                _logger.debug("seq_ratio : {0}".format(sr))
                if sr >= self.threshold:
                    _logger.debug("merged with ltid {0}".format(n_child.ltid))
                    new_lt = self.merge_lt(nc_lt, l_w)
                    if not new_lt == nc_lt:
                        self._replace(n_child.ltid, new_lt, l_s)
                        _logger.debug("ltid {0} replaced".format(n_child.ltid))
                        _logger.debug("-> {0}".format(\
                                self._get_lt(n_child.ltid)))
                    self._count(n_child.ltid)
                    return n_child.ltid
            else:
                if len(n_parent) < self.max_child:
                    _logger.debug("no node to be merged, add new node")
                    n = self._new_node(l_w, l_s)
                    n_parent.join(n)
                    return n.ltid
                else:
                    _logger.debug("children : {0}".format(\
                            [e.ltid for e in n_parent.l_child]))
                    _logger.debug("go down to ltid")
                    nc_lt = self._get_lt(n_child.ltid).words
                    l_sim = [(self.similarity(self._get_lt(n_child.ltid).words,
                            l_w), n_child) for n_child in n_parent]
                    n_parent = max(l_sim, key=lambda x: x[0])[1]

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
            return l_cnt

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
    ltgen = LTGen(None, 0.9, 4)
    ltgen.test_make()


if __name__ == "__main__":
    #logger_super = logging.getLogger("lt_generate_common")
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

