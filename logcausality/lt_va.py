#!/usr/bin/env python
# coding: utf-8

"""DEAD"""


import sys
import math
import cPickle as pickle

import common
import config
import lt_common
import lt_misc
import logparser


class LTManager(lt_common.LTManager):

    def __init__(self, conf, db, table, reset_db, ltg_alg):
        self.searchtree = None
        self.ltgen = None

        super(LTManager, self).__init__(conf, db, table, reset_db, ltg_alg)
        
        self.src_fn = self.conf.get("log_template_va", "src_path")
        if self.src_fn is not None or self.src_fn == "":
            self.src_fn = conf.get("general", "src_path")

        self._init_ltgen()
        if self.searchtree is None:
            self.searchtree = lt_misc.LTSearchTree(self.sym)

    def _init_ltgen(self):
        if self.ltgen is None:
            self.ltgen = LTGenVA(self.sym,
                update_flag = self.conf.get("log_template_va", "incre_update"),
                th_mode = self.conf.get("log_template_va", "threshold_mode"),
                threshold = self.conf.getfloat("log_template_va", "threshold"))

    def process_line(self, l_w, l_s):
        #ltw = self.ltgen.process_line(l_w)
        ltid = self.searchtree.search(l_w)
        if ltid is None:
            ltline = self.add_lt(l_w, l_s)
            self.searchtree.add(ltline.ltid, ltw)
        else:
            self.count_lt(ltid)
            ltline = self.table[ltid]
        return ltline

    def load(self):
        if self.ltgen is None:
            self._init_ltgen()
        self.ltgen.d_w, self.searchtree = self._load_pickle()

    def dump(self):
        obj = (self.ltgen.d_w, self.searchtree)
        self._dump_pickle(obj)


class LTGenVA():

    def __init__(self, sym, update_flag, th_mode, threshold):
        self.sym = sym
        self.update_flag = update_flag
        self.th_mode = th_mode
        self.threshold = threshold
        self.d_w = {}
        
    def mk_dict(self, src_path):
        for fp in common.rep_dir(src_path):
            with open(fp, 'r') as f:
                for line in f:
                    dt, host, l_w, l_s = logparser.process_line(line)
                    if l_w is None: continue
                    self._count_line(l_w)

    def _th(self, l_cnt):
        if self.th_mode == "median":
            seq = sorted(l_cnt, reverse=True)
            num = int(math.ceil(1.0 * self.threshold * len(l_cnt) ))
            return seq[num - 1]
        else:
            raise ValueError()

    def _count_line(self, l_w):
        for w in l_w:
            self.d_w[w] = self.d_w.get(w, 0) + 1

    def _make_lt(self, l_w):
        l_cnt = []
        for w in l_w:
            if self.d_w.has_key(w):
                cnt = self.d_w[w]
            else:
                cnt = 0
            l_cnt.append(cnt)

        threshold = self._th(l_cnt)
        ltw = []
        for w, cnt in zip(l_w, l_cnt):
            if cnt >= threshold:
                ltw.append(w)
            else:
                ltw.append(self.sym)
        return ltw

    def process_line(self, l_w):
        # return format
        if self.update_flag:
            self._count_line(l_w)
        return self._make_lt(l_w)


