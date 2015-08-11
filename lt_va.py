#!/usr/bin/env python
# coding: utf-8

import sys
import math

import config
import fslib
import lt_common
import logheader
import logsplitter

_config = config.common_config()

class LTManager(lt_common.LTManager):

    def __init__(self, filename, past_targets):
        super(LTManager, self).__init__(filename)
        self.ltgen = VA(past_targets)
        self.searchtree = lt_common.LTSearchTree()

    def process_line(self, l_w, l_s):
        ltw = self.ltgen.process_line(l_w)
        ltid = self.searchtree.search(ltw)
        if ltid is None:
            ltid = self.table.add_lt(ltw, l_s)
            self.searchtree.add(ltid, ltw)
        else:
            self.table.count_lt(ltid)
        return ltid


class VA():

    sym = _config.get("log_template", "variable_symbol")
    
    def __init__(self, past_targets, th_mode = "median"):
        self.th_mode = th_mode
        self.d_w = {}
        for fp in fslib.rep_dir(past_targets):
            with open(fp, 'r') as f:
                for line in f:
                    message, info = logheader.split_header(line.strip("\n"))
                    if message is None: continue
                    l_w, l_s = logsplitter.split(message)
                    self._count_line(l_w)

    def _th(self, l_cnt):
        if self.th_mode == "median":
            seq = sorted(l_cnt, reverse=True)
            num = int(math.ceil(1.0 * len(l_cnt) / 2))
            return seq[num - 1]

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
        # retrun format
        self._count_line(l_w)
        return self._make_lt(l_w)


def test_make():
    ltm = LTManager(None, "va_learn.temp")
    ltm.process_dataset("test.temp")
    ltm.show()


if __name__ == "__main__":
    test_make()


