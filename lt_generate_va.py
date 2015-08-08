#!/usr/bin/env python
# coding: utf-8

import sys
import math

import fslib
import logheader
import logsplitter
import lt_generate_common


class LTGen(lt_generate_common.LTGen):

    def __init__(self, ltins, targets, th_mode = "median"):
        super(LTGen, self).__init__(ltins)
        #lt_generate.LTGen.__init__(self, ltins)
        self.th_mode = th_mode
        self.d_w = {}
        for fp in fslib.rep_dir(targets):
            with open(fp, 'r') as f:
                for line in f:
                    message, info = logheader.split_header(line.strip("\n"))
                    if message is None: continue
                    l_w, l_s = logsplitter.split(message)
                    for w in l_w:
                        self.d_w[w] = self.d_w.get(w, 0) + 1

    def _th(self, l_cnt):
        if self.th_mode == "median":
            seq = sorted(l_cnt, reverse=True)
            num = int(math.ceil(1.0 * len(l_cnt) / 2))
            return seq[num - 1]

    def process_line(self, l_w, l_s):
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
        return self._read_lt(ltw, l_s)


def test_make():
    ltgen = LTGen(None, "test.temp")
    ltgen.test_make()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit("usage : {0} targets".format(sys.argv[0]))
    ltgen = LTGen(None, sys.argv[1:])
    ltgen.generate_ltset(sys.argv[1:])

