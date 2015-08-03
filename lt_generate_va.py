#!/usr/bin/env python
# coding: utf-8

import math

import fslib
import logheader
import logsplitter
import lt_generate


class LTGen(lt_generate.LogTemplateGenerater):

    def __init__(self, targets, th_mode = "median"):
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

    def mk_lt(self, l_w):
        l_cnt = []
        for w in l_w:
            if self.d_w.has_key(w):
                cnt = self.d_w[w]
            else:
                cnt = 0
            l_cnt.append(cnt)

        threshold = self._th(l_cnt)
        ret = []
        for w, cnt in zip(l_w, l_cnt):
            if cnt >= threshold:
                ret.append(w)
            else:
                ret.append(self.sym)
        return ret


