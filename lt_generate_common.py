#!/usr/bin/env python
# coding: utf-8

import sys
import os
import logging

import fslib
import config
import testlog
import logheader
import logsplitter
import lt_manager
#import lt_generate_va
#import lt_generate_shiso

_config = config.common_config()
_logger = logging.getLogger(__name__)


class LTGen(object):

    sym = _config.get("log_template", "variable_symbol")

    def __init__(self, ltins = None):
        if ltins is None:
            self._ltins = lt_manager.LogTemplate()
        else:
            self._ltins = ltins

    def _add(self, l_w, l_s):
        return self._ltins.add(l_w, l_s, 1)

    def _count(self, ltid):
        return self._ltins.count(ltid)

    def _replace(self, ltid, l_w, l_s):
        return self._ltins.replace(ltid, l_w, l_s)

    def _search(self, l_w):
        return self._ltins.search(l_w)

    def _read_lt(self, l_w, l_s):
        # add or count
        return self._ltins.read_lt(l_w, l_s)

    def _get_lt(self, ltid):
        return self._ltins[ltid]

    def generate_ltset(self, targets):
        for fp in fslib.rep_dir(targets):
            with open(fp, 'r') as f:
                for line in f:
                    _logger.debug("line > {0}".format(line.rstrip("\n")))
                    message, info = logheader.split_header(line.rstrip("\n"))
                    if message is None: continue
                    l_w, l_s = logsplitter.split(message)
                    #ltid = self._search(l_w)
                    #if ltid is not None:
                    #    self._count(ltid)
                    #else:
                    #    _logger.debug("unknown line, analyze it")
                    #    ltw = self.process_line(l_w, l_s)
                    ltw = self.process_line(l_w, l_s)
        self.dump()

    def test_make(self):
        fn = "test.temp"
        if not os.path.exists(fn):
            import testlog
            testlog.test_make(output = fn)
        self.generate_ltset(fn)

    def process_line(self, l_w, l_s):
        raise NotImplementedError

    def dump(self):
        self._ltins.dump(_config.get("log_template", "db_filename"))


