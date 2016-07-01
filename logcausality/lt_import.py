#!/usr/bin/env python
# coding: utf-8

"""DEAD"""


import logging

import lt_common
import lt_misc
import logparser

_logger = logging.getLogger(__name__.rpartition(".")[-1])


class LTManager(lt_common.LTManager):

    def __init__(self, conf, db, table, reset_db, ltg_alg):
        self.searchtree = None

        super(LTManager, self).__init__(conf, db, table, reset_db, ltg_alg)

        self.def_path = conf.get("log_template_import", "def_path")
        self.mode = conf.get("log_template_import", "mode")
        if self.searchtree is None:
            self.searchtree = lt_misc.LTSearchTree(self.sym)

        self._open_def()

    def _open_def(self):
        lp = logparser.LogParser(self.conf)
        with open(self.def_path, 'r') as f:
            for line in f:
                if self.mode == "plain":
                    line = line.rstrip("\n")
                    ltw, lts = lp.split_message(line)
                    ltline = self.add_lt(ltw, lts, 0) 
                    self.searchtree.add(ltline.ltid, ltw)
                else:
                    raise ValueError("import_mode string is invalid")

    def process_line(self, l_w, l_s):
        ltid = self.searchtree.search(l_w)
        if ltid is None:
            _logger.warning(
                    "No log template found for message : {0}".format(l_w))
        else:
            self.count_lt(ltid)
            return self.table[ltid]

    def load(self):
        self.searchtree = self._load_pickle()

    def dump(self):
        obj = self.searchtree
        self._dump_pickle(obj)

