#!/usr/bin/env python
# coding: utf-8

import sys
import os

import fslib
import config
import logheader
import logsplitter
import lt_manager
import lt_generate_va

_config = config.common_config()


class LogTemplateGenerater():

    sym = _config.get("log_template", "variable_symbol")
    
    def __init__(self):
        raise NotImplementedError

    def mk_lt(self):
        raise NotImplementedError


def generate_lt(alg, targets):
    ltins = lt_manager.LogTemplate()
    if alg == "va":
        ltgen = lt_generate_va.LTGen(targets)
    else:
        raise ValueError("Invalid algorithm name")

    for fp in fslib.rep_dir(targets):
        with open(fp, 'r') as f:
            for line in f:
                message, info = logheader.split_header(line.strip("\n"))
                if message is None: continue
                l_w, l_s = logsplitter.split(message)
                ltw = ltgen.mk_lt(l_w)
                ltins.read_lt(ltw, l_s)
    
    ltins.dump(_config.get("log_template", "db_filename"))


def test_make(alg = "va"):
    fn = "test.temp"
    if not os.path.exists(fn):
        import testlog
        testlog.test_make(output = fn)
    generate_lt(alg, fn)


if __name__ == "__main__":
    usage = "usage : {0} alg targets ...".format(sys.argv[0])
    if len(sys.argv) <= 1:
        sys.exit(usage)
    elif len(sys.argv) == 2:
        if sys.argv[1] == "test":
            test_make()
        else:
            sys.exit(usage)
    elif len(sys.argv) > 2:
        generate_lt(sys.argv[1], sys.argv[2:])

