#!/usr/bin/env python
# coding: utf-8

"""
lt_crf.py needs CRF++ (https://taku910.github.io/crfpp/)
and python wrapper in CRF++.
"""

import sys
import CRFPP
#import logging

import common
import config
import lt_common
#import logparser


class LTGenCRF(lt_common.LTGen):

    def __init__(self, table, sym, model):
        super(LTGenCRF, self).__init__(table, sym)
        self.crf = CRFPP.Tagger("-m " + model + " -v 3 -n2")

    def process_line(self, l_w, l_s):
        self.crf.clear()
        for w in l_w:
            self.crf.add("{0} W".format(w))
        self.crf.parse()

        tpl = []
        for wid, w in enumerate(l_w):
            label = self.crf.y2(wid)
            if label == "D":
                tpl.append(w)
            elif label == "V":
                tpl.append(self._sym)

        if self._table.exists(tpl):
            tid = self._table.get_tid(tpl)
            return tid, self.state_unchanged
        else:
            tid = self._table.add(tpl)
            return tid, self.state_added


def train(conf, train_fn = None):
    model_fn = conf.get("log_template_crf", "model_filename")
    template_fn = conf.get("log_template_crf", "feature_template")
    if train_fn is None:
        train_fn = conf.get("log_template_crf", "train_filename")

    cmd = ["crf_learn", template_fn, train_fn, model_fn]
    ret, stdout, stderr = common.call_process()
    if not ret == "0":
        raise ValueError("crf_learn failed")
    print stdout
    sys.stderr.write(stderr)
    return


def lt2trainsource(conf, fn = None):
    import log_db
    ld = log_db.LogData(conf)
    sym = conf.get("log_template", "variable_symbol")
    if fn is None:
        fn = conf.get("log_template_crf", "train_filename")
    ret = []
    for lt in ld.iter_lt():
        l_train = []
        tpl = lt.ltw
        ex = ld.iter_lines(ltid = lt.ltid).next().l_w
        for w_tpl, w_ex in zip(tpl, ex):
            if w_tpl == sym:
                l_train.append((w_ex, "V"))
            else:
                l_train.append((w_tpl, "D"))
        ret.append("\n".join(["{0[0]} {0[1]} {0[1]}".format(train)
                for train in l_train]))
    with open(fn, "w") as f:
        f.write("\n\n".join(ret))


if __name__ == "__main__":
    import optparse
    usage = "usage: {0} [options] mode".format(sys.argv[0])
    op = optparse.OptionParser(usage)
    op.add_option("-c", "--config", action="store",
            dest="conf", type="string", default=config.DEFAULT_CONFIG_NAME,
            help="configuration file path")
    options, args = op.parse_args()

    conf = config.open_config(options.conf)
    lt2trainsource(conf)


