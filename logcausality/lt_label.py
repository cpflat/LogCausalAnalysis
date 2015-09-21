#!/usr/bin/env python
# coding: utf-8

import sys
import os
import optparse
import logging

import config
import fslib
import logparser
import log_db

_logger = logging.getLogger(__name__)
DEFAULT_LABEL_CONF = "/".join((os.path.dirname(__file__),
        "lt_label.conf.sample"))


class LTLabel():

    group_header = "group_"
    label_header = "label_"

    def __init__(self, conf_fn):
        self.conf = config.ExtendedConfigParser(noopterror = False)
        self.conf.read(conf_fn)

        self.groups = []
        self.labels = []
        for sec in self.conf.sections():
            if sec[:len(self.group_header)] == self.group_header:
                self.groups.append(sec[len(self.group_header):])
            elif sec[:len(self.label_header)] == self.label_header:
                self.labels.append(sec[len(self.label_header):])
        self.d_group = {}
        # TODO reversed dict
        for group in self.groups:
            section = self.group_header + group
            for label in self.conf.gettuple(section, "members"):
                self.d_group.setdefault(group, []).append(label)
        self.rules = []
        for label in self.labels:
            section = self.label_header + label
            for rulename in self.conf.gettuple(section, "rules"):
                l_word = self.conf.gettuple(section, rulename + "_word")
                l_rule = self.conf.gettuple(section, rulename + "_rule")
                assert len(l_word) == len(l_rule)
                self.rules.append((label, l_word, l_rule))

    def _test_rule(self, ltline, l_word, l_rule):
        for word, rule in zip(l_word, l_rule):
            if rule == "equal":
                if word in ltline.ltw:
                    # satisfied
                    pass
                else:
                    return False
            elif rule == "in":
                for w in ltline.ltw:
                    if word in w:
                        # satisfied
                        break
                else:
                    return False
            elif rule == "in_ord":
                for w in ltline.ltw:
                    if word.lower() in w.lower():
                        # satisfied
                        break
                else:
                    return False
            else:
                raise ValueError("Invalid rule name")
        else:
            return True

    def get_lt_label(self, ltline):
        for label, word, rule in self.rules:
            if self._test_rule(ltline, word, rule):
                return label
        else:
            return None

    def get_ltg_label(self, ltgid, l_ltline):
        d_score = {} # key : ruleid, value : score
        for ltline in l_ltline:
            for rid, t_rule in enumerate(self.rules):
                label, word, rule = t_rule
                if self._test_rule(ltline, word, rule):
                    d_score[rid] = d_score.get(rid, 0) + 1
        l_cand = []
        max_score = 0
        for rid, score in d_score.items():
            if score > max_score:
                max_score = score
                l_cand = [rid]
            elif score == max_score:
                l_cand.append(rid)
        if len(l_cand) > 1:
            _logger.info("multiple label for ltgid {0} : {1}".format(\
                    ltgid, [self.rules[rid][0] for rid in l_cand]))
            return self.rules[l_cand[0]][0]
        elif len(l_cand) == 1:
            return self.rules[l_cand[0]][0]
        else:
            return None


def test_ltlabel(conf):

    def output(ld, ltgid, label):
        return " ".join((label, ld.show_ltgroup(ltgid)))

    ld = log_db.LogData(conf)
    ltconf_path = conf.get("visual", "ltlabel")
    if ltconf_path == "":
        ltconf_path = DEFAULT_LABEL_CONF
    ll = LTLabel(ltconf_path)
    
    d_buf = {}
    buf_none = []
    for ltgid in ld.iter_ltgid():
        label = ll.get_ltg_label(ltgid, ld.ltg_members(ltgid))
        if label is None:
            buf_none.append(output(ld, ltgid, str(label)))
        else:
            d_buf.setdefault(label, []).append(output(ld, ltgid, label))
    for k, buf in sorted(d_buf.iteritems()):
        print "\n".join(buf)
        print
    print "\n".join(buf_none)


if __name__ == "__main__":
    usage = "usage: {0} [options]".format(sys.argv[0])
    op = optparse.OptionParser(usage)
    op.add_option("-c", "--config", action="store",
            dest="conf", type="string", default=config.DEFAULT_CONFIG_NAME,
            help="configuration file path")
    options, args = op.parse_args()
    conf = config.open_config(options.conf)
    config.set_common_logging(conf, _logger, [])
    test_ltlabel(conf)

