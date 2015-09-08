#!/usr/bin/env python
# coding: utf-8

import sys
import os
import optparse

import config
import fslib
import logparser
import log_db

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
                if word in ltline.words:
                    # satisfied
                    pass
                else:
                    return False
            elif rule == "in":
                for w in ltline.words:
                    if word in w:
                        # satisfied
                        break
                else:
                    return False
            elif rule == "in_ord":
                for w in ltline.words:
                    if word.lower() in w.lower():
                        # satisfied
                        break
                else:
                    return False
            else:
                raise ValueError("Invalid rule name")
        else:
            return True

    def get_label(self, ltline):
        for label, word, rule in self.rules:
            if self._test_rule(ltline, word, rule):
                return label
        else:
            return None


def test_ltlabel(conf):

    def output(ll, ltline):
        return " ".join((str(ll.get_label(ltline)), str(ltline)))

    ldb = log_db.ldb_manager(conf)
    ldb.open_lt()
    ltconf_path = conf.get("visual", "ltlabel")
    if ltconf_path == "":
        ltconf_path = DEFAULT_LABEL_CONF
    ll = LTLabel(ltconf_path)
    d_buf = {}
    buf_none = []
    for ltline in ldb.lt.table:
        label = ll.get_label(ltline)
        if label is None:
            buf_none.append(output(ll, ltline))
        else:
            d_buf.setdefault(label, []).append(output(ll, ltline))
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
    test_ltlabel(conf)

