#!/usr/bin/env python
# coding: utf-8

"""
Note:
    Configuration defined earlier is prior.
"""


import sys
import os
import re
import optparse
import logging

import config
import strutil
import logparser
import log_db

_logger = logging.getLogger(__name__.rpartition(".")[-1])
DEFAULT_LABEL_CONF = "/".join((os.path.dirname(__file__),
        "lt_label.conf.sample"))


class LTLabel():

    group_header = "group_"
    label_header = "label_"

    def __init__(self, conf_fn, default_group = None):
        self.conf = config.ExtendedConfigParser()
        self.conf.read(conf_fn)
        self.default_group = default_group

        self.groups = []
        self.labels = []
        for sec in self.conf.sections():
            if sec[:len(self.group_header)] == self.group_header:
                self.groups.append(sec[len(self.group_header):])
            elif sec[:len(self.label_header)] == self.label_header:
                self.labels.append(sec[len(self.label_header):])

        self.d_group = {} # key : group, val : [label, ...]
        self.d_rgroup = {} # key : label, val : [group, ...]
        for group in self.groups:
            section = self.group_header + group
            for label in self.conf.gettuple(section, "members"):
                self.d_group.setdefault(group, []).append(label)
                self.d_rgroup.setdefault(label, []).append(group)
        self.rules = [] # [(label, (re_matchobj, ...)), ...]
        for label in self.labels:
            section = self.label_header + label
            for rulename in self.conf.gettuple(section, "rules"):
                l_re = []
                l_restr = self.conf.gettuple(section, rulename)
                for re_str in l_restr:
                    if rulename[0] == "i":
                        re_obj = re.compile(re_str, re.IGNORECASE)
                    elif rulename[0] == "c":
                        re_obj = re.compile(re_str, )
                    else:
                        raise ValueError(
                                "lt_label rulename invalid ({0})".format(
                                    rulename))
                    l_re.append(re_obj)
                self.rules.append((label, tuple(l_re)))

            #    l_word = self.conf.gettuple(section, rulename + "_word")
            #    l_rule = self.conf.gettuple(section, rulename + "_rule")
            #    assert len(l_word) == len(l_rule)
            #    self.rules.append((label, l_word, l_rule))

    #def _test_rule(self, ltline, l_word, l_rule):
    #    for word, rule in zip(l_word, l_rule):
    #        if rule == "equal":
    #            if word in ltline.ltw:
    #                # satisfied
    #                pass
    #            else:
    #                return False
    #        if rule == "equal_ord":
    #            if word.lower() in [w.lower() for w in ltline.ltw]:
    #                # satisfied
    #                pass
    #            else:
    #                return False
    #        elif rule == "in":
    #            for w in ltline.ltw:
    #                if word in w:
    #                    # satisfied
    #                    break
    #            else:
    #                return False
    #        elif rule == "in_ord":
    #            for w in ltline.ltw:
    #                if word.lower() in w.lower():
    #                    # satisfied
    #                    break
    #            else:
    #                return False
    #        else:
    #            raise ValueError("Invalid rule name")
    #    else:
    #        return True

    def _test_rule(self, ltline, t_re):
        for reobj in t_re:
            for word in ltline.ltw:
                if reobj.match(word):
                    break
                else:
                    pass
            else:
                # no word matches
                return False
        else:
            # all reobj passes
            return True
    
    def get_lt_label(self, ltline):
        for label, t_re in self.rules:
            if self._test_rule(ltline, t_re):
                return label
        else:
            return None

    def get_lt_group(self, ltline):
        label = self.get_lt_label(ltline)
        return self.get_group(label)

    def get_ltg_label(self, ltgid, l_ltline):
        d_score = {} # key : ruleid, value : score
        for ltline in l_ltline:
            for rid, t_rule in enumerate(self.rules):
                label, t_re = t_rule
                if self._test_rule(ltline, t_re):
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

    def get_ltg_group(self, ltgid, l_ltline):
        label = self.get_ltg_label(ltgid, l_ltline)
        return self.get_group(label)

    def get_group(self, label):
        if label is None:
            return self.default_group
        else:
            group = self.d_rgroup[label]
            assert len(group) == 1
            return group[0]


def init_ltlabel(conf):
    ltconf_path = conf.get("visual", "ltlabel")
    if ltconf_path == "":
        ltconf_path = DEFAULT_LABEL_CONF
    return LTLabel(ltconf_path)


def test_ltlabel(conf):

    def output(ld, ltgid, label, group):
        if label is None:
            label = str(label)
        return " ".join((group, label, ld.show_ltgroup(ltgid)))

    ld = log_db.LogData(conf)
    ltconf_path = conf.get("visual", "ltlabel")
    if ltconf_path == "":
        ltconf_path = DEFAULT_LABEL_CONF
    ll = LTLabel(ltconf_path)
    
    d_buf = {}
    buf_none = []
    for ltgid in ld.iter_ltgid():
        l_gid = ld.ltg_members(ltgid)
        if len(l_gid) == 1:
            #label = ll.get_lt_label(ltgid, ld.ltg_members(ltgid))
            label = ll.get_ltg_label(ltgid, ld.ltg_members(ltgid))
            group = ll.get_group(label)
        else:
            label = ll.get_ltg_label(ltgid, ld.ltg_members(ltgid))
            group = ll.get_group(label)
        if label is None:
            buf_none.append(output(ld, ltgid, str(label), group))
        else:
            d_buf.setdefault(label, []).append(output(ld, ltgid, label, group))
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

