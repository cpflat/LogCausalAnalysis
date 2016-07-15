#!/usr/bin/env python
# coding: utf-8

import ipaddress
from collections import defaultdict

import config
import clsbase

class HostAlias(clsbase.singleton):

    """
    Note:
        1 host can not belong to multiple host groups, because
        group definition is used to label and classify variables
        in log templates.
    """

    def __init__(self, conf):
        self.fn = conf.get("database", "host_alias_filename")
        self._d_alias = defaultdict(list) # key = alias, val = List[host]
        self._d_ralias = {} # key = host, val = alias
        self._d_group = defaultdict(list) # key = group, val = List[host]
        self._d_rgroup = {} # key = host, val = group
        self._open(self.fn)

    def _open(self, fn):
        group = "default"
        if fn is None or fn == "":
            return
        with open(fn, "r") as f:
            for line in f:
                line = line.rstrip("\n")
                if line == "" or line[0] == "#":
                    continue
                elif line[0] == "[" and "]" in line:
                    group = line.strip("[").partition("]")[0]
                elif line[0] == "<" and ">" in line:
                    names = line.rstrip("\n").split()
                    if len(names) <= 1:
                        continue
                    alias = names[0]
                    self._d_alias[alias] = names[1:]
                    self._d_group[group] += names[1:]
                    for name in names[1:]:
                        self._d_ralias[name] = alias
                        self._d_rgroup[name] = group
                else:
                    names = line.rstrip("\n").split()
                    if len(names) == 0:
                        continue
                    self._d_group[group] += names
                    for name in names:
                        self._d_rgroup[name] = group

    def print_definitions(self):
        print "[aliases]"
        for k, v in self._d_alias.iteritems():
            print k
            print " ".join(v)
        print
        print "[groups]"
        for k, v in self._d_group.iteritems():
            print k
            print " ".join(v)
            print

    def resolve_host(self, string):
        if self._d_ralias.has_key(string):
            return self._d_ralias[string]
        else:
            return string

    def get_group(self, string):
        if self._d_rgroup.has_key(string):
            return self._d_rgroup[string]
        else:
            return string

    def has_key(self, string):
        return self._d_ralias.has_key(string)


def test_hostalias(conf):
    names = ["192.168.0.1",
            "www.test.localdomain",
            "192.168.0.3",
            "localhost",
            "www3"]
    #ha = HostAlias("host_alias.txt")
    ha = HostAlias(conf)
    ha.print_definitions()
    print
    print "[test aliasing]"
    for name in names:
        print name
        print ha.resolve_host(name)
        print


if __name__ == "__main__":
    usage = ""
    import optparse
    op = optparse.OptionParser(usage)
    op.add_option("-c", "--config", action="store",
            dest="conf", type="string", default=config.DEFAULT_CONFIG_NAME,
            help="configuration file path")
    options, args = op.parse_args()
    conf = config.open_config(options.conf)
    test_hostalias(conf)

