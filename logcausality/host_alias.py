#!/usr/bin/env python
# coding: utf-8

import ipaddress
from collections import defaultdict

#import common
import config

#class HostAlias(common.singleton):
class HostAlias(object):

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
        self._l_net = []
        self._open(self.fn)

    def _open(self, fn):
        group = "default"
        if fn is None or fn == "":
            return
        with open(fn, "r") as f:
            for line in f:
                line = line.rstrip("\n")
                line = line.partition("#")[0]
                if line == "" or line[0] == "#":
                    continue
                elif line[0] == "[" and "]" in line:
                    group = line.strip("[").partition("]")[0]
                elif line[0] == "<" and ">" in line:
                    l_temp = line.strip("<").partition(">")
                    alias = l_temp[0]
                    names = [alias] + l_temp[2].strip().rstrip("\n").split()
                    self._add_def(names, alias = alias, group = group)
                else:
                    names = line.rstrip("\n").split()
                    if len(names) == 0:
                        continue
                    self._add_def(names, group = group)

    def _add_def(self, l_name, alias = None, group = None):

        def add_alias(name, alias):
            if alias is not None:
                self._d_alias[alias].append(name)
                self._d_ralias[name] = alias
            else:
                self._d_alias[name].append(name)
                self._d_ralias[name] = name

        def add_groupdef(key, group):
            if group is not None:
                self._d_group[group].append(key)
                self._d_rgroup[key] = group

        for name in l_name:
            if "/" in name:
                try:
                    net = str(ipaddress.ip_network(name))
                    add_alias(net, alias)
                    add_groupdef(net, group)
                    self._l_net.append(net)
                except ValueError:
                    add_alias(name, alias)
                    add_groupdef(name, group)
            else:
                try:
                    addr = str(ipaddress.ip_address(name))
                    add_alias(addr, alias)
                    add_groupdef(addr, group)
                except ValueError:
                    add_alias(name, alias)
                    add_groupdef(name, group)

    def print_definitions(self):
        print "[aliases]"
        for k, val in self._d_alias.iteritems():
            print("<{0}> ".format(k) + " ".join([str(v) for v in val]))
        print
        print("[groups]")
        for k, val in self._d_group.iteritems():
            print(k)
            print(" ".join([str(v) for v in val]))
            print

    def isknown(self, string):
        try:
            addr = str(ipaddress.ip_address(string))
            for net in self._l_net:
                if addr in net:
                    return True
            else:
                if addr in self._d_ralias.keys():
                    return True
                else:
                    return False
        except ValueError:
            name = string.lower()
            return self._d_ralias.has_key(name)

    def resolve_host(self, string):
        try:
            addr = str(ipaddress.ip_address(string))
            for net in self._l_net:
                if addr in net:
                    return self._d_ralias[net]
            else:
                if addr in self._d_ralias.keys():
                    return self._d_ralias[addr]
                else:
                    return None
        except ValueError:
            name = string.lower()
            if self._d_ralias.has_key(name):
                return self._d_ralias[name]
            else:
                return None

    def get_group(self, string):
        try:
            addr = str(ipaddress.ip_address(string))
            for net in self._l_net:
                if addr in net:
                    return self._d_rgroup[net]
            else:
                if addr in self._d_rgroup.keys():
                    return self._d_rgroup[addr]
                else:
                    return None
        except ValueError:
            name = string.lower()
            if self._d_rgroup.has_key(name):
                return self._d_rgroup[name]
            else:
                return None


def test_hostalias(conf):
    names = ["192.168.0.1",
            "www.TEST.localdomain",
            "localhost",
            "www",
            "www3",
            "hoge",
            "10.100.1.254",
            "8.8.6.0"]
    #conf.set("database", "host_alias_filename", "host_alias_test.txt")
    ha = HostAlias(conf)
    ha.print_definitions()
    print
    print "[test aliasing]"
    for name in names:
        print name
        print ha.isknown(name)
        print ha.resolve_host(name)
        print ha.get_group(name)
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

