#!/usr/bin/env python
# coding: utf-8


class HostAlias():

    def __init__(self, conf):
        self.fn = conf.get("database", "host_alias_filename")
        self.d_host = {}
        self.d_rhost = {}
        self._open(self.fn)

    def _open(self, fn):
        if fn is None:
            return
        with open(fn, "r") as f:
            for line in f:
                if line[0] == "#":
                    continue
                names = line.rstrip("\n").split()
                if len(names) <= 1:
                    continue
                group = names[0]
                self.d_host[group] = names
                for name in names:
                    self.d_rhost[name] = group

    def print_definitions(self):
        for k, v in self.d_host.iteritems():
            print k
            print " ".join(v)

    def host(self, string):
        if self.d_rhost.has_key(string):
            return self.d_rhost[string]
        else:
            return string

    def has_key(self, string):
        return self.d_rhost.has_key(string)


def test_hostalias():
    names = ["192.168.0.1",
            "www.test.localdomain",
            "192.168.0.3",
            "localhost",
            "www3"]
    #ha = HostAlias("host_alias.txt")
    ha = HostAlias(None)
    ha.print_definitions()
    for name in names:
        print name
        print ha.host(name)
        print

if __name__ == "__main__":
    test_hostalias()

