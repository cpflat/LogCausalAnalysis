#!/usr/bin/env python
# coding: utf-8

import re
import ipaddress

import host_alias


class LabelWord():

    def __init__(self, conf):
        self._d_re = {}

        self._d_re["DIGIT"] = [re.compile(r"^\d+$")]
        self._d_re["DATE"] = [re.compile(r"^\d{2}/\d{2}$"),
                              re.compile(r"^\d{4}-\d{2}-\d{2}")]
        self._d_re["TIME"] = [re.compile(r"^\d{2}:\d{2}:\d{2}$")]

        self._other = "OTHER"
        self._ha = host_alias.HostAlias(conf)
        self._host = "HOST"

    def label(self, word):
        ret = self.isipaddr(word)
        if ret is not None:
            return ret

        if self._ha.isknown(word):
            return self._host
        
        for k, l_reobj in self._d_re.iteritems():
            for reobj in l_reobj:
                if reobj.match(word):
                    return k
        
        return self._other

    @staticmethod
    def isipaddr(word):
        try:
            ret = ipaddress.ip_address(unicode(word))
            if isinstance(ret, ipaddress.IPv4Address):
                return "IPv4ADDR"
            elif isinstance(ret, ipaddress.IPv6Address):
                return "IPv6ADDR"
            else:
                raise TypeError("ip_address returns unknown type? {0}".format(
                        str(ret)))
        except ValueError:
            return None


def test_label():
    lwobj = LabelWord()
    l_w = ["hoge", "hige", "1234", "[345]", "123.4.5.67", "8.8.8.8", "::2"]
    for w in l_w:
        print w, lwobj.label(w)


if __name__ == "__main__":
    test_label()

