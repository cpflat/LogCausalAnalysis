#!/usr/bin/env python
# coding: utf-8

import re
import ipaddress


class LabelWord():

    def __init__(self):
        self._d_re = {}
        self._d_re["DIGIT"] = re.compile(r"^[0-9]+$")
        self._other = "OTHER"

    def label(self, word):
        ret = self.isipaddr(word)
        if ret is not None:
            return ret

        for k, reobj in self._d_re.iteritems():
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

