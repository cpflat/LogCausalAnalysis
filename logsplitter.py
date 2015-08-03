#!/usr/bin/env python
# coding: utf-8

import sys
import re

import config

L_RE = [
        re.compile(r"^\d{2}:\d{2}:\d{2}(\.\d+)?$"),
        re.compile(r"^[0-9a-fA-F]{2}:[0-9a-fA-F]{2}:[0-9a-fA-F]{2}:[0-9a-fA-F]{2}:[0-9a-fA-F]{2}:[0-9a-fA-F]{2}$"),
        re.compile(r"^((([0-9a-fA-F]{1,4}:){7}([0-9a-fA-F]{1,4}|:))|(([0-9a-fA-F]{1,4}:){6}(:[0-9a-fA-F]{1,4}|((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3})|:))|(([0-9a-fA-F]{1,4}:){5}(((:[0-9a-fA-F]{1,4}){1,2})|:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3})|:))|(([0-9a-fA-F]{1,4}:){4}(((:[0-9a-fA-F]{1,4}){1,3})|((:[0-9a-fA-F]{1,4})?:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}))|:))|(([0-9a-fA-F]{1,4}:){3}(((:[0-9a-fA-F]{1,4}){1,4})|((:[0-9a-fA-F]{1,4}){0,2}:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}))|:))|(([0-9a-fA-F]{1,4}:){2}(((:[0-9a-fA-F]{1,4}){1,5})|((:[0-9a-fA-F]{1,4}){0,3}:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}))|:))|(([0-9a-fA-F]{1,4}:){1}(((:[0-9a-fA-F]{1,4}){1,6})|((:[0-9a-fA-F]{1,4}){0,4}:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}))|:))|(:(((:[0-9a-fA-F]{1,4}){1,7})|((:[0-9a-fA-F]{1,4}){0,5}:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}))|:)))$")
        ]
_config = config.common_config()

class LogSplitter():

    symdef = _config.get("log_template", "sym_filename")

    def __init__(self):
        self.spl = []  # splitter
        self.cspl = []  # conditional splitter
        self._init_symbol()

    def _init_symbol(self, fn = None):
        buf = []
        if fn is None:
            fn = self.symdef
        with open(fn, "r") as f:
            for line in f:
                buf.append(line.rstrip("\n").split("0"))
        self.spl = buf[0]
        self.cspl = buf[1]

    def _re_cspl(self, string):
        # judge string is a variable or complex word to be parted
        for restr in L_RE:
            if restr.match(string):
                return True
        else:
            return False

    def part(self, string):
        for cnt, c in enumerate(string):
            if c in self.spl:
                return string[0:cnt], string[cnt], string[cnt+1:]
        # partition in reversed order if string is not one variable
        for cnt in reversed(range(len(string))):
            if string[cnt] in self.cspl:
                if self._re_cspl(string):
                    return string, None, None
                else:
                    return string[0:cnt], string[cnt], string[cnt+1:]
        return string, None, None


LS = LogSplitter()


def split_word(string):
    w1, s, w2 = LS.part(string)
    if w2 is None:
        assert s is None
        return [(w1, 'w')]
    else:
        ret1 = split_word(w1)
        ret2 = split_word(w2)
        return ret1 + [(s, 's')] + ret2


def merge_sym(l_elem):
    l_w = []
    l_s = []
    temp = []
    for e in l_elem:
        if e[1] == 'w':
            if len(e[0]) > 0:
                l_s.append("".join(temp))
                temp = []
                l_w.append(e[0])
        elif e[1] == 's':
            temp.append(e[0])
        else:
            raise ValueError
    else:
        l_s.append("".join(temp))
    assert len(l_w) + 1 == len(l_s)
    return l_w, l_s


def split(line):
    return split_message(line)


def split_message(line):
    ret = split_word(line.rstrip("\n"))
    return merge_sym(ret) 


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit("usage: {0} fn".format(sys.argv[0]))
    with open(sys.argv[1], 'r') as f:
        for line in f:
            l_w, l_s = split_message(line.rstrip("\n"))
            org = line.rstrip("\n")
            rec =  "".join([s + w for w, s in zip(l_w + [""], l_s)])
            assert org == rec
            print org
            print l_w, l_s
            print

