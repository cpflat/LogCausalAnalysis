#!/usr/bin/env python
# coding: utf-8

import sys
import os
import datetime
import collections
import ConfigParser


class ExtendedConfigParser(ConfigParser.SafeConfigParser):

    def __init__(self, noopterror=True, defaults=None,
            dict_type=collections.OrderedDict, allow_no_value=False):
        self._conf = ConfigParser.SafeConfigParser(defaults = defaults,
                dict_type = dict_type, allow_no_value = allow_no_value)
        self._noopt = noopterror

    def _no_option(self, err):
        if self._noopt:
            raise err
        else:
            return None

    def _call_method(self, method_name, section, name):
        method = getattr(self._conf, method_name)
        try:
            ret = method(section, name)
        except ConfigParser.NoOptionError as err:
            return self._no_option(err)
        except ValueError as err:
            return self._no_option(err)
        else:
            return ret

    def read(self, fn):
        return self._conf.read(fn)

    def get(self, section, name):
        return self._call_method(sys._getframe().f_code.co_name, section, name)

    def getint(self, section, name):
        return self._call_method(sys._getframe().f_code.co_name, section, name)

    def getfloat(self, section, name):
        return self._call_method(sys._getframe().f_code.co_name, section, name)

    def getboolean(self, section, name):
        return self._call_method(sys._getframe().f_code.co_name, section, name)

    def gettuple(self, section, name):
        try:
            ret = self._conf.get(section, name)
        except ConfigParser.NoOptionError as err:
            return self._no_option(err)
        else:
            return tuple(e.strip() for e in ret.split(",")
                    if not e.strip() == "")

    def getlist(self, section, name):
        try:
            ret = self._conf.get(section, name)
        except ConfigParser.NoOptionError as err:
            return self._no_option(err)
        else:
            return [e.strip() for e in ret.split(",")
                    if not e.strip() == ""]

    def getdt(self, section, name):
        try:
            ret = self._conf.get(section, name)
        except ConfigParser.NoOptionError as err:
            return self._no_option(err)
        else:
            return datetime.datetime.strptime(ret.strip(),
                    "%Y-%m-%d %H:%M:%S")

    def getterm(self, section, name):
        try:
            ret = self._conf.get(section, name)
        except ConfigParser.NoOptionError as err:
            return self._no_option(err)
        else:
            return tuple(datetime.datetime.strptime(e.strip(),
                    "%Y-%m-%d %H:%M:%S") for e in ret.split(","))

    def getdur(self, section, name):
        try:
            ret = self._conf.get(section, name)
        except ConfigParser.NoOptionError as err:
            return self._no_option(err)
        else:
            return str2dur(ret)


class GroupDef():

    def __init__(self, fn, default_val = None):
        self.gdict = {}
        self.rgdict = {}
        self.default = default_val
        if fn is None or fn == "":
            pass
        else:
            self.open_def(fn)

    def open_def(self, fn):
        group = None
        with open(fn, 'r'):
            for line in f:
                # ignore after comment sygnal
                line = line.strip().partition("#")[0]
                if line == "":
                    continue
                elif line[0] == "#":
                    continue
                elif line[0] == "[" and line[1] == "]":
                    group = line[1:].strip("[]")
                else:
                    if group is None:
                        raise ValueError("no group definition before value")
                    val = line 
                    self.gdict.setdefault(group, []).append(val)
                    self.rgdict.setdefault(val, []).append(group)

    def setdefault(self, group):
        self.default = group

    def groups(self):
        return self.gdict.keys()

    def values(self):
        return self.rgdict.keys()

    def ingroup(self, group, val):
        if self.rgdict(val):
            return val in self.rgdict[val]
        else:
            return False

    def get_group(self, val):
        if self.rgdict.has_key(val):
            return self.rgdict[val]
        else:
            return []

    def get_value(self, group):
        if self.gdict.has_key(group):
            return self.gdict[group]
        else:
            return []

    def iter_def(self):
        for group, l_val in self.gdict.iteritems():
            for val in l_val:
                yield group, val


def str2dur(string):
    if "s" in string:
        num = int(string.partition("s")[0])
        return datetime.timedelta(seconds = num)
    elif "m" in string:
        num = int(string.partition("m")[0])
        return datetime.timedelta(minutes = num)
    elif "h" in string:
        num = int(string.partition("h")[0])
        return datetime.timedelta(hours = num)
    elif "d" in string:
        num = int(string.partition("d")[0])
        return datetime.timedelta(days = num)
    else:
        raise ValueError("Duration string invalid")


# singleton config instance, to be shared in whole system
_fn = "config.conf"
if not os.path.exists(_fn):
    raise IOError("common configuration file {0} not found".format(_fn))
_config = ExtendedConfigParser(noopterror = False)
_config.read(_fn)

def common_config():
    return _config

