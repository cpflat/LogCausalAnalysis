#!/usr/bin/env python
# coding: utf-8

import sys
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


#class ExtendedConfigParser(ConfigParser.SafeConfigParser):
#
#    def __init__(self, defaults=None,
#            dict_type=collections.OrderedDict, allow_no_value=False):
#        ConfigParser.SafeConfigParser.__init__(self, defaults = defaults,
#                dict_type = dict_type, allow_no_value = allow_no_value)
#
#    def get(self, section, name):
#        try:
#            ConfigParser.SafeConfigParser.get(section, name)
#
#
#    def gettuple(self, section, name):
#        return tuple(e.strip() for e in self.get(section, name).split(",")
#                if not e.strip() == "")
#
#    def getlist(self, section, name):
#        return [e.strip() for e in self.get(section, name).split(",")
#                if not e.strip() == ""]
#
#    def getdt(self, section, name):
#        return datetime.datetime.strptime(self.get(section, name).strip(),
#                "%Y-%m-%d %H:%M:%S")
#
#    def getterm(self, section, name):
#        return tuple(datetime.datetime.strptime(
#                e.strip(), "%Y-%m-%d %H:%M:%S")
#                for e in self.get(section, name).split(","))
#
#    def getdur(self, section, name):
#        return str2dur(self.get(section, name))


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


_fn = "config.conf"
_config = ExtendedConfigParser()
_config.read(_fn)

def common_config():
    return _config

