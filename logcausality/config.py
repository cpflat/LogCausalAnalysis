#!/usr/bin/env python
# coding: utf-8

import sys
import os
import datetime
import logging
import collections
import ConfigParser

DEFAULT_CONFIG_NAME = "/".join((os.path.dirname(__file__),
        "config.conf.sample"))
LOGGER_NAME = __name__.rpartition(".")[-1]
_logger = logging.getLogger(LOGGER_NAME)
_ch = logging.StreamHandler()
_ch.setLevel(logging.WARNING)
_logger.setLevel(logging.WARNING)
_logger.addHandler(_ch)


class ExtendedConfigParser(ConfigParser.SafeConfigParser, object):
    
    def __init__(self):
        
        #super(ExtendedConfigParser, self).__init__(self)
        ConfigParser.SafeConfigParser.__init__(self)
        self.filename = None # source filename

    def read(self, fn):
        if not os.path.exists(fn):
            raise IOError("{0} not found".format(fn))
        else:
            self.filename = fn
            return super(ExtendedConfigParser, self).read(fn)

    def merge(self, conf, warn = False):

        def opt_warn(section, option, fn):
            _logger.warning("""
Invalid option name {0} in section {1} in {2}, ignored
                    """.strip().format(option, section, fn))

        def sec_warn(section, fn):
            _logger.warning("Invalid section name {0} in {1}".format(
                    section, fn))
        
        for section in conf.sections():
            if section in self.sections():
                for option in conf.options(section):
                    value = conf.get(section, option)
                    if option in self.options(section):
                        self.set(section, option, value)
                    else:
                        if warn:
                            opt_warn(section, option, conf.filename)
            else:
                if warn:
                    sec_warn(section, conf.filename)
        return self

    def gettuple(self, section, name):
        ret = self.get(section, name)
        return tuple(e.strip() for e in ret.split(",")
                if not e.strip() == "")

    def getlist(self, section, name):
        ret = self._conf.get(section, name)
        if ret == "":
            return None
        else:
            return [e.strip() for e in ret.split(",")
                    if not e.strip() == ""]

    def getdt(self, section, name):
        ret = self._conf.get(section, name)
        if ret == "":
            return None
        else:
            return datetime.datetime.strptime(ret.strip(),
                    "%Y-%m-%d %H:%M:%S")

    def getterm(self, section, name):
        ret = self._conf.get(section, name)
        if ret == "":
            return None
        else:
            return tuple(datetime.datetime.strptime(e.strip(),
                    "%Y-%m-%d %H:%M:%S") for e in ret.split(","))

    def getdur(self, section, name):
        ret = self._conf.get(section, name)
        if ret == "":
            return None
        else:
            return str2dur(ret)


class GroupDef():

    """
    Define grouping by external text
    Rules:
        description after # in a line will be recognized as comment
        line "[GROUP_NAME]" will change group to set
        other lines add elements in group set with GROUP_NAME line
    """

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
        with open(fn, 'r') as f:
            for line in f:
                # ignore after comment sygnal
                line = line.strip().partition("#")[0]
                if line == "":
                    continue
                elif line[0] == "#":
                    continue
                elif line[0] == "[" and line[-1] == "]":
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
    elif "w" in string:
        num = int(string.partition("w")[0])
        return datetime.timedelta(days = num)
    else:
        raise ValueError("Duration string invalid")


def open_config(fn = None):
    conf = ExtendedConfigParser()
    conf.read(DEFAULT_CONFIG_NAME)
    if fn is not None:
        conf2 = ExtendedConfigParser()
        conf2.read(fn)
        conf.merge(conf2, warn = True)
    return conf


# common objects for logging
def set_common_logging(conf, logger = None, l_logger_name = []):
    fn = conf.get("general", "info_log")
    fmt = logging.Formatter(
            fmt = "%(asctime)s %(levelname)s (%(threadName)s) %(message)s",
            datefmt = "%Y-%m-%d %H:%M:%S")
    lv = logging.INFO
    if fn is None:
        ch = logging.StreamHandler()
    else:
        ch = logging.FileHandler(fn)
    ch.setFormatter(fmt)
    ch.setLevel(lv)
    if logger is not None:
        logger.setLevel(lv)
        logger.addHandler(ch)
    for ln in l_logger_name:
        temp = logging.getLogger(ln)
        temp.setLevel(lv)
        temp.addHandler(ch)
    return ch


def release_common_logging(ch, logger = None, l_logger_name = None):
    if logger is not None:
        logger.removeHandler(ch)
    if l_logger_name is not None:
        for ln in l_logger_name:
            temp = logging.getLogger(ln)
            temp.removeHandler(ch)


#def overwrite_config(conf_fn1, conf_fn2, output):
#    conf1 = open_config(conf_fn1) # conf1 as basis
#    conf2 = open_config(conf_fn2) # overwrite with conf2
#    for section in conf2.sections():
#        if section in conf1.sections():
#            for option in conf2.options(section):
#                value = conf2.get(section, option)
#                if option in conf1.options(section):
#                    conf1.set(section, option, value)
#    with open(output, 'w') as f:
#        conf1.write(f)


def test_config(conf_name):
    conf = open_config(conf_name)
    pass


if __name__ == "__main__":
    # import old config, merge it with new default config, and output
    if len(sys.argv) < 2:
        sys.exit("usage : {0} config".format(sys.argv[0]))
    #default_conf = DEFAULT_CONFIG_NAME
    conf = sys.argv[1]
    test_config(conf)
    #output = sys.argv[2]
    #overwrite_config(default_conf, conf, output) 

