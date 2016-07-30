#!/usr/bin/env python
# coding: utf-8

import os
import datetime
import logging


class Timer():

    def __init__(self, header, output = None):
        self.start_dt = None
        self.header = header
        self.output = output

    def _output(self, string):
        if isinstance(self.output, logging.Logger):
            self.output.info(string)
        else:
            print string

    def start(self):
        self.start_dt = datetime.datetime.now()
        self._output("{0} start".format(self.header))

    def stop(self):
        if self.start_dt is None:
            raise AssertionError("call start() before stop()")
        self.end_dt = datetime.datetime.now()
        self._output("{0} done ({1})".format(self.header,
                self.end_dt - self.start_dt))


def rep_dir(args):
    if isinstance(args, list):
        ret = []
        for arg in args:
            if os.path.isdir(arg):
                ret.extend(["/".join((arg, fn)) \
                        for fn in sorted(os.listdir(arg))])
            else:
                ret.append(arg)
        return ret
    elif isinstance(args, str):
        arg = args
        if os.path.isdir(arg):
            return ["/".join((arg, fn)) for fn in sorted(os.listdir(arg))]
        else:
            return [arg]
    else:
        raise NotImplementedError


def recur_dir(args):

    def open_path(path):
        if os.path.isdir(path):
            ret = []
            for fn in sorted(os.listdir(path)):
                ret += open_path("/".join((path, fn)))
            return ret
        else:
            return [path]

    if isinstance(args, list):
        l_fn = []
        for arg in args:
            l_fn += open_path(arg)
    elif isinstance(args, str):
        l_fn = open_path(args)
    else:
        raise NotImplementedError
    return ret


def mkdir(path):
    if not os.path.exists(path):
        os.mkdir(path)
    elif not os.path.isdir(path):
        raise OSError("something already exists on given path, " \
                "and it is NOT directory")
    else:
        pass


def rm(path):
    if os.path.exists(path):
        os.remove(path)
        return True
    else:
        return False


def rm_dirchild(dirpath):
    for fpath in rep_dir(dirpath):
        os.remove(fpath)

