#!/usr/bin/env python
# coding: utf-8

import os
import time
import datetime
import logging


# classes

class singleton(object):

    def __new__(clsObj, *args, **kwargs):
        tmpInstance = None
        if not hasattr(clsObj, "_instanceDict"):
            clsObj._instanceDict = {}
            clsObj._instanceDict[str(hash(clsObj))] = \
                    super(singleton, clsObj).__new__(clsObj, *args, **kwargs)
            tmpInstance = clsObj._instanceDict[str(hash(clsObj))]
        elif not hasattr(clsObj._instanceDict, str(hash(clsObj))):
            clsObj._instanceDict[str(hash(clsObj))] = \
                    super(singleton, clsObj).__new__(clsObj, *args, **kwargs)
            tmpInstance = clsObj._instanceDict[str(hash(clsObj))]
        else:
            tmpInstance = clsObj._instanceDict[str(hash(clsObj))]
        return tmpInstance


# file managing

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
    return l_fn


def mkdir(path):
    if not os.path.exists(path):
        os.mkdir(path)
    elif not os.path.isdir(path):
        raise OSError("something already exists on given path, " \
                "and it is NOT a directory")
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


# parallel computing

def mthread_queueing(l_thread, pal):
    """
    Args:
        l_thread (List[threading.Thread]): A sequence of thread objects.
        func (func): Target function to execute in parallel.    
        l_args (List[args, kwargs]): A sequence of arguments for func.
        pal (int): Maximum number of threads executed at once.
    """
    l_job = []
    while len(l_thread) > 0:
        if len(l_job) < pal:
            job = l_thread.pop(0)
            job.start()
            l_job.append(job)
        else:
            time.sleep(1)
            l_job = [j for j in l_job if j.is_alive()]
    else:
        for job in l_job:
            job.join()


def mprocess_queueing(l_process, pal):
    mthread_queueing(l_process, pal)


# measurement

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




