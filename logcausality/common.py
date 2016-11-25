#!/usr/bin/env python
# coding: utf-8

import os
import time
import datetime
import logging
#import subprocess  # for python3
import subprocess32 as subprocess  # for python2
#from collections import UserDict  # for python3
from UserDict import UserDict  # for python2


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


class SequenceKeyDict(UserDict):

    def _key(self, key):
        return tuple(sorted(list(key)))

    def __contains__(self, key):
        k = self._key(key)
        return UserDict.__contains__(self, k)

    def __getitem__(self, key):
        k = self._key(key)
        return UserDict.__getitem__(self, k)

    def __delitem__(self, key):
        k = self._key(key)
        return UserDict.__delitem__(self, k, item)

    def __setitem__(self, key, item):
        k = self._key(key)
        return UserDict.__setitem__(self, k, item)

    def get(self, key, failobj=None):
        k = self._key(key)
        return UserDict.get(self, k, failobj)

    def has_key(self, key):
        k = self._key(key)
        return UserDict.has_key(self, k)

    def pop(self, key, *args):
        k = self._key(key)
        return UserDict.pop(self, k, *args)

    def setdefault(self, key, *args, **kwargs):
        k = self._key(key)
        return UserDict.setdefault(self, k, *args, **kwargs)


class IDDict():

    def __init__(self, keyfunc = None):
        self._d_obj = {}
        self._d_id = {}
        if keyfunc is None:
            self.keyfunc = lambda x: x
        else:
            self.keyfunc = keyfunc

    def _next_id(self):
        next_id = len(self._d_obj)
        assert not self._d_obj.has_key(next_id)
        return next_id

    def add(self, obj):
        if self.exists(obj):
            return self._d_id[self.keyfunc(obj)]
        else:
            keyid = self._next_id()
            self._d_obj[keyid] = obj
            self._d_id[self.keyfunc(obj)] = keyid
            return keyid

    def exists(self, obj):
        return self._d_id.has_key(self.keyfunc(obj))

    def get(self, keyid):
        return self._d_obj[keyid]


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


def last_modified(args, latest = False):
    """Get the last modified time of a file or a set of files.

    Args:
        args (str or list[str]): Files to investigate.
        latest (Optional[bool]): If true, return the latest datetime
                of timestamps. Otherwise, return the oldest timestamp.

    Returns:
        datetime.datetime

    """
    def file_timestamp(fn):
        stat = os.stat(fn)
        t = stat.st_mtime
        return datetime.datetime.fromtimestamp(t)

    if isinstance(args, list):
        l_dt = [file_timestamp(fn) for fn in args]
        if latest:
            return max(l_dt)
        else:
            return min(l_dt)
    elif isinstance(args, str):
        return file_timestamp(args)
    else:
        raise NotImplementedError


# subprocess
def call_process(cmd):
    """Call a subprocess and handle standard outputs.
    
    Args:
        cmd (list): A sequence of command strings.
    
    Returns:
        ret (int): Return code of the subprocess.
        stdout (str)
        stderr (str)
    """

    p = subprocess.Popen(cmd, shell=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    ret = p.returncode
    return ret, stdout, stderr


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




