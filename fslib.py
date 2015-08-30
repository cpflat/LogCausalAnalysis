#!/usr/bin/env python
# coding: utf-8

import os


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
                ret += open_path("/".join((arg, fn)))
            return ret
        else:
            return [path]

    if isinstance(args, list):
        for arg in args:
            ret = open_path(arg)
    else:
        ret = open_path(args)
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

