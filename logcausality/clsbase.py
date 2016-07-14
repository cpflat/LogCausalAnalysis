#!/usr/bin/env python
# coding: utf-8


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

