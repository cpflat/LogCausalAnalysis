#!/usr/bin/env python
# coding: utf-8


import timelabel

class NodeStat():

    def __init__(self, nid, l_nodevalue, maxval=2):
        self.nid = nid
        self.l_elem = l_nodevalue
        self.maxval = maxval # if maxval == 2: val is binary data

    def __iter__(self):
        raise NotImplementedError

    def get_values(self):
        raise NotImplementedError


class NodeValue():

    def __init__(self, key, value):
        self.key = key
        self.val = value  # val : discrete integer, val < maxval


class EventSequence(NodeStat):

    def __init__(self, nid, top_dt, end_dt, dur, maxval=2, default=0):
        self.nid = nid
        self.maxval = maxval
        self.ts = timelabel.TimeSeries(top_dt, end_dt, dur, default)
        self.tsflag = False

        self.l_elem = []

    def __iter__(self):
        return self.ts.__iter__()

    def _add(self, ev):
        self.l_elem.append(ev)

    def add_event(self, arg):
        if isinstance(arg, list):
            for e in arg:
                self._add(ev)
        else:
            self._add(arg)
        self.tsflag = False

    def get_values(self):
        if not self.tsflag:
            self.ts.setdata([(e.key, e.val) for e in self.l_elem])
            self.tsflag = True
        return [v for v in self.ts] 


class Event(NodeValue):

    def __init__(self, dt, value):
        self.key = dt
        self.val = value
        self.args = None


