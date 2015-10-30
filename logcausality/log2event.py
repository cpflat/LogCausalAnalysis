#!/usr/bin/env python
# coding: utf-8


import config
import log_db
import nodestat
import ltfilter


class LogEventIDMap():

    def __init__(self):
        self.eidlen = 0
        self.emap = {} # key : eid, val : info
        self.ermap = {} # key : info, val : eid

    def __len__(self):
        return self.eidlen

    def _info(self, line):
        return (line.lt.ltgid, line.host)

    def eid(self, line):
        info = self._info(line)
        if self.ermap.has_key(info):
            return self.ermap[info]
        else:
            eid = self.eidlen
            self.eidlen += 1
            self.emap[eid] = info
            self.ermap[info] = eid
            return eid

    def has_eid(self, eid):
        return self.emap.has_key(eid)

    def info(self, eid):
        return self.emap[eid]

    def get_eid(self, info):
        return self.ermap[info]

    def rearrange(self, l_eid):
        import copy
        assert len(l_eid) == self.eidlen
        emap = copy.deepcopy(self.emap)
        ermap = copy.deepcopy(self.ermap)
        self.emap = {}
        self.ermap = {}
        for new_eid, old_eid in enumerate(l_eid):
            old_info = emap[old_eid]
            self.emap[new_eid] = old_info 
            self.ermap[old_info] = new_eid


def log2event(conf, top_dt, end_dt, dur, area):
    ld = log_db.LogData(conf)
    ltf = ltfilter.IDFilter(conf.getlist("dag", "use_filter"))
    evmap = LogEventIDMap()
    edict = {} # key : eid, val : nodestat.EventSequence

    if area == "all":
        iterobj = ld.iter_lines(top_dt = top_dt, end_dt = end_dt)
    elif area[:5] == "host_":
        host = area[5:]
        iterobj = ld.iter_lines(top_dt = top_dt, end_dt = end_dt, host = host)
    else:
        iterobj = ld.iter_lines(top_dt = top_dt, end_dt = end_dt, area = area)

    for line in iterobj:
        if not ltf.isremoved(line.lt.ltid):
            ev = nodestat.Event(line.dt, 1)
            eid = evmap.eid(line)
            ev.key = line.dt
            ev.val = 1
            if not edict.has_key(eid):
                edict[eid] = nodestat.EventSequence(eid, \
                        top_dt, end_dt, dur, maxval=2, default=0)
            edict[eid].add_event(ev)
    return edict, evmap

