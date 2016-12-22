#!/usr/bin/env python
# coding: utf-8

import sys
import datetime
import copy
import logging
from collections import namedtuple

import config
import dtutil
import log_db
import evfilter

_logger = logging.getLogger(__name__.rpartition(".")[-1])
EvDef = namedtuple("EvDef", ["type", "note", "gid", "host"])


class EventDefinitionMap():
    """Before analyze system log messages, we need to classify them with
    multiple criterions like log template IDs and hostnames.
    This class defines classified groups as "Event", and provide
    interconvirsion functions between Event IDs and their
    classifying criterions.
    
    This class allows 2 types of classifying criterion combinations.
        ltgid-host: Use log template grouping IDs and hostnames.
        ltid-host: Use log template IDs and hostnames.
    
    In addition, their can be virtual Events, not a group of log messages
    but a set of symptoms found in the data sequence of log messages.
    This class allows 2 types of virtual Events.
        periodic_top: The start of periodic appearance of an Event.
        periodic_end: The end of periodic appearance of an Event.

    The definition of Event is saved as a nametuple EvDef.
    Evdef has following attributes.
        type (int): An event type identifier.
            0: Normal event that comes from a raw log message appearance.
            1: periodic_top event.
            2: periodic_end event.
        note (any): Some paramaters to show the event characteristics.
            In the case of periodic_*, this attributes requires the interval
            of periodic appearance (seconds(int)) of log messages.
        gid (int): 
        host (str):

    Attributes:
        type_normal (int): 0.
        type_periodic_top (int): 1.
        type_periodic_end (int): 2.
        event_gid (str): A string to assign classifying criterion
            of log messages. 1 of [ltgid, ltid].
    """
    type_normal = 0
    type_periodic_top = 1
    type_periodic_end = 2
    type_periodic_remainder = 3

    def __init__(self, top_dt, end_dt, gid_name = "ltgid"):
        """
        Args:
            event_gid (str): A string to assign classifying criterion
                of log messages. 1 of [ltgid, ltid].
        """
        self.top_dt = top_dt
        self.end_dt = end_dt
        assert gid_name in ("ltid", "ltgid")
        self.gid_name = gid_name
        self.l_attr = ["gid", "host"]

        self._emap = {} # key : eid, val : evdef
        self._ermap = {} # key : evdef, val : eid

    def __len__(self):
        return len(self._emap)

    def _eids(self):
        return self._emap.keys()

    def _next_eid(self):
        eid = len(self._emap)
        while self._emap.has_key(eid):
            eid += 1
        else:
            return eid

    def generate(self, ld, top_dt = None, end_dt = None):
        l = len(self)
        if self.gid_name == "ltid":
            iterobj = ld.whole_host_lt(top_dt, end_dt)
        elif self.gid_name == "ltgid":
            iterobj = ld.whole_host_ltg(top_dt, end_dt)
        else:
            raise NotImplementedError
        for host, gid in iterobj:
            d = {"type" : self.type_normal,
                 "note" : None,
                 "gid" : gid,
                 "host" : host}
            evdef = EvDef(**d)

            if self._ermap.has_key(evdef):
                pass
            else:
                eid = self._next_eid()
                self._emap[eid] = evdef
                self._ermap[evdef] = eid
        return len(self) - l

    def process_line(self, line):
        gid = line.get(self.gid_name)
        d = {"type" : self.type_normal,
             "note" : None,
             "gid" : gid,
             "host" : line.host}
        evdef = EvDef(**d)

        if self._ermap.has_key(evdef):
            return self._ermap[evdef]
        else:
            eid = self._next_eid()
            self._emap[eid] = evdef
            self._ermap[evdef] = eid
            return eid

    def add_virtual_event(self, info, type_id, note):
        d = {"type" : type_id,
             "note" : note,
             "gid" : info.gid,
             "host" : info.host}
        evdef = EvDef(**d)
 
        eid = self._next_eid()
        self._emap[eid] = evdef
        self._ermap[evdef] = eid
        return eid

    def update_event(self, eid, info, type_id, note):
        d = {"type" : type_id,
             "note" : note,
             "gid" : info.gid,
             "host" : info.host}
        evdef = EvDef(**d)

        self._emap[eid] = evdef
        self._ermap[evdef] = eid
        return eid

    def has_eid(self, eid):
        return self._emap.has_key(eid)

    def has_info(self, info):
        return self._ermap.has_key(info)

    def info(self, eid):
        return self._emap[eid]

    def info_dict(self, eid):
        info = self._emap[eid]
        return {key : getattr(info, key) for key in info._fields}

    def info_str(self, eid):
        info = self._emap[eid]
        string = ", ".join(["{0}={1}".format(key, getattr(info, key))
                for key in self.l_attr])
        
        if info.type == self.type_normal:
            return "[{0}]".format(string)
        elif info.type == self.type_periodic_top:
            return "start[{0}]({1}sec)".format(string, info.note)
        elif info.type == self.type_periodic_end:
            return "end[{0}]({1}sec)".format(string, info.note)
        elif info.type == self.type_periodic_remainder:
            return "remain[{0}]({1}sec)".format(string, info.note)
        else:
            # NotImplemented
            return "({0})".format(string)

    def info_repr(self, ld, eid, limit = 5):
        info = self._emap[eid]
        d = {"head" : limit, "foot" : limit,
                "top_dt" : self.top_dt, "end_dt" : self.end_dt}
        d[self.gid_name] = info.gid
        d["host"] = info.host
        return ld.show_log_repr(**d)

    def iterline_args(self, eid, top_dt = None, end_dt = None, area = None):
        evdef = self.info(eid)
        d = {"top_dt" : top_dt,
             "end_dt" : end_dt,
             "host" : evdef.host,
             "area" : area}
        d[self.gid_name] = evdef.gid
        return d

    def get_eid(self, info):
        return self._ermap[info]

    def iter_eid(self):
        return self._emap.iterkeys()

    def iter_evdef(self):
        return self._ermap.iterkeys()

    def pop(self, eid):
        evdef = self._emap.pop(eid)
        self._ermap.pop(evdef)
        return evdef

    def move_eid(self, old_eid, new_eid):
        evdef = self.pop(old_eid)
        self._emap[new_eid] = evdef
        self._ermap[evdef] = new_eid

    def rearrange(self, l_eid):
        emap = copy.deepcopy(self._emap)
        ermap = copy.deepcopy(self._ermap)
        self._emap = {}
        self._ermap = {}
        for new_eid, old_eid in enumerate(l_eid):
            old_evdef = emap[old_eid]
            self.emap[new_eid] = old_evdef
            self.ermap[old_evdef] = new_eid


def _copy_evmap(evmap):
    new_evmap = EventDefinitionMap(evmap.top_dt, evmap.end_dt,
            evmap.gid_name)
    new_evmap._emap = copy.deepcopy(evmap._emap)
    new_evmap._ermap = copy.deepcopy(evmap._ermap)
    return new_evmap


def log2event(conf, ld, top_dt, end_dt, area):
    gid_name = conf.get("dag", "event_gid")
    evmap = EventDefinitionMap(top_dt, end_dt, gid_name)
    edict = {} # key : eid, val : list(datetime.datetime)

    iterobj = ld.iter_lines(top_dt = top_dt, end_dt = end_dt, area = area)
    for line in iterobj:
        eid = evmap.process_line(line)
        edict.setdefault(eid, []).append(line.dt)

    return edict, evmap


def generate_evmap(conf, ld, top_dt, end_dt):
    gid_name = conf.get("dag", "event_gid")
    evmap = EventDefinitionMap(top_dt, end_dt, gid_name)
    evmap.generate(ld, top_dt, end_dt)
    return evmap


def filter_edict(conf, edict, evmap, ld, top_dt, end_dt, area):
    l_result = evfilter.periodic_events(conf, ld, top_dt, end_dt, area,
            edict, evmap)

    temp_edict = copy.deepcopy(edict)
    temp_evmap = _copy_evmap(evmap)
    for eid, interval in l_result:
        temp_edict.pop(eid)
        temp_evmap.pop(eid)
    return _remap_eid(temp_edict, temp_evmap)


def replace_edict(conf, edict, evmap, ld, top_dt, end_dt, area):
    
    def _across_term_top(l_dt, pe, top_dt, dur, err):
        err_dur = datetime.timedelta(seconds = int(dur.total_seconds() * err))
        top_pe = min(pe)
        if top_pe == min(l_dt):
            if top_pe < top_dt + (dur + err_dur):
                return True
        else:
            return False

    def _across_term_end(l_dt, pe, end_dt, dur, err):
        err_dur = datetime.timedelta(seconds = int(dur.total_seconds() * err))
        end_pe = max(pe)
        if end_pe == max(l_dt):
            if end_pe > end_dt - (dur + err_dur):
                return True
        else:
            return False

    l_result = evfilter.periodic_events(conf, ld, top_dt, end_dt, area,
            edict, evmap)

    err = conf.getfloat("filter", "seq_error")
    dup = conf.getboolean("filter", "seq_duplication") 
    pcnt = conf.getint("filter", "periodic_count") 
    pterm = conf.getdur("filter", "periodic_term") 
    repl_top = conf.getboolean("filter", "replace_top")
    repl_end = conf.getboolean("filter", "replace_end")

    temp_edict = copy.deepcopy(edict)
    temp_evmap = _copy_evmap(evmap)
    for eid, interval in l_result:
        l_dt = edict[eid]
        if dup:
            l_pe, npe = dtutil.separate_periodic_dup(l_dt, interval, err)
        else:
            l_pe, npe = dtutil.separate_periodic(l_dt, interval, err)
        _logger.info("Event {0} ({1}) -> pseq * {2} ({3}) + {4}".format(
                eid, len(l_dt), len(l_pe),
                [len(pe) for pe in l_pe], len(npe)))

        l_remain = []
        l_top = []
        l_end = []
        for pe in l_pe:
            if len(pe) < pcnt:
                l_remain += pe
            elif repl_top and \
                    not _across_term_top(l_dt, pe, top_dt, interval, err):
                l_top.append(min(pe))
            elif repl_end and \
                    not _across_term_end(l_dt, pe, end_dt, interval, err):
                l_end.append(max(pe))
            else:
                l_remain += pe
        l_remain += npe

        if len(l_remain) == len(l_dt):
            pass
        elif len(l_remain) > 0:
            temp_evmap.update_event(eid, evmap.info(eid),
                    EventDefinitionMap.type_periodic_remainder,
                    int(interval.total_seconds()))
            temp_edict[eid] = sorted(l_remain)
            _logger.info(
                    "periodic event {0} ({1}) is filtered".format(
                    eid, evmap.info_str(eid)) + \
                    " and left (-> {0})".format(temp_evmap.info_str(eid)))
        else:
            temp_edict.pop(eid)
            temp_evmap.pop(eid)
            _logger.info("periodic event {0} ({1}) is removed ".format(
                    eid, evmap.info_str(eid)))
        if len(l_top) > 0:
            new_eid = temp_evmap.add_virtual_event(evmap.info(eid),
                    EventDefinitionMap.type_periodic_top,
                    int(interval.total_seconds()))
            temp_edict[new_eid] = sorted(l_top)
            _logger.info("virtual event {0} ({1}) added".format(
                    new_eid, temp_evmap.info_str(new_eid)) + \
                    " from event {0} ({1})".format(
                    eid, evmap.info_str(eid)))
        if len(l_end) > 0:
            new_eid = temp_evmap.add_virtual_event(evmap.info(eid),
                    EventDefinitionMap.type_periodic_end,
                    int(interval.total_seconds()))
            temp_edict[new_eid] = sorted(l_end)
            _logger.info("virtual event {0} ({1}) added".format(
                    new_eid, temp_evmap.info_str(new_eid)) + \
                    " from event {0} ({1})".format(
                    eid, evmap.info_str(eid)))
    
    #return _remap_eid(temp_edict, temp_evmap)
    return temp_edict, temp_evmap


def _remap_eid(edict, evmap):
    new_eid = 0
    for old_eid in edict.keys():
        if old_eid == new_eid:
            new_eid += 1
        else:
            temp = edict[old_eid]
            edict.pop(old_eid)
            edict[new_eid] = temp
            evmap.move_eid(old_eid, new_eid)
            new_eid += 1

    return edict, evmap


def event2stat(edict, top_dt, end_dt, dur, binarize = True):
    d_stat = {}
    l_label = dtutil.label(top_dt, end_dt, dur)

    for eid, l_ev in edict.iteritems():
        d_stat[eid] = dtutil.discretize(l_ev, l_label, binarize)
    return d_stat


def test_log2event(conf):
    import pc_log
    ld = log_db.LogData(conf)
    for args in pc_log.pc_all_args(conf):
        top_dt = args[1]
        end_dt = args[2]
        dur = args[3]
        area = args[4]
        _logger.info("testing log2event({0} - {1} in {2})".format(
                top_dt, end_dt, area))
        edict, evmap = pc_log.get_edict(conf, top_dt, end_dt, dur, area)

        assert len(edict) == len(evmap)
        for eid in edict.keys():
            print("Event {0} : {1}".format(eid, evmap.info_str(eid)))
            if evmap.info(eid).type == EventDefinitionMap.type_normal:
                print(evmap.info_repr(ld, eid))
            else:
                print("\n".join([str(dt) for dt in edict[eid]]))
                print("\n".join(["#" + w for w
                        in evmap.info_repr(ld, eid).split("\n")]))
            print


if __name__ == "__main__":
    usage = "usage: {0} [options]".format(sys.argv[0])
    import optparse
    op = optparse.OptionParser(usage)
    op.add_option("-c", "--config", action="store",
            dest="conf", type="string", default=config.DEFAULT_CONFIG_NAME,
            help="configuration file path")
    (options, args) = op.parse_args()

    conf = config.open_config(options.conf)
    config.set_common_logging(conf, _logger, ["pc_log", "evfilter"])
    test_log2event(conf)


