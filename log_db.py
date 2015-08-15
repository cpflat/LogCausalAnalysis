#!/usr/bin/env python
# coding: utf-8

import sys
import os
import datetime
import sqlite3
import logging
#import cPickle as pickle

import config
import fslib
import lt_shiso as lt
import logsplitter
import logheader

_config = config.common_config()
_logger = logging.getLogger(__name__)

class LogMessage():

    __module__ = os.path.splitext(os.path.basename(__file__))[0]

    def __init__(self, ltins, ltid, ltgid, dt, host, args):
        self.lt = ltins
        self.ltid = ltid
        self.ltgid = ltgid
        self.dt = dt
        self.host = host
        self.args = args

    def __str__(self):
        return " ".join((str(self.dt), self.host, str(self.ltid),\
                str(self.args)))

    def restore(self):
        return self.lt.table[self.ltid].restore(self.args)

    def restore_message(self):
        return " ".join((str(self.dt), str(self.host), self.restore()))

    def restore_wordlist(self):
        return self.lt.table[self.ltid].restore_wordlist(self.args)

    def verify_ltid(self, ltid):
        return (ltid is None) or (self.ltid == ltid)

    def verify_dt(self, top_dt, end_dt):
        return ((top_dt is None) or (self.dt > top_dt)) and \
                ((end_dt is None) or (self.dt < end_dt))

    def verify_host(self, host):
        return (host is None) or (self.host == host)

    def verify(self, ltid, top_dt, end_dt, host):
        return self.verify_ltid(ltid) and self.verify_dt(top_dt, end_dt) and \
                self.verify_host(host)


class LogList():

    __module__ = os.path.splitext(os.path.basename(__file__))[0]

    def __init__(self, ltins):
        self.lt = ltins
        self.l_line = []

    def __iter__(self):
        self.itercnt = 0
        self.plen = len(self.l_line)
        return self

    def __next__(self):
        return self.next()

    def next(self):
        if self.itercnt >= self.plen:
            raise StopIteration
        ret = self.l_line[self.itercnt]
        self.itercnt += 1
        return ret

    def __len__(self):
        return len(self.l_line)

    def add(self, elem):
        self.l_line.append(elem)

    def merge(self, other):
        self.l_line.extend(other.l_line)

    def get(self, ltid, top_dt, end_dt, host):
        ret = LogList(self.lt)
        for line in self.l_line:
            if line.verify(ltid, top_dt, end_dt, host):
                ret.add(line)
        return ret


class HostArea():

    def __init__(self, fn = _config.get("database", "area_filename")):
        self.hdict = {}
        self.defaultgroup = ""
        self.open_def(fn)

    def open_def(self, fn):
        group = ""
        f = open(fn, 'r')
        for line in f.readlines():
            line = line.rstrip("\n")
            if line == "": continue
            if line[0] == "#":
                setgroup = line[1:].strip()
                if group == "": self.defaultgroup = setgroup
                group = setgroup
            else:
                if group == "":
                    raise IOError("no group name")
                if "," in line:
                    host = line.split(",")[0]
                else:
                    host = line
                #self.hdict[host] = group
                self.hdict.setdefault(host, []).append(group)

    def get_group(self, host):
        if self.hdict.has_key(host):
            return self.hdict[host]
        else:
            return self.defaultgroup

    def get_def(self):
        ret = []
        for host, l_area in self.hdict.iteritems():
            for area in l_area:
                ret.append((host, area))
        return ret


class LogDBManager(object):

    def __init__(self, ltfn = None):
        self._init_lt(ltfn)

    def _init_lt(self, ltfn):
        self.lt = lt.LTManager(ltfn)
        self.lt.set_param(0.9, 4)

    def open_lt(self, fn = None):
        self.lt.load() 

    def _init_db(self):
        raise NotImplementedError

    def formatdb(self):
        raise NotImplementedError

    def add(self, ltid, dt, host, args):
        raise NotImplementedError

    def commit(self):
        raise NotImplementedError

    def get(self, ltid = None, top_dt = None, end_dt = None,
            host = None, area = None):
        raise NotImplementedError

    def generate(self, ltid = None, top_dt = None, end_dt = None,
            host = None, area = None):
        raise NotImplementedError


#class LogDBManagerPickle(LogDBManager):
#
#    DIRNAME = "logpickle"
#
#    def __init__(self, dirname = None, ltfn = None):
#        self.lt = lt_manager.open_lt(ltfn)
#        if dirname is None:
#            dirname = self.DIRNAME
#        self.dirname = dirname
#        fslib.mkdir(self.dirname)
#        self.lastdata = None
#        self.lastfn = None
#
#    def _init_buffer(self, fn):
#        self._load(fn)
#        self.lastfn = fn
#        #self.lastdata = self._init_db()
#
#    def _load(self, fn):
#        if fn is None:
#            fn = self.lastfn
#        fpath = self._filepath(fn)
#        if os.path.exists(fpath):
#            with open(self._filepath(fn), 'r') as f:
#                self.lastdata = pickle.load(f)
#        else:
#            self.lastdata = self._init_db()
#
#    def _dump(self, fn = None):
#        if fn is None:
#            fn = self.lastfn
#        with open(self._filepath(fn), 'w') as f:
#            pickle.dump(self.lastdata, f)
#
#    @staticmethod
#    def _filename(dt):
#        return dt.strftime('%Y%m%d')
#
#    def _filepath(self, fn):
#        return "/".join((self.dirname, fn))
#
#    def formatdb(self):
#        fslib.rm_dirchild(self.dirname)
#
#    def add(self, ltid, dt, host, args):
#        fn = self._filename(dt)
#        if not self.lastfn == fn:
#            if self.lastfn is not None:
#                self._dump()
#            self._init_buffer(fn)
#        e = LogMessage(self.lt, ltid, ltid, dt, host, args)
#        self.lastdata.add(e)
#
#    def commit(self):
#        if self.lastfn is not None:
#            self._dump()
#
#    @staticmethod
#    def _days_dt(top_dt, end_dt):
#        ret = []
#        date = top_dt.replace(hour=0, minute=0, second=0)
#        #assert date < end_dt
#        while date < end_dt:
#            ret.append(date)
#            date += datetime.timedelta(days=1)
#        return ret
#
#    def get(self, ltid = None, top_dt = None, end_dt = None, host = None):
#        data = self._init_db()
#        for dt in self._days_dt(top_dt, end_dt):
#            fpath = self._filepath(self._filename(dt))
#            if os.path.exists(fpath):
#                with open(fpath, 'r') as f:
#                    temp_data = pickle.load(f)
#                    data.merge(temp_data.get(ltid, top_dt, end_dt, host))
#        return data


class LogDBManagerSQL(LogDBManager):

    def __init__(self, ltfn = None, dbfn = None):
        super(LogDBManagerSQL, self).__init__(ltfn)
        if dbfn is None:
            dbfn = _config.get("database", "db_filename")
        self.dbfn = dbfn
        if os.path.exists(self.dbfn):
            self._open()
        else:
            self._open()
            self._initdb()

    def __del__(self):
        self._close()

    def _open(self):
        self.connect = sqlite3.connect(self.dbfn)
        self.connect.text_factory = str

    def _initdb(self):
        sql = u"""
            create table db (
                id integer primary key autoincrement not null,
                ltid integer,
                dt text,
                host text,
                args text
            );
        """
        self.connect.execute(sql)

    def _close(self):
        self.connect.close()

    def formatdb(self):
        self._close()
        fslib.rm(self.dbfn)
        self._open()
        self._initdb()

    def add(self, ltid, dt, host, args):
        sql = u"""
            insert into db (ltid, dt, host, args) values (
                :ltid,
                :dt,
                :host,
                :args
            );
        """
        sqlv = {
            "ltid" : ltid,
            "dt" : dt.strftime('%Y-%m-%d %H:%M:%S'),
            "host" : host,
            "args" : ",".join(args)
        }
        self.connect.execute(sql, sqlv)

    def update(self, l_cond_column, l_cond_value, l_new_column, l_new_value):
        if "area" in l_cond_column:
            raise ValueError("update do not allow area in condition")
        else:
            sql_header = "update db set "
        l_buf = []
        for col, val in zip(l_new_column, l_new_value):
            l_buf.append("{0} = \'{1}\'".format(col, val))
        sql_update = ", ".join(l_buf)
        sql_mid = " where "
        for col, val in zip(l_cond_column, l_cond_value):
            l_buf.append("{0} = \'{1}\'".format(col, val))
        sql_cond = " and ".join(l_buf)
        sql = "".join((sql_header, sql_update, sql_mid, sql_cond))
        cursor = self.connect.cursor()
        cursor.execute(sql)


    def update_lid(self, lid, l_new_column, l_new_value):
        sql_header = "update db set "
        l_buf = []
        for col, val in zip(l_new_column, l_new_value):
            if col == "area":
                l_buf.append("{0} = \'{1}\'".format(col, val))
            else:
                l_buf.append("{0} = \'{1}\'".format(col, val))
        sql_update = ", ".join(l_buf)
        sql_cond = " where id = {0}".format(lid)
        sql = "".join((sql_header, sql_update, sql_cond))
        cursor = self.connect.cursor()
        cursor.execute(sql)


    def commit(self):
        self.connect.commit()
        sql = u"""
            create index whole_index on db(ltid, dt, host);
        """
        try:
            self.connect.execute(sql)
        except sqlite3.OperationalError:
            pass
        self.connect.commit()

    def areadb(self):
        areains = HostArea()
        try:
            sql = u"""
                create table area (
                    defid integer primary key autoincrement not null,
                    host text,
                    area text
                );
            """
            self.connect.execute(sql)
        except sqlite3.OperationalError:
            pass
        for host, area in areains.get_def(): 
            sql = u"""
                insert into area (host, area) values (
                    :host,
                    :area
                );
            """
            sqlv = {
                "host" : host,
                "area" : area
            }
            self.connect.execute(sql, sqlv) 
        self.connect.commit()

    def _select(self, ltid, top_dt, end_dt, host, area):
        if area is not None:
            sql_header = "select * from db left outer join area " \
            "on db.host == area.host where"
        else:
            sql_header = "select * from db where"
        sqlc = []
        if ltid is not None:
            sqlc.append("db.ltid = :ltid")
        if top_dt is not None:
            sqlc.append("db.dt >= :top_dt")
        if end_dt is not None:
            sqlc.append("db.dt < :end_dt")
        if host is not None:
            sqlc.append("db.host = :host")
        if area is not None:
            sqlc.append("area.area = :area")
        if len(sqlc) == 0:
            raise ValueError("More than 1 argument should NOT be None")
        sql = " ".join((sql_header, " and ".join(sqlc)))
        sqlv = {
            "ltid" : ltid,
            "top_dt" : top_dt,
            "end_dt" : end_dt,
            "host" : host,
            "area" : area
        }
        cursor = self.connect.cursor()
        cursor.execute(sql, sqlv)
        return cursor

    def _restore_lm(self, line):
        lid = line[0]
        ltid = line[1]
        ltgid = line[1] # To be edited if LTG added
        dt = datetime.datetime.strptime(line[2], '%Y-%m-%d %H:%M:%S')
        host = line[3]
        if line[4] == "":
            args = []
        else:
            args = self.lt.table[ltid].get_variable(line[4].split(","))
            #args = line[4].split(",")
        return lid, LogMessage(self.lt, ltid, ltgid, dt, host, args)

    # Notice : use generate to avoid memory excess
    def get(self, ltid = None, top_dt = None, end_dt = None, \
            host = None, area = None):
        cursor = self._select(ltid, top_dt, end_dt, host, area)
        ret = self._init_db()
        for line in cursor:
            lid, lm = self._restore_lm(line)
            ret.add(lm)
        return ret

    def generate(self, ltid = None, top_dt = None, end_dt = None, \
            host = None, area = None):
        cursor = self._select(ltid, top_dt, end_dt, host, area)
        for line in cursor:
            lid, lm = self._restore_lm(line)
            yield lm

    def generate_with_id(self, ltid = None, top_dt = None, end_dt = None, \
            host = None, area = None):
        cursor = self._select(ltid, top_dt, end_dt, host, area)
        for line in cursor:
            lid, lm = self._restore_lm(line)
            yield lid, lm

    def generate_wordlist(self, ltid = None, top_dt = None, end_dt = None, \
            host = None, area = None):
        # return wordlist, not using information of lt
        cursor = self._select(ltid, top_dt, end_dt, host, area)
        for line in cursor:
            wordlist = line[4]
            if wordlist == "":
                yield []
            else:
                yield wordlist.split(",")


def ldb_manager(ltfn = None):
    return LogDBManagerSQL(ltfn, _config.get("database", "db_filename"))
    #if SQLFLAG:
    #    return LogDBManagerSQL()
    #else:
    #    return LogDBManagerPickle()


def db_add(ldb, line):
    message, info = logheader.split_header(line)
    if message is None: return
    l_w, l_s = logsplitter.split(message)
    ltid = ldb.lt.process_line(l_w, l_s)
    if ltid is None:
        _logger.warning(
                "Log template not found for message [{0}]".format(line))
    else:
        #l_var = ldb.lt.table[ltid].get_variable(l_w)
        #ldb.add(ltid, info["timestamp"], info["hostname"], l_var)
        ldb.add(ltid, info["timestamp"], info["hostname"], l_w)


def construct_db(targets):
    ldb = ldb_manager()
    ldb.formatdb()
    for fn in fslib.rep_dir(targets):
        with open(fn, 'r') as f:
            for line in f:
                line = line.rstrip("\n")
                db_add(ldb, line)
    ldb.areadb()
    ldb.commit()
    ldb.lt.dump()


def area_db():
    ldb = ldb_manager()
    ldb.areadb()


def test_make():
    if not os.path.exists(_config.get("log_template", "db_filename")):
        import lt_generate
        lt_generate.test_make()
    construct_db("test.temp")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit("usage: {0} targets...".format(sys.argv[0]))
    if sys.argv[1] == "--test":
        test_make()
        sys.exit()
    targets = sys.argv[1:]
    construct_db(targets)


