#!/usr/bin/env python
# coding: utf-8

import sys
import os
import datetime
import sqlite3
import logging

import config
import fslib
import logparser
import lt_common

_logger = logging.getLogger(__name__)


class LogMessage():

    def __init__(self, lid, lt, dt, host, l_w):
        self.lid = lid
        self.lt = lt
        self.dt = dt
        self.host = host
        self.l_w = l_w

    def __str__(self):
        return " ".join((str(self.dt), self.host, str(self.lt.ltid),\
                str(self.l_w)))

    def var(self):
        return self.lt.var(self.l_w)

    def restore_message(self):
        # restore original log message without header
        return self.lt.restore_message(self.l_w)

    def restore_line(self):
        # restore original log message with header (dt, host)
        return " ".join((str(self.dt), str(self.host),
                self.restore_message()))


class LogData():

    def __init__(self, conf, reset_db = False):
        self.conf = conf
        self.reset_db = reset_db
        sym = conf.get("log_template", "variable_symbol")
        self.table = lt_common.LTTable(sym) # lt_common.LTTable
        self.db = LogDB(conf, self.table, reset_db) # log_db.LogDB
        self.ltm = None # lt_common.LTManager

    def set_ltm(self):
        lt_alg = self.conf.get("log_template", "lt_alg")
        ltg_alg = self.conf.get("log_template", "ltgroup_alg")
        # ltg_alg : used in lt_common.LTManager._init_ltgroup
        if lt_alg == "shiso":
            import lt_shiso
            self.ltm = lt_shiso.LTManager(self.conf, self.db, self.table,
                    self.reset_db, ltg_alg)
        elif lt_alg == "va":
            import lt_va
            self.ltm = lt_va.LTManager(self.conf, self.db, self.table,
                    self.reset_db, ltg_alg)
        elif lt_alg == "import":
            import lt_import
            self.ltm = lt_import.LTManager(self.conf, self.db, self.table,
                    self.reset_db, ltg_alg)
        else:
            raise ValueError("lt_alg({0}) invalid".format(lt_alg))

    def iter_lines(self, lid = None, ltid = None, ltgid = None, top_dt = None,
            end_dt = None, host = None, area = None):
        if area == "all":
            area = None
        return self.db.iter_lines(lid = lid, ltid = ltid, ltgid = ltgid,
                top_dt = top_dt, end_dt = end_dt, host = host, area = area)

    def show_log_repr(self, limit = None, ltid = None, ltgid = None,
            top_dt = None, end_dt = None, host = None, area = None):

        buf = []
        cnt = 0
        limit_flag = False
        for line in self.iter_lines(lid = None, ltid = ltid, ltgid = ltgid,
                top_dt = top_dt, end_dt = end_dt, host = host, area = area):
            if limit is not None and cnt >= limit:
                limit_flag = True
            else:
                buf.append(line.restore_line())
            cnt += 1
        else:
            length = cnt
        if limit_flag:
            buf.append("... ({0})".format(length))
        else:
            buf.append("({0})".format(length))
        print "\n".join(buf)

    def whole_term(self):
        return self.db.whole_term()
        
    def iter_lt(self):
        for ltline in self.table:
            yield ltline

    def lt(self, ltid):
        return self.table[ltid]

    def iter_ltgid(self):
        return self.db.iter_ltgid()

    def ltg_members(self, ltgid):
        return [self.table[ltid] for ltid in self.db.get_ltg_members(ltgid)]

    @staticmethod
    def _str_ltline(ltline):
        return " ".join((str(ltline.ltid), "({0})".format(ltline.ltgid),
                str(ltline), "({0})".format(ltline.cnt)))

    def show_all_lt(self):
        buf = []
        for ltline in self.table:
            buf.append(self._str_ltline(ltline))
        return "\n".join(buf)

    def show_all_ltgroup(self):
        if self.db.len_ltg() == 0:
            self.show_all_lt()
        else:
            buf = []
            for ltgid in self.db.iter_ltgid():
                buf.append(self.show_ltgroup(ltgid))
            return "\n".join(buf)

    def show_ltgroup(self, gid):
        buf = []
        l_ltid = self.db.get_ltg_members(gid)
        length = len(l_ltid)
        cnt = 0
        for ltid in l_ltid:
            ltline = self.table[ltid]
            cnt += ltline.cnt
            buf.append(self._str_ltline(ltline))
        buf = ["[ltgroup {0} ({1}, {2})]".format(gid, length, cnt)] + buf
        return "\n".join(buf)

    def add_line(self, ltid, dt, host, l_w):
        self.db.add_line(ltid, dt, host, l_w)

    def update_area(self):
        self.db._areadb()

    def commit_db(self):
        self.db.commit()
        if self.ltm is not None:
            self.ltm.dump()


class LogDB():

    def __init__(self, conf, table, reset_db):
        self.table = table
        self.dbfn = conf.get("database", "db_filename")
        self.areafn = conf.get("database", "area_filename")
        self.splitter = conf.get("database", "split_symbol")

        if reset_db == True:
            fslib.rm(self.dbfn)
            self._open()
            self._initdb()
            self._areadb()
        elif os.path.exists(self.dbfn):
            self._open()
            self._init_lttable()
        else:
            self._open()
            self._initdb()
            self._areadb()

    def __del__(self):
        self._close()

    def _open(self):
        self.connect = sqlite3.connect(self.dbfn)
        self.connect.text_factory = str

    def _close(self):
        self.connect.close()

    def _initdb(self):
        # init table log
        sql = u"""
            create table log (
                lid integer primary key autoincrement not null,
                ltid integer,
                dt text,
                host text,
                words text
            );
        """
        self.connect.execute(sql)

        # init table lt
        sql = u"""
            create table lt (
                ltid integer primary key,
                ltw text,
                lts text,
                count integer
            );
        """
        self.connect.execute(sql)

        # init table ltg
        sql = u"""
            create table ltg (
                ltid integer primary key,
                ltgid integer
            );
        """
        self.connect.execute(sql)

        # init table area
        sql = u"""
            create table area (
                host text primary key,
                area text
            );
        """
        self.connect.execute(sql)

        # init index
        sql = u"""
            create index log_index on log(ltid, dt, host);
        """
        self.connect.execute(sql)

    def commit(self):
        self.connect.commit()
    
    def add_line(self, ltid, dt, host, l_w):
        sql = u"""
            insert into log (ltid, dt, host, words) values (
                :ltid,
                :dt,
                :host,
                :words
            );
        """
        sqlv = {
            "ltid" : ltid,
            "dt" : dt.strftime('%Y-%m-%d %H:%M:%S'),
            "host" : host,
            "words" : self.splitter.join(l_w)
        }
        self.connect.execute(sql, sqlv)

    def iter_lines(self, lid = None, ltid = None, ltgid = None, top_dt = None,
            end_dt = None, host = None, area = None):
        d_cond = {}
        if lid is not None: d_cond["lid"] = lid
        if ltid is not None: d_cond["ltid"] = ltid
        if ltgid is not None: d_cond["ltgid"] = ltgid
        if top_dt is not None: d_cond["top_dt"] = top_dt
        if end_dt is not None: d_cond["end_dt"] = end_dt
        if host is not None: d_cond["host"] = host
        if area is not None: d_cond["area"] = area
        if len(d_cond) == 0:
            raise ValueError("More than 1 argument should NOT be None")
        for row in self._select_log(d_cond):
            lid = row[0]
            ltid = row[1]
            dt = datetime.datetime.strptime(row[2], '%Y-%m-%d %H:%M:%S')
            host = row[3]
            if row[4] == "":
                l_w = []
            else:
                l_w = row[4].split(self.splitter)
            yield LogMessage(lid, self.table[ltid], dt, host, l_w)

    def iter_words(self, lid = None, ltid = None, ltgid = None, top_dt = None,
            end_dt = None, host = None, area = None):
        d_cond = {}
        if lid is not None: d_cond["lid"] = lid
        if ltid is not None: d_cond["ltid"] = ltid
        if ltgid is not None: d_cond["ltgid"] = ltgid
        if top_dt is not None: d_cond["top_dt"] = top_dt
        if end_dt is not None: d_cond["end_dt"] = end_dt
        if host is not None: d_cond["host"] = host
        if area is not None: d_cond["area"] = area
        if len(d_cond) == 0:
            raise ValueError("More than 1 argument should NOT be None")
        for row in self._select_log(d_cond):
            if row[4] == "":
                yield []
            else:
                yield row[4].split(self.splitter)

    def _select_log(self, d_cond):
        # d_cond = {column : value, ...}
        if len(d_cond) == 0:
            raise ValueError("called select with empty condition")

        sql_header = """
            select log.lid, log.ltid, log.dt, log.host, log.words from log
            left outer join ltg on log.ltid == ltg.ltid
        """
        if "area" in d_cond.keys():
            sql_header += " left outer join area on log.host == area.host"
        sqlc = []
        for k, v in d_cond.iteritems():
            if k in ("lid", "ltid", "host"):
                sqlc.append("log.{0} = :{0}".format(k))
            elif k == "top_dt":
                sqlc.append("log.dt >= :top_dt")
            elif k == "end_dt":
                sqlc.append("log.dt < :end_dt")
            elif k == "ltgid":
                sqlc.append("ltg.ltgid = :ltgid")
            elif k == "area":
                sqlc.append("area.area = :area")
        sql = " ".join((sql_header, "where", " and ".join(sqlc)))
        cursor = self.connect.cursor()
        cursor.execute(sql, d_cond)
        return cursor

    def update_log(self, d_cond, d_update):
        # updating rows in table db (NOT allow editing lt / ltg / area)
        if len(d_cond) == 0:
            raise ValueError("called update with empty condition")

        sql_header = "update log set"
        l_buf = []
        for k, v in d_update.iteritems():
            assert k in ("ltid", "top_dt", "end_dt", "host")
            l_buf.append("{0} = \'{1}\'".format(k, v))
        sql_update = ", ".join(l_buf)
        sqlc = []
        for k, v in d_cond.iteritems():
            if k in ("lid", "ltid", "top_dt", "end_dt", "host"):
                sqlc.append("{0} = :{0}".format(k))
            elif k == "ltgid":
                sqlc.append("ltid in " + 
                        "(select ltid from ltg where ltgid = :ltgid)")
            elif k == "area":
                sqlc.append("host in " + 
                        "(select host from area where area = :area)")
        sql_cond = " and ".join(sqlc)
        sql = " ".join((sql_header, sql_update, "where", sql_cond))
        cursor = self.connect.cursor()
        cursor.execute(sql, d_cond)

    def whole_term(self):
        sql = "select dt from log"
        cursor = self.connect.cursor()
        cursor.execute(sql)
        s = set()
        for row in cursor:
            dt = datetime.datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S')
            s.add(dt.date())
        l_dt = [datetime.datetime.combine(d, datetime.time()) for d in s]
        l_dt.sort(reverse = False)
        return l_dt[0], l_dt[-1] + datetime.timedelta(days = 1)

    def add_lt(self, ltline):
        # add to lt
        sql = u"""
            insert into lt (ltid, ltw, lts, count) values (
                :ltid,
                :ltw,
                :lts,
                :count
            );
        """
        if ltline.lts is None:
            lts = None
        else:
            lts = self.splitter.join(ltline.lts)
        sqlv = {
            "ltid" : ltline.ltid,
            "ltw" : self.splitter.join(ltline.ltw),
            "lts" : lts,
            "count" : ltline.cnt,
        }
        self.connect.execute(sql, sqlv)

        self.add_ltg(ltline.ltid, ltline.ltgid)

    def add_ltg(self, ltid, ltgid):
        # add to ltg
        sql = u"""
            insert into ltg (ltid, ltgid) values (
                :ltid,
                :ltgid
            );
        """
        sqlv = {
            "ltid" : ltid,
            "ltgid" : ltgid,
            }
        self.connect.execute(sql, sqlv)

    def update_lt(self, ltid, ltw, lts, count):
        sql_header = "update lt set"
        sqlv = {"ltid" : ltid}
        l_buf = []
        if ltw is not None:
            l_buf.append("ltw = :ltw")
            sqlv["ltw"] = self.splitter.join(ltw)
        if lts is not None:
            l_buf.append("lts = :lts")
            sqlv["lts"] = self.splitter.join(lts)
        if count is not None:
            l_buf.append("count = :count")
            sqlv["count"] = count
        sql_update = ", ".join(l_buf)
        sql_cond = "where ltid = :ltid".format(ltid)
        sql = " ".join((sql_header, sql_update, sql_cond))
        cursor = self.connect.cursor()
        cursor.execute(sql, sqlv)

    def remove_lt(self, ltid):
        # remove from lt
        sql = "delete from lt where ltid = :ltid"
        sqlv = {"ltid" : ltid}
        self.connect.execute(sql, sqlv)

        # remove from ltg
        sql = "delete from ltg where ltid = :ltid"
        sqlv = {"ltid" : ltid}
        self.connect.execute(sql, sqlv)

    def _init_lttable(self):
        sql = "select lt.ltid, ltg.ltgid, lt.ltw, lt.lts, lt.count " + \
                "from lt left outer join ltg where lt.ltid = ltg.ltid"
        cursor = self.connect.cursor()
        cursor.execute(sql)
        for row in cursor:
            ltid = row[0]
            ltgid = row[1]
            ltw = row[2].split(self.splitter)
            temp = row[3]
            if temp is None:
                lts = None
            else:
                lts = temp.split(self.splitter)
            count = row[4]
            self.table.restore_lt(ltid, ltgid, ltw, lts, count)

    def len_ltg(self):
        sql = "select count(*) from ltg"
        cursor = self.connect.cursor()
        cursor.execute(sql)
        return cursor.fetchone()[0]

    def iter_ltg_def(self):
        sql = "select ltid, ltgid from ltg"
        cursor = self.connect.cursor()
        cursor.execute(sql)
        for row in cursor:
            ltid, ltgid = row
            yield ltid, ltgid

    def iter_ltgid(self):
        sql = "select distinct ltgid from ltg"
        cursor = self.connect.cursor()
        cursor.execute(sql)
        for row in cursor:
            ltgid = row[0]
            yield ltgid

    def get_ltg_members(self, ltgid):
        sql = "select ltid from ltg where ltgid = {0}".format(ltgid)
        cursor = self.connect.cursor()
        cursor.execute(sql)
        return [row[0] for row in cursor]

    def reset_ltg(self):
        sql = "delete from ltg"
        self.connect.execute(sql)

    def _areadb(self):
        areadict = config.GroupDef(self.areafn)
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
        for area, host in areadict.iter_def():
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


def process_files(conf, targets, rflag, fflag):
    if len(targets) == 0:
        if conf.getboolean("general", "src_recur") or rflag:
            l_fp = fslib.recur_dir(conf.getlist("general", "src_path"))
        else:
            l_fp = fslib.rep_dir(conf.getlist("general", "src_path"))
    else:
        if rflag:
            l_fp = fslib.recur_dir(targets)
        else:
            l_fp = fslib.rep_dir(targets)

    lp = logparser.LogParser(conf)
    ld = LogData(conf, fflag)
    ld.set_ltm()

    start_dt = datetime.datetime.now()
    _logger.info("log_db task start")

    for fp in l_fp:
        with open(fp, 'r') as f:
            _logger.info("log_db processing {0}".format(fp))
            for line in f:
                dt, host, l_w, l_s = lp.process_line(line)
                if l_w is None: continue
                ltline = ld.ltm.process_line(l_w, l_s)
                if ltline is None:
                    _logger.warning("Log template not found " + \
                            "for message [{0}]".format(line))
                else:
                    ld.add_line(ltline.ltid, dt, host, l_w)
    ld.commit_db()
    
    end_dt = datetime.datetime.now()
    _logger.info("log_db task done ({0})".format(end_dt - start_dt))


def remake_ltgroup(conf):
    lp = logparser.LogParser(conf)
    ld = LogData(conf)
    ld.set_ltm()
    
    start_dt = datetime.datetime.now()
    _logger.info("log_db remake_ltg task start")
    
    ld.ltm.remake_ltg()
    ld.commit_db()
    
    end_dt = datetime.datetime.now()
    _logger.info("log_db remake_ltg task done ({0})".format(end_dt - start_dt))


def remake_area(conf):
    lp = logparser.LogParser(conf)
    ld = LogData(conf)
    ld.update_area()
    ld.commit_db()


if __name__ == "__main__":

    import optparse
    usage = """
usage: {0} [options] <file...>
  with arguments:
    add log data in given src data files
  with no arguments :
    add log data in src data files defined in config
    """.strip().format(sys.argv[0])

    op = optparse.OptionParser(usage)
    op.add_option("-c", "--config", action="store",
            dest="conf", type="string", default=config.DEFAULT_CONFIG_NAME,
            help="configuration file path")
    op.add_option("-f", action="store_true", dest="format",
            default=False, help="format db and reconstruct")
    op.add_option("-r", action="store_true", dest="recur",
            default=False, help="search log file recursively")
    op.add_option("-g", "--group", action="store_true", dest="gflag",
            default=False, help="remake ltgroup for existing log templates")
    options, args = op.parse_args()

    conf = config.open_config(options.conf)
    config.set_common_logging(conf, _logger, ["lt_common", "lt_shiso"])

    if options.gflag:
        remake_ltgroup(conf)
    else:
        process_files(conf, args, options.recur, options.format)






