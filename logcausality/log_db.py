#!/usr/bin/env python
# coding: utf-8

import sys
import os
import datetime
import sqlite3
import logging

import config
import fslib
import db_common
import logparser
import lt_common

_logger = logging.getLogger(__name__.rpartition(".")[-1])


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

    def count_lines(self):
        return self.db.count_lines()

    def dt_term(self):
        return self.db.dt_term()

    def whole_term(self):
        # return date term that cover all log data
        top_dt, end_dt = self.db.dt_term()
        top_dt = datetime.datetime.combine(top_dt.date(), datetime.time())
        end_dt = datetime.datetime.combine(end_dt.date(), datetime.time()) + \
                datetime.timedelta(days = 1)
        return top_dt, end_dt

    def whole_host(self, top_dt = None, end_dt = None):
        return self.db.whole_host(top_dt = top_dt, end_dt = end_dt)

    def count_lt(self):
        return self.db.count_lt()

    def count_ltg(self):
        return self.db.count_ltg()

    def iter_lt(self):
        for ltline in self.table:
            yield ltline

    def lt(self, ltid):
        return self.table[ltid]

    def iter_ltgid(self):
        return self.db.iter_ltgid()

    def ltg_members(self, ltgid):
        return [self.table[ltid] for ltid in self.db.get_ltg_members(ltgid)]

    def host_area(self, host):
        return self.db.host_area(host)

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
        if self.db.count_ltg() == 0:
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
        return LogMessage(ltid, self.table[ltid], dt, host, l_w)

    def update_area(self):
        self.db._init_area()

    def commit_db(self):
        self.db.commit()
        if self.ltm is not None:
            self.ltm.dump()


class LogDB():

    def __init__(self, conf, table, reset_db):
        self.table = table
        self.line_cnt = 0
        self.db_type = conf.get("database", "database")
        self.areafn = conf.get("database", "area_filename")
        self.splitter = conf.get("database", "split_symbol")

        if self.db_type == "sqlite3":
            dbpath = conf.get("database", "sqlite3_filename")
            if dbpath is None:
                # for compatibility
                dbpath = conf.get("database", "db_filename")
            self.db = db_common.sqlite3(dbpath)
        elif self.db_type == "mysql":
            host = conf.get("database", "mysql_host")
            dbname = conf.get("database", "mysql_dbname")
            user = conf.get("database", "mysql_user")
            passwd = conf.get("database", "mysql_passwd")
            self.db = db_common.mysql(host, dbname, user, passwd)
        else:
            raise ValueError("invalid database type ({0})".format(
                    self.db_type))

        if self.db.db_exists():
            if reset_db == True:
                self.db.reset()
                self._init_tables()
                self._init_area()
            else:
                self.line_cnt = self.count_lines()
                self._init_lttable()
        else:
            self._init_tables()
            self._init_area()

    def _init_tables(self):
        # init table log
        table_name = "log"
        l_key = [db_common.tablekey("lid", "integer",
                    ("primary_key", "auto_increment", "not_null")),
                 db_common.tablekey("ltid", "integer"),
                 db_common.tablekey("dt", "datetime"),
                 db_common.tablekey("host", "text"),
                 db_common.tablekey("words", "text")]
        sql = self.db.create_table_sql(table_name, l_key)
        self.db.execute(sql)

        # init table lt
        table_name = "lt"
        l_key = [db_common.tablekey("ltid", "integer", ("primary_key",)),
                 db_common.tablekey("ltw", "text"),
                 db_common.tablekey("lts", "text"),
                 db_common.tablekey("count", "integer")]
        sql = self.db.create_table_sql(table_name, l_key)
        self.db.execute(sql)

        # init table ltg
        table_name = "ltg"
        l_key = [db_common.tablekey("ltid", "integer", ("primary_key",)),
                 db_common.tablekey("ltgid", "integer")]
        sql = self.db.create_table_sql(table_name, l_key)
        self.db.execute(sql)

        # init table area
        table_name = "area"
        l_key = [db_common.tablekey("defid", "integer",
                    ("primary_key", "auto_increment", "not_null")),
                 db_common.tablekey("host", "text"),
                 db_common.tablekey("area", "text")]
        sql = self.db.create_table_sql(table_name, l_key)
        self.db.execute(sql)

        self._init_index()

    def _init_index(self):
        l_table_name = self.db.get_table_names()

        table_name = "log"
        index_name = "log_index"
        l_key = [db_common.tablekey("ltid", "integer"),
                 db_common.tablekey("dt", "datetime"),
                 db_common.tablekey("host", "text", (100, ))]
        if not index_name in l_table_name:
            sql = self.db.create_index_sql(table_name, index_name, l_key)
            self.db.execute(sql)
            
        table_name = "ltg"
        index_name = "ltg_index"
        l_key = [db_common.tablekey("ltgid", "integer")]
        if not index_name in l_table_name:
            sql = self.db.create_index_sql(table_name, index_name, l_key)
            self.db.execute(sql)
        
        table_name = "area"
        index_name = "area_index"
        l_key = [db_common.tablekey("area", "text", (100, ))]
        if not index_name in l_table_name:
            sql = self.db.create_index_sql(table_name, index_name, l_key)
            self.db.execute(sql)

    def commit(self):
        self.db.commit()
    
    def add_line(self, ltid, dt, host, l_w):
        table_name = "log"
        d_val = {
            "ltid" : ltid,
            "dt" : self.db.strftime(dt),
            "host" : host,
            "words" : self.splitter.join(l_w),
        }
        l_ss = [db_common.setstate(k, k) for k in d_val.keys()]
        sql = self.db.insert_sql(table_name, l_ss)
        self.db.execute(sql, d_val)

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
            lid = int(row[0])
            ltid = int(row[1])
            dt = self.db.datetime(row[2])
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
        if len(d_cond) == 0:
            raise ValueError("called select with empty condition")
        args = d_cond.copy()

        table_name = "log"
        l_key = ["lid", "ltid", "dt", "host", "words"]
        l_cond = []
        for c in d_cond.keys():
            if c == "ltgid":
                sql= self.db.select_sql("ltg", ["ltid"],
                        [db_common.cond(c, "=", c)])
                l_cond.append(db_common.cond("ltid", "in", sql, False))
            elif c == "area":
                sql= self.db.select_sql("area", ["host"],
                        [db_common.cond(c, "=", c)])
                l_cond.append(db_common.cond("host", "in", sql, False))
            elif c == "top_dt":
                l_cond.append(db_common.cond("dt", ">=", c))
                args[c] = self.db.strftime(d_cond[c])
            elif c == "end_dt":
                l_cond.append(db_common.cond("dt", "<", c))
                args[c] = self.db.strftime(d_cond[c])
            else:
                l_cond.append(db_common.cond(c, "=", c))
        sql = self.db.select_sql(table_name, l_key, l_cond)
        return self.db.execute(sql, args)

    def update_log(self, d_cond, d_update):
        if len(d_cond) == 0:
            raise ValueError("called update with empty condition")
        args = d_cond.copy()

        table_name = "log"
        l_ss = []
        for k, v in d_update.iteritems():
            assert k in ("ltid", "top_dt", "end_dt", "host")
            keyname = "update_" + k
            l_ss.append(db_common.setstate(k, keyname))
            args[keyname] = v
        l_cond = []
        for c in d_cond.keys():
            if c == "ltgid":
                sql= self.db.select_sql("ltg", ["ltid"],
                        [db_common.cond(c, "=", c)])
                l_cond.append(db_common.cond("ltid", "in", sql, False))
            elif c == "area":
                sql= self.db.select_sql("area", ["host"],
                        [db_common.cond(c, "=", c)])
                l_cond.append(db_common.cond("host", "in", sql, False))
            elif c == "top_dt":
                l_cond.append(db_common.cond("dt", ">=", c))
                args[c] = self.db.strftime(d_cond[c])
            elif c == "end_dt":
                l_cond.append(db_common.cond("dt", "<", c))
                args[c] = self.db.strftime(d_cond[c])
            else:
                l_cond.append(db_common.cond(c, "=", c))
        sql = self.db.update_sql(table_name, l_ss, l_cond)
        self.db.execute(sql, args)

    def count_lines(self):
        table_name = "log"
        l_key = ["max(lid)"]
        sql = self.db.select_sql(table_name, l_key)
        cursor = self.db.execute(sql)
        return int(cursor.fetchone()[0])

    def dt_term(self):
        table_name = "log"
        l_key = ["min(dt)", "max(dt)"]
        sql = self.db.select_sql(table_name, l_key)
        cursor = self.db.execute(sql)
        top_dtstr, end_dtstr = cursor.fetchone()
        if None in (top_dtstr, end_dtstr):
            raise ValueError("No data found in DB")
        return self.db.datetime(top_dtstr), self.db.datetime(end_dtstr)

    def whole_host(self, top_dt = None, end_dt = None):
        table_name = "log"
        l_key = ["host"]
        l_cond = []
        args = {}
        if top_dt is not None:
            l_cond.append(db_common.cond("dt", ">=", "top_dt"))
            args["top_dt"] = top_dt
        if end_dt is not None:
            l_cond.append(db_common.cond("dt", "<", "end_dt"))
            args["end_dt"] = end_dt
        sql = self.db.select_sql(table_name, l_key, l_cond, opt = ["distinct"])
        cursor = self.db.execute(sql, args)
        return [row[0] for row in cursor]

    def add_lt(self, ltline):
        table_name = "lt"
        l_ss = []
        l_ss.append(db_common.setstate("ltid", "ltid"))
        l_ss.append(db_common.setstate("ltw", "ltw"))
        l_ss.append(db_common.setstate("lts", "lts"))
        l_ss.append(db_common.setstate("count", "count"))
        if ltline.lts is None:
            lts = None
        else:
            lts = self.splitter.join(ltline.lts)
        args = {
            "ltid" : ltline.ltid,
            "ltw" : self.splitter.join(ltline.ltw),
            "lts" : lts,
            "count" : ltline.cnt,
        }
        sql = self.db.insert_sql(table_name, l_ss)
        self.db.execute(sql, args)

        self.add_ltg(ltline.ltid, ltline.ltgid)

    def add_ltg(self, ltid, ltgid):
        table_name = "ltg"
        l_ss = []
        l_ss.append(db_common.setstate("ltid", "ltid"))
        l_ss.append(db_common.setstate("ltgid", "ltgid"))
        args = {"ltid" : ltid, "ltgid" : ltgid}
        sql = self.db.insert_sql(table_name, l_ss)
        self.db.execute(sql, args)

    def update_lt(self, ltid, ltw, lts, count):
        table_name = "lt"
        l_ss = []
        args = {}
        if ltw is not None:
            l_ss.append(db_common.setstate("ltw", "ltw"))
            args["ltw"] = self.splitter.join(ltw)
        if lts is not None:
            l_ss.append(db_common.setstate("lts", "lts"))
            args["lts"] = self.splitter.join(lts)
        if count is not None:
            l_ss.append(db_common.setstate("count", "count"))
            args["count"] = count
        l_cond = [db_common.cond("ltid", "=", "ltid")]
        args["ltid"] = ltid

        sql = self.db.update_sql(table_name, l_ss, l_cond)
        self.db.execute(sql, args)

    def remove_lt(self, ltid):
        args = {"ltid" : ltid}

        # remove from lt
        table_name = "lt"
        l_cond = [db_common.cond("ltid", "=", "ltid")]
        sql = self.db.delete_sql(table_name, l_cond)
        self.db.execute(sql, args)

        # remove from ltg
        table_name = "ltg"
        l_cond = [db_common.cond("ltid", "=", "ltid")]
        sql = self.db.delete_sql(table_name, l_cond)
        self.db.execute(sql, args)

    def _init_lttable(self):
        table_name = self.db.join_sql("left outer",
                "lt", "ltg", "ltid", "ltid")
        l_key = ("lt.ltid", "ltg.ltgid", "lt.ltw", "lt.lts", "lt.count")
        sql = self.db.select_sql(table_name, l_key)
        cursor = self.db.execute(sql)
        for row in cursor:
            ltid = int(row[0])
            ltgid = int(row[1])
            ltw = row[2].split(self.splitter)
            temp = row[3]
            if temp is None:
                lts = None
            else:
                lts = temp.split(self.splitter)
            count = int(row[4])
            self.table.restore_lt(ltid, ltgid, ltw, lts, count)

    def count_lt(self):
        table_name = "lt"
        l_key = ["count(*)"]
        sql = self.db.select_sql(table_name, l_key)
        cursor = self.db.execute(sql)
        return int(cursor.fetchone()[0])

    def count_ltg(self):
        table_name = "ltg"
        l_key = ["max(ltgid)"]
        sql = self.db.select_sql(table_name, l_key)
        cursor = self.db.execute(sql)
        return int(cursor.fetchone()[0])

    def iter_ltg_def(self):
        table_name = "ltg"
        l_key = ["ltid", "ltgid"]
        sql = self.db.select_sql(table_name, l_key)
        cursor = self.db.execute(sql)
        for row in cursor:
            ltid, ltgid = row
            yield int(ltid), int(ltgid)

    def iter_ltgid(self):
        table_name = "ltg"
        l_key = ["ltgid"]
        sql = self.db.select_sql(table_name, l_key, opt = ["distinct"])
        cursor = self.db.execute(sql)
        for row in cursor:
            ltgid = row[0]
            yield int(ltgid)

    def get_ltg_members(self, ltgid):
        table_name = "ltg"
        l_key = ["ltid"]
        l_cond = [db_common.cond("ltgid", "=", "ltgid")]
        args = {"ltgid" : ltgid}
        sql = self.db.select_sql(table_name, l_key, l_cond)
        cursor = self.db.execute(sql, args)
        return [int(row[0]) for row in cursor]

    def reset_ltg(self):
        sql = self.db.delete_sql("ltg")
        self.db.execute(sql)

    def _init_area(self):
        if self.areafn is None or self.areafn == "":
            return
        areadict = config.GroupDef(self.areafn)
        table_name = "area"
        l_ss = [db_common.setstate("host", "host"),
                db_common.setstate("area", "area")]
        sql = self.db.insert_sql(table_name, l_ss)
        for area, host in areadict.iter_def():
            args = {
                "host" : host,
                "area" : area
            }
            self.db.execute(sql, args)
        self.commit()

    def host_area(self, host):
        table_name = area
        l_key = ["area"]
        l_cond = [db_common.cond("host", "=", "host")]
        args = {"host" : host}
        sql = self.db.select_sql(table_name, l_key, l_cond)
        cursor = self.db.execute(sql, args)
        return [row[0] for row in cursor]


def process_files(conf, targets, initflag, diff = False):
    # Adding log data to DB
    # conf : config.ExtendedConfigParser
    # targets : filename of log data to add
    # initflag : If True, initialize DB before adding data
    # diff : Only add newer log data than latest log in DB

    lp = logparser.LogParser(conf)
    ld = LogData(conf, initflag)
    ld.set_ltm()
    if diff:
        latest = ld.dt_term()[1]

    start_dt = datetime.datetime.now()
    _logger.info("log_db task start")

    for fp in targets:
        if os.path.isdir(fp):
            sys.stderr.write(
                    "{0} is a directory, fail to process\n".format(fp))
            sys.stderr.write(
                    "Use -r if you need to search log data recursively\n")
        else:
            with open(fp, 'r') as f:
                _logger.info("log_db processing {0}".format(fp))
                for line in f:
                    dt, host, l_w, l_s = lp.process_line(line)
                    if diff and dt < latest: continue
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


def info(conf):
    ld = LogData(conf)
    print("[DB status]")
    print("Registered log lines : {0}".format(ld.count_lines()))
    print("Term : {0[0]} - {0[1]}".format(ld.dt_term()))
    print("Log templates : {0}".format(ld.count_lt()))
    print("Log template groups : {0}".format(ld.count_ltg()))
    print("Hosts : {0}".format(len(ld.whole_host())))


def migrate(conf):
    ld = LogData(conf)
    ld.db._init_index()
    ld.update_area()
    ld.commit_db()
    

def remake_ltgroup(conf):
    ld = LogData(conf)
    ld.set_ltm()
    
    start_dt = datetime.datetime.now()
    _logger.info("log_db remake_ltg task start")
    
    ld.ltm.remake_ltg()
    ld.commit_db()
    
    end_dt = datetime.datetime.now()
    _logger.info("log_db remake_ltg task done ({0})".format(end_dt - start_dt))


def remake_area(conf):
    ld = LogData(conf)
    ld.update_area()
    ld.commit_db()


if __name__ == "__main__":

    usage = """
usage: {0} [options] args...
args:
  make : initialize DB and add log data given in config
  make FILES : initialize DB and add log data in FILES
  add FILES : add all log data in FILES to existing DB
  update FILES : add newer log data found in FILES to existing DB
  info : show abstruction of DB status
  remake-area : reconstruct area definiton of hosts in DB
  remake-ltgroup : Remake ltgroup definition for existing log templates.
                   Ltgroups made with this process will be usually
                   different from that with incremental processing.
    """.strip().format(sys.argv[0])

    import optparse
    op = optparse.OptionParser(usage)
    op.add_option("-c", "--config", action="store",
            dest="conf", type="string", default=config.DEFAULT_CONFIG_NAME,
            help="configuration file path")
    op.add_option("-r", action="store_true", dest="recur",
            default=False, help="search log file recursively")
    options, args = op.parse_args()

    conf = config.open_config(options.conf)
    config.set_common_logging(conf, _logger, 
            ["lt_common", "lt_shiso", "lt_va", "lt_import"])

    if len(args) == 0:
        sys.exit(usage)
    mode = args.pop(0)
    if mode == "make":
        if len(args) == 0:
            if conf.getboolean("general", "src_recur") or options.recur:
                targets = fslib.recur_dir(conf.getlist("general", "src_path"))
            else:
                targets = fslib.rep_dir(conf.getlist("general", "src_path"))
        else:
            if options.recur:
                targets = fslib.recur_dir(args)
            else:
                targets = fslib.rep_dir(args)
        process_files(conf, targets, True)
    elif mode == "add":
        if len(args) == 0:
            sys.exit("give me filenames of log data to add to DB")
        else:
            if options.recur:
                targets = fslib.recur_dir(args)
            else:
                targets = fslib.rep_dir(args)
        process_files(conf, targets, False)
    elif mode == "update":
        if len(args) == 0:
            sys.exit("give me filenames of log data to add to DB")
        else:
            if options.recur:
                targets = fslib.recur_dir(args)
            else:
                targets = fslib.rep_dir(args)
        process_files(conf, targets, False, diff = True)
    elif mode == "info":
        info(conf)
    elif mode == "remake-area":
        remake_area(conf)
    elif mode == "remake-ltgroup":
        remake_ltgroup(conf)
    elif mode == "migrate":
        migrate(conf)
    else:
        sys.exit("Invalid argument")



