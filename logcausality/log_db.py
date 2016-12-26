#!/usr/bin/env python
# coding: utf-8

"""
Construct and manage database of log messages, templates,
and grouping definitions.
"""

import sys
import os
import datetime
import sqlite3
import logging

import common
import config
import strutil
import db_common
import logparser
import lt_common
import host_alias

_logger = logging.getLogger(__name__.rpartition(".")[-1])


class LogMessage():
    """An annotated log message.
    
    An instance have a set of information about 1 log message line,
    including timestamp, hostname, log template, and message contents.

    Attributes:
        lid (int): A message identifier in DB.
        lt (lt_common.LogTemplate): Log template of this message.
        dt (datetime.datetime): A timestamp for this message.
        host (str): A hostname that output this message.
        l_w (List(str)): A sequence of words in this message.

    """
    
    def __init__(self, lid, lt, dt, host, l_w):
        """
        Args:
            lid (int): A message identifier in DB.
            lt (lt_common.LogTemplate): Log template of this message.
            dt (datetime.datetime): A timestamp for this message.
            host (str): A hostname that output this message.
            l_w (List(str)): A sequence of words in this message.

        """
        self.lid = lid
        self.lt = lt
        self.dt = dt
        self.host = host
        self.l_w = l_w

    def __str__(self):
        """str: Show attributes in 1 string."""
        return " ".join((str(self.dt), self.host, str(self.lt.ltid),\
                str(self.l_w)))

    def get(self, name):
        """Return value for given attribute."""
        if name == "lid":
            return self.lid
        elif name == "ltid":
            return self.lt.ltid
        elif name == "ltgid":
            return self.lt.ltgid
        elif name == "host":
            return self.host
        else:
            raise NotImplementedError

    def var(self):
        """str: Get sequence of all variable words in this message.
        Variable words are presented with mask (defaults **) in log template.
        """
        return self.lt.var(self.l_w)

    def restore_message(self):
        """str: Get pseudo original log message
        including headers (timestamp and hostname).
        """
        return self.lt.restore_message(self.l_w)

    def restore_line(self):
        """str: Get original log message contents
        except headers (timestamp and hostname).
        """
        return " ".join((str(self.dt), str(self.host),
                self.restore_message()))


class LogData():

    """Interface to get, add or edit log messages in DB.

    Attributes:
        conf (config.ExtendedConfigParser): A common configuration object.
        lttable (lt_common.LTTable): Log template table object.
        db (LogDB): Log database instance.
        ltm (lt_common.LTManager): Log template classifier object.
    """

    def __init__(self, conf, edit = False, reset_db = False):
        """
        Args:
            conf (config.ExtendedConfigParser): A common configuration object.
            edit (Optional[bool]): Defaults to False.
                True if database will be added or edited.
                False if database is used in readonly mode.
            reset_db (Optional[bool]): Defaults to False.
                If True, database will be reset before following process.

        """
        self.conf = conf
        self._reset_db = reset_db
        sym = conf.get("log_template", "variable_symbol")
        self.lttable = lt_common.LTTable(sym) # lt_common.LTTable
        self.db = LogDB(conf, self.lttable, edit, reset_db) # log_db.LogDB
        self.ltm = None # lt_common.LTManager

    def init_ltmanager(self):
        """Initialize log template classifier object.
        Call this before adding new log message in original
        plain string format (not classified with log template) to DB.
        """
        if self.ltm is None:
            self.ltm = lt_common.init_ltmanager(self.conf, self.db,
                    self.lttable, self._reset_db)

    def iter_lines(self, **kargs):
        """Generate log messages in DB that satisfy conditions
        given in arguments. All arguments are defaults to None, and ignored.

        Args:
            lid (Optional[int]): A message identifier in DB.
            ltid (Optional[int]): A log template identifier.
            ltgid (Optional[int]): A log template grouping identifier.
            top_dt (Optional[datetime.datetime]): Condition for timestamp.
                Messages output after 'top_dt' will be yield.
            end_dt (Optional[datetime.datetime]): Condition for timestamp.
                Messages output before 'end_dt' will be yield.
            host (Optional[str]): A source hostname of the message.
            area (Optional[str]): An area name of source hostname.
                Some reserved area-name is available;
                \"all\" to use whole data for 1 area,
                \"host_*\" to use 1 hostname as 1 area.
                (\"each\" is only used for pc_log argument initialization)

        Yields:
            LogMessage: An annotated log message instance
                which satisfies all given conditions.
        """
        _logger.info("iter_lines called ({0})".format(" ".join(
                ["{0}:{1}".format(k, v) for k, v in kargs.iteritems()
                if v is not None])))
        return self.db.iter_lines(**kargs)

    def show_log_repr(self, head = 0, foot = 0,
            ltid = None, ltgid = None,
            top_dt = None, end_dt = None, host = None, area = None):
        """Show representative log messages in DB
        that satisfy conditions given in arguments.
        Arguments except 'limit' are defaults to None, and ignored.
        Results will be shown in Standard Output.

        Args:
            head (Optional[int]): Show leading lines of log messages.
            foot (Optional[int]): Show trailing lines of log messages.
                if both head and foot are 0, all lines are shown.
            lid (Optional[int]): A message identifier in DB.
            ltid (Optional[int]): A log template identifier.
            ltgid (Optional[int]): A log template grouping identifier.
            top_dt (Optional[datetime.datetime]): Condition for timestamp.
                Messages output after 'top_dt' will be yielded.
            end_dt (Optional[datetime.datetime]): Condition for timestamp.
                Messages output before 'end_dt' will be yielded.
            host (Optional[str]): A source hostname of the message.
            area (Optional[str]): An area name of source hostname.
        """
        l_line = sorted([line for line in self.iter_lines(lid = None,
            ltid = ltid, ltgid = ltgid, top_dt = top_dt, end_dt = end_dt,
            host = host, area = area)], key = lambda x: x.dt)
        if (head <= 0 and foot <= 0) or \
                (len(l_line) <= head + foot):
            buf = [line.restore_line() for line in l_line]
        else:
            buf = []
            if head > 0:
                buf += [line.restore_line() for line in l_line[:head]]
            buf += ["..."]
            if foot > 0:
                buf += [line.restore_line() for line in l_line[-foot:]]
            buf.append("({0})".format(len(l_line)))
        return "\n".join(buf)

    def count_lines(self):
        """int: Number of all messages in DB."""
        return self.db.count_lines()

    def dt_term(self):
        """datetime.datetime, datetime.datetime:
            Period of all registered log messages in DB."""
        return self.db.dt_term()

    def whole_term(self):
        """datetime.datetime, datetime.datetime:
            Period of dates that include all log messages in DB.
        """
        # return date term that cover all log data
        top_dt, end_dt = self.db.dt_term()
        top_dt = datetime.datetime.combine(top_dt.date(), datetime.time())
        end_dt = datetime.datetime.combine(end_dt.date(), datetime.time()) + \
                datetime.timedelta(days = 1)
        return top_dt, end_dt

    def whole_host_lt(self, top_dt = None, end_dt = None):
        """List[str, str]: Sequence of all combinations of 
        hostname and ltids in DB."""
        return self.db.whole_host_lt(top_dt = top_dt, end_dt = end_dt)

    def whole_host_ltg(self, top_dt = None, end_dt = None):
        """List[str, str]: Sequence of all combinations of 
        hostname and ltids in DB."""
        ret = set()
        for host, ltid in self.whole_host_lt(top_dt = top_dt, end_dt = end_dt):
            ret.add((host, self.ltgid_from_ltid(ltid))) 
        return list(ret)

    def whole_host(self, top_dt = None, end_dt = None):
        """List[str]: Sequence of all source hostname in DB."""
        return self.db.whole_host(top_dt = top_dt, end_dt = end_dt)

    def count_lt(self):
        """int: Number of all log templates."""
        return self.db.count_lt()

    def count_ltg(self):
        """int: Number of all log template groups."""
        return self.db.count_ltg()

    def iter_lt(self):
        """Yields lt_common.LogTemplate: All log template instance in DB."""
        for ltline in self.lttable:
            yield ltline

    def lt(self, ltid):
        """Get log template instance of given log template identifier.

        Args:
            ltid (int): A log template identifier.

        Returns:
            lt_common.LogTemplate: A log template instance.

        """
        return self.lttable[ltid]

    def iter_ltgid(self):
        """Yields int: Get all identifiers of log template groups."""
        return self.db.iter_ltgid()

    def ltgid_from_ltid(self, ltid):
        lt = self.lttable[ltid]
        return lt.ltgid

    def ltg_members(self, ltgid):
        """Get all log templates in given log template group.
        
        Args:
            ltgid (int): A log template group identifier.

        Returns:
            List[lt_common.LogTemplates]: Sequence of all log template
                instances that belongs to given log template group.
        
        """
        return [self.lttable[ltid] for ltid in self.db.get_ltg_members(ltgid)]

    def host_area(self, host):
        """Get area names that given host belongs to.
        
        Args:
            host (str): A hostname.

        Returns:
            List[str]: A sequence of area names that given host belongs to.

        """
        return self.db.host_area(host)

    @staticmethod
    def _str_ltline(ltline):
        return " ".join((str(ltline.ltid), "({0})".format(ltline.ltgid),
                str(ltline), "({0})".format(ltline.cnt)))

    def show_template_table(self):
        """For debugging"""
        self.init_ltmanager()
        return self.ltm._table

    def dump_template_table(self):
        """For debugging"""
        self.init_ltmanager()
        return "\n".join([" ".join(tpl) for tpl in self.ltm._table])

    def show_all_lt(self):
        """Show all log templates. Log template identifier
        and its template message will be output.

        Returns:
            str: Output message buffer.
        """
        buf = []
        for ltline in self.lttable:
            buf.append(self._str_ltline(ltline))
        return "\n".join(buf)

    def show_all_ltgroup(self):
        """Show all log template groups. Log template grouping identifier
        and its template candidates will be output.

        Returns:
            str: Output message buffer.
        """
        if self.db.count_ltg() == 0:
            self.show_all_lt()
        else:
            buf = []
            for ltgid in self.db.iter_ltgid():
                buf.append(self.show_ltgroup(ltgid))
            return "\n".join(buf)

    def show_ltgroup(self, gid):
        """Show log template groups.

        Returns:
            str: Output message buffer.
        """
        buf = []
        l_ltid = self.db.get_ltg_members(gid)
        length = len(l_ltid)
        cnt = 0
        for ltid in l_ltid:
            ltline = self.lttable[ltid]
            cnt += ltline.cnt
            buf.append(self._str_ltline(ltline))
        buf = ["[ltgroup {0} ({1}, {2})]".format(gid, length, cnt)] + buf
        return "\n".join(buf)

    def add_line(self, ltid, dt, host, l_w):
        """Add a log message to DB.

        Args:
            ltid (int): A log template identifier.
            dt (datetime.datetime): A timestamp.
            host (str): A source hostname of log message.
            l_w (List[str]): A sequence of log message contents.

        Returns:
            LogMessage: An annotated log message instance.
        """
        self.db.add_line(ltid, dt, host, l_w)
        return LogMessage(ltid, self.lttable[ltid], dt, host, l_w)

    def update_area(self):
        self.db._init_area()

    def commit_db(self):
        """Commit requested changes in LogDB.
        """
        self.db.commit()
        if self.ltm is not None:
            self.ltm.dump()


class LogDB():
    """Interface of DB transaction for log data.
    
    Note:
        It is not recommended to use this class directly.
        Instead, use LogData.
    """

    def __init__(self, conf, lttable, edit, reset_db):
        self.lttable = lttable
        self._line_cnt = 0
        self.areafn = conf.get("database", "area_filename")
        self._splitter = conf.get("database", "split_symbol")

        db_type = conf.get("database", "database")
        if db_type == "sqlite3":
            dbpath = conf.get("database", "sqlite3_filename")
            if dbpath is None:
                # for compatibility
                dbpath = conf.get("database", "db_filename")
            self.db = db_common.sqlite3(dbpath)
        elif db_type == "mysql":
            host = conf.get("database", "mysql_host")
            dbname = conf.get("database", "mysql_dbname")
            user = conf.get("database", "mysql_user")
            passwd = conf.get("database", "mysql_passwd")
            self.db = db_common.mysql(host, dbname, user, passwd)
        else:
            raise ValueError("invalid database type ({0})".format(
                    db_type))

        if edit:
            if self.db.db_exists():
                if reset_db == True:
                    self.db.reset()
                    self._init_tables()
                    self._init_area()
                else:
                    self._line_cnt = self.count_lines()
                    self._init_lttable()
            else:
                if reset_db == True:
                    _logger.warning(
                            "Requested to reset DB, but database not found")
                self._init_tables()
                self._init_area()
        else: 
            if self.db.db_exists():
                self._line_cnt = self.count_lines()
                self._init_lttable()
            else:
                raise IOError("database not found")

    def _init_tables(self):
        table_name = "log"
        l_key = [db_common.tablekey("lid", "integer",
                    ("primary_key", "auto_increment", "not_null")),
                 db_common.tablekey("ltid", "integer"),
                 db_common.tablekey("dt", "datetime"),
                 db_common.tablekey("host", "text"),
                 db_common.tablekey("words", "text")]
        sql = self.db.create_table_sql(table_name, l_key)
        self.db.execute(sql)

        table_name = "lt"
        l_key = [db_common.tablekey("ltid", "integer", ("primary_key",)),
                 db_common.tablekey("ltw", "text"),
                 db_common.tablekey("lts", "text"),
                 db_common.tablekey("count", "integer")]
        sql = self.db.create_table_sql(table_name, l_key)
        self.db.execute(sql)

        table_name = "ltg"
        l_key = [db_common.tablekey("ltid", "integer", ("primary_key",)),
                 db_common.tablekey("ltgid", "integer")]
        sql = self.db.create_table_sql(table_name, l_key)
        self.db.execute(sql)

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
            "words" : self._splitter.join(l_w),
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
        if area is None or area == "all":
            pass
        elif area[:5] == "host_":
            d_cond["host"] = area[5:]
        else:
            d_cond["area"] = area
        if host is not None: d_cond["host"] = host

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
                l_w = strutil.split_igesc(row[4], self._splitter)
            yield LogMessage(lid, self.lttable[ltid], dt, host, l_w)

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
                yield strutil.split_igesc(row[4], self._splitter)

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

    def whole_host_lt(self, top_dt = None, end_dt = None):
        table_name = "log"
        l_key = ["host", "ltid"]
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
        return [(row[0], row[1]) for row in cursor]

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
            lts = self._splitter.join(ltline.lts)
        args = {
            "ltid" : ltline.ltid,
            "ltw" : self._splitter.join(ltline.ltw),
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
            args["ltw"] = self._splitter.join(ltw)
        if lts is not None:
            l_ss.append(db_common.setstate("lts", "lts"))
            args["lts"] = self._splitter.join(lts)
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
            ltw = strutil.split_igesc(row[2], self._splitter)
            temp = row[3]
            if temp is None:
                lts = None
            else:
                lts = strutil.split_igesc(temp, self._splitter)
            count = int(row[4])
            self.lttable.restore_lt(ltid, ltgid, ltw, lts, count)

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
        return int(cursor.fetchone()[0]) + 1

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


def _iter_line_from_files(targets):
    for fp in targets:
        if os.path.isdir(fp):
            sys.stderr.write(
                    "{0} is a directory, fail to process\n".format(fp))
            sys.stderr.write(
                    "Use -r if you need to search log data recursively\n")
        else:
            if not os.path.isfile(fp):
                raise IOError("File {0} not found".format(fp))
            with open(fp, 'r') as f:
                _logger.info("log_db processing file {0}".format(fp))
                for line in f:
                    yield line


def process_line(msg, ld, lp, ha, isnew_check = False, latest = None):
    """Add a log message to DB.
    
    Args:
        msg (str): A log message to process.
            Line feed code will be ignored.
        ld (LogData): An log database interface opened in edit mode.
            Needs to initialize template classifier with ld.init_ltmanager.
        lp (logparser.LogParser): An open message parser.
        latest (Optional[datetime.datetime]): If not None,
            Ignore messages that have later timestamp than 'latest'.

    Returns:
        LogMessage: An annotated log message instance.
            Same as lines given with LogData.iterlines.
    """
    line = None

    dt, org_host, l_w, l_s = lp.process_line(msg)
    if latest is not None and dt < latest: return None
    if l_w is None: return None
    l_w = [strutil.add_esc(w) for w in l_w]
    host = ha.resolve_host(org_host)
    print("host = {0} -> {1}".format(org_host, host))
    if host is None: host = org_host

    _logger.debug("Processing [{0}]".format(" ".join(l_w)))
    ltline = ld.ltm.process_line(l_w, l_s)
    if ltline is None:
        ld.ltm.failure_output(msg)
    else:
        _logger.debug("Template [{0}]".format(ltline))
        line = ld.add_line(ltline.ltid, dt, host, l_w)
    return line


def process_files(conf, targets, reset_db, isnew_check = False):
    """Add log messages to DB from files.

    Args:
        conf (config.ExtendedConfigParser): A common configuration object.
        targets (List[str]): A sequence of filepaths to process.
        reset_db (bool): True if DB needs to reset before adding,
            False otherwise. 
        isnew_check (Optional[bool]): If True, add message to DB
            only if its timestamp is newest of existing messages in DB.

    Raises:
        IOError: If a file in targets not found.
    """
    ld = LogData(conf, edit = True, reset_db = reset_db)
    ld.init_ltmanager()
    lp = logparser.LogParser(conf)
    ha = host_alias.HostAlias(conf)
    latest = ld.dt_term()[1] if isnew_check else None

    for line in _iter_line_from_files(targets):
        process_line(line, ld, lp, ha, isnew_check, latest)

    ld.commit_db()


def process_init_data(conf, targets, isnew_check = False):
    """Add log messages to DB from files. This function do NOT process
    messages incrementally. Use this to avoid bad-start problem of
    log template generation with clustering or training methods.

    Note:
        This function needs large memory space.

    Args:
        conf (config.ExtendedConfigParser): A common configuration object.
        targets (List[str]): A sequence of filepaths to process.
        isnew_check (Optional[bool]): If True, add message to DB
            only if its timestamp is newest of existing messages in DB.

    Raises:
        IOError: If a file in targets not found.
    """
    ld = LogData(conf, edit = True, reset_db = True)
    ld.init_ltmanager()
    lp = logparser.LogParser(conf)
    ha = host_alias.HostAlias(conf)
    latest = ld.dt_term()[1] if isnew_check else None

    l_line = []
    l_data = []
    for line in _iter_line_from_files(targets):
        dt, org_host, l_w, l_s = lp.process_line(line)
        if latest is not None and dt < latest: continue
        if l_w is None: continue
        l_w = [strutil.add_esc(w) for w in l_w]
        host = ha.resolve_host(org_host)
        if host is None: host = org_host
        l_line.append((l_w, l_s))
        l_data.append((dt, host))

    for ltline, line, data in zip(ld.ltm.process_init_data(l_line),
                                  l_line, l_data):
        l_w, l_s = line
        dt, host = data
        ld.add_line(ltline.ltid, dt, host, l_w)

    ld.commit_db()


def info(conf):
    """Show abstruction of log messages registered in DB.

    Args:
        conf (config.ExtendedConfigParser): A common configuration object.

    """

    ld = LogData(conf)
    print("[DB status]")
    print("Registered log lines : {0}".format(ld.count_lines()))
    print("Term : {0[0]} - {0[1]}".format(ld.dt_term()))
    print("Log templates : {0}".format(ld.count_lt()))
    print("Log template groups : {0}".format(ld.count_ltg()))
    print("Hosts : {0}".format(len(ld.whole_host())))


def show_lt(conf):
    ld = LogData(conf)
    print ld.show_all_ltgroup()


def dump_lt(conf):
    ld = LogData(conf)
    print ld.show_all_lt()


def show_lt_import(conf):
    ld = LogData(conf)
    for ltobj in ld.iter_lt():
        print " ".join(ltobj.ltw)


def show_template_table(conf):
    ld = LogData(conf)
    print ld.show_template_table()


def dump_template_table(conf):
    ld = LogData(conf)
    print ld.dump_template_table()

def show_repr(conf):
    ld = LogData(conf)
    for ltgid in ld.iter_ltgid():
        print ld.show_ltgroup(ltgid)
        print ld.show_log_repr(head = 5, foot = 5, ltgid = ltgid)
        print

def show_all_host(conf, top_dt = None, end_dt = None):
    ld = LogData(conf)
    for host in ld.whole_host():
        print host

def migrate(conf):
    ld = LogData(conf, edit = True)
    ld.db._init_index()
    ld.update_area()
    ld.commit_db()
    

def remake_ltgroup(conf):
    ld = LogData(conf, edit = True)
    ld.init_ltmanager()
    ld.ltm.remake_ltg()
    ld.commit_db()


def remake_area(conf):
    ld = LogData(conf, edit = True)
    ld.init_ltmanager()
    ld.update_area()
    ld.commit_db()


def _get_targets(conf, args, recur):
    if len(args) == 0:
        if conf.getboolean("general", "src_recur") or recur:
            targets = common.recur_dir(conf.getlist("general", "src_path"))
        else:
            targets = common.rep_dir(conf.getlist("general", "src_path"))
    else:
        if recur:
            targets = common.recur_dir(args)
        else:
            targets = common.rep_dir(args)
    return targets


if __name__ == "__main__":

    usage = """
usage: {0} [options] args...
args:
  make : initialize DB and add log data given in config
  make FILES : initialize DB and add log data in FILES
  add FILES : add all log data in FILES to existing DB
  update FILES : add newer log data found in FILES to existing DB
  info : show abstruction of DB status
  show-lt : show all log templates in DB
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
    op.add_option("--debug", action="store_true", dest="debug",
            default=False, help="set logging level to DEBUG")
    options, args = op.parse_args()

    conf = config.open_config(options.conf)
    lv = logging.DEBUG if options.debug else logging.INFO
    config.set_common_logging(conf, _logger, 
            ["lt_common", "lt_shiso", "lt_va", "lt_import"], lv = lv)

    if len(args) == 0:
        sys.exit(usage)
    mode = args.pop(0)
    if mode == "make":
        targets = _get_targets(conf, args, options.recur)
        timer = common.Timer("log_db make", output = _logger)
        timer.start()
        process_files(conf, targets, True)
        timer.stop()
    elif mode == "make-init":
        targets = _get_targets(conf, args, options.recur)
        timer = common.Timer("log_db make-init", output = _logger)
        timer.start()
        process_init_data(conf, targets)
        timer.stop()
    elif mode == "add":
        if len(args) == 0:
            sys.exit("give me filenames of log data to add to DB")
        else:
            if options.recur:
                targets = common.recur_dir(args)
            else:
                targets = common.rep_dir(args)
        timer = common.Timer("log_db add", output = _logger)
        timer.start()
        process_files(conf, targets, False)
        timer.stop()
    elif mode == "update":
        if len(args) == 0:
            sys.exit("give me filenames of log data to add to DB")
        else:
            if options.recur:
                targets = common.recur_dir(args)
            else:
                targets = common.rep_dir(args)
        timer = common.Timer("log_db update", output = _logger)
        timer.start()
        process_files(conf, targets, False, diff = True)
        timer.stop()
    elif mode == "info":
        info(conf)
    elif mode == "show-lt":
        show_lt(conf)
    elif mode == "dump-lt":
        dump_lt(conf)
    elif mode == "show-tpl":
        show_template_table(conf)
    elif mode == "dump-tpl":
        dump_template_table(conf)
    elif mode == "show-lt-import":
        show_lt_import(conf)
    elif mode == "show-log-repr":
        show_repr(conf)
    elif mode == "all-host":
        show_all_host(conf)
    elif mode == "remake-area":
        remake_area(conf)
    elif mode == "remake-ltgroup":
        remake_ltgroup(conf)
    elif mode == "migrate":
        migrate(conf)
    else:
        sys.exit("Invalid argument")



