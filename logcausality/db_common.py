#!/usr/bin/env python
# coding: utf-8

import os
import datetime
from collections import namedtuple

TableKey = namedtuple("TableKey", ("key", "type", "attr"))
Condition = namedtuple("Condition", ("key", "opr", "val", "repl"))
Setstate = namedtuple("SetState", ("key", "val"))


class database(object):

    # d_key in create_table : key = key_name, val = [type, attr, attr...]
    # d_opr : operand to use in compararizon
    # l_repl : values with given keys are replaced in sql_query
    #          to generate sql with subquery

    def __init__(self, dbpath):
        raise NotImplementedError

    def db_exists(self):
        raise NotImplementedError

    def reset(self):
        raise NotImplementedError

    def commit(self):
        raise NotImplementedError
        
    def strftime(self, dt):
        if isinstance(dt, datetime.datetime):
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(dt, str):
            return dt
        else:
            raise TypeError

    def strptime(self, string):
        return datetime.datetime.strptime(string, '%Y-%m-%d %H:%M:%S')

    def datetime(self, ret):
        raise NotImplementedError

    def _ph(self, varname):
        raise NotImplementedError
        
    def _set_state(self, l_setstate):
        if len(l_setstate) == 0:
            raise ValueError("empty setstates")
        l_buf = []
        for ss in l_setstate:
            l_buf.append("{0} = {1}".format(ss.key, self._ph(ss.val)))
        return ", ".join(l_buf)

    def _cond_state(self, l_cond):
        l_buf = []
        for cond in l_cond:
            if cond.repl:
                buf = "{0.key} {0.opr} {1}".format(cond, self._ph(cond.val))
            else:
                buf = "{0.key} {0.opr} ({0.val})".format(cond)
            l_buf.append(buf)
        return " and ".join(l_buf)

    def _table_key_type(self, type_str):
        # allowed typename : integer, text, datetime
        raise NotImplementedError

    def _table_key_attr(self, attr):
        # allowed attr : primary_key, auto_increment, not_null
        raise NotImplementedError

    def _index_key(self, tablekey):
        raise NotImplementedError

    def join_sql(self, join_opt, table_name1, table_name2, key1, key2):
        return "{1} {0} join {2} on {1}.{3} = {2}.{4}".format(join_opt,
                table_name1, table_name2, key1, key2)

    def create_table_sql(self, table_name, l_key):
        l_def = []
        for key in l_key:
            type_name = self._table_key_type(key.type)
            if len(key.attr) > 0:
                l_attr = []
                for a in key.attr:
                    l_attr.append(self._table_key_attr(a))
                l_def.append("{0} {1} {2}".format(key.key, type_name,
                        " ".join(l_attr)))
            else:
                l_def.append("{0} {1}".format(key.key, type_name))
        sql = "create table {0} ({1})".format(table_name, ", ".join(l_def))
        return sql

    def create_index_sql(self, table_name, index_name, l_key):
        sql = "create index {0} on {1}({2})".format(index_name,
                table_name, ", ".join([self._index_key(key) for key in l_key]))
        return sql

    def select_sql(self, table_name, l_key, l_cond = [], opt = []):
        # now only "distinct" is allowed for opt
        sql_header = "select"
        if "distinct" in opt:
            sql_header += " distinct"
        sql = "{0} {1} from {2}".format(sql_header, ", ".join(l_key),
                table_name)
        if len(l_cond) > 0:
            sql += " where {0}".format(self._cond_state(l_cond))
        return sql 

    def insert_sql(self, table_name, l_setstate):
        l_key, l_val = zip(*[(ss.key, self._ph(ss.val)) for ss in l_setstate])
        sql = "insert into {0} ({1}) values ({2})".format(table_name,
                ", ".join(l_key), ", ".join(l_val))
        return sql

    def update_sql(self, table_name, l_setstate, l_cond = []):
        sql = "update {0} set {1}".format(table_name,
                self._set_state(l_setstate))
        if len(l_cond) > 0:
            sql += " where {0}".format(self._cond_state(l_cond))
        return sql 

    def delete_sql(self, table_name, l_cond = []):
        sql = "delete from {0}".format(table_name)
        if len(l_cond) > 0:
            sql += " where {0}".format(self._cond_state(l_cond))
        return sql

    def drop_sql(self, table_name):
        return "drop table {0}".format(table_name)

    def execute(self, sql, args):
        raise NotImplementedError

    def get_table_names(self):
        raise NotImplementedError


class sqlite3(database):

    def __init__(self, dbpath):
        self.dbpath = dbpath
        self.connect = None

    def __del__(self):
        if self.connect is not None:
            self.connect.commit()
            self.connect.close()
    
    def _open(self):
        import sqlite3 as sqlite3_mod
        self.connect = sqlite3_mod.connect(self.dbpath)
        self.connect.text_factory = str
    
    def db_exists(self):
        if os.path.exists(self.dbpath):
            return True
        else:
            return False

    def reset(self):
        if os.path.exists(self.dbpath):
            os.remove(self.dbpath)

    def commit(self):
        if self.connect is not None:
            self.connect.commit()

    def datetime(self, ret):
        return self.strptime(ret)

    def _ph(self, varname):
        return ":{0}".format(varname)

    def _table_key_type(self, type_str):
        if type_str == "datetime":
            return "text"
        else:
            return type_str

    def _table_key_attr(self, attr):
        if attr == "primary_key":
            return "primary key"
        elif attr == "auto_increment":
            return "autoincrement"
        elif attr == "not_null":
            return "not null"
        else:
            raise NotImplementedError

    def _index_key(self, tablekey):
        return tablekey.key
        
    def execute(self, sql, args = {}):
        #print sql
        #if len(args) > 0: print args
        if self.connect is None:
            self._open()
        cursor = self.connect.cursor()
        if len(args) == 0:
            cursor.execute(sql)
        else:
            cursor.execute(sql, args)
        return cursor

    def get_table_names(self):
        sql = "select name from sqlite_master"
        cursor = self.execute(sql)
        return [row[0] for row in cursor]


class mysql(database):

    def __init__(self, host, dbname, user, passwd):
        global MySQLdb
        import MySQLdb
        self.host = host
        self.dbname = dbname
        self.user = user
        self.passwd = passwd
        self.connect = None

    def __del__(self):
        if self.connect is not None:
            self.connect.commit()
            self.connect.close()

    def _open(self):
        if not self.db_exists():
            self._init_database()
        self.connect = MySQLdb.connect(host = self.host, db = self.dbname,
                                       user = self.user, passwd = self.passwd)

    def _init_database(self):
        connect = self._connect_root()
        cursor = connect.cursor()
        cursor.execute("create database {0}".format(self.dbname))

    def _connect_root(self):
        return MySQLdb.connect(host = self.host, user = self.user,
                               passwd = self.passwd)

    def db_exists(self):
        connect = self._connect_root()
        cursor = connect.cursor()
        cursor.execute("show databases")
        return self.dbname in [row[0] for row in cursor]

    def reset(self):
        if self.db_exists():
            connect = self._connect_root()
            cursor = connect.cursor()
            cursor.execute("drop database {0}".format(self.dbname))
            connect.commit()

    def commit(self):
        if self.connect is not None:
            self.connect.commit()

    def datetime(self, ret):
        return ret

    def _ph(self, varname):
        return "%({0})s".format(varname)

    def _table_key_type(self, type_str):
        if type_str == "integer":
            return "int"
        else:
            return type_str

    def _table_key_attr(self, attr):
        if attr == "primary_key":
            return "primary key"
        elif attr == "auto_increment":
            # differ from sqlite
            return "auto_increment"
        elif attr == "not_null":
            return "not null"
        else:
            raise NotImplementedError
    
    def _index_key(self, tablekey):
        if tablekey.type == "text":
            return "{0}({1})".format(tablekey.key, tablekey.attr[0])
        else:
            return tablekey.key

    def execute(self, sql, args = {}):
        #print sql
        #if len(args) > 0: print args
        if self.connect is None:
            self._open()
        cursor = self.connect.cursor()
        if len(args) == 0:
            cursor.execute(sql)
        else:
            cursor.execute(sql, args)
        return cursor

    def get_table_names(self):
        sql = "show tables"
        cursor = self.execute(sql)
        return [row[0] for row in cursor]


def tablekey(keyname, typename, attributes = tuple()):
    return TableKey(keyname, typename, tuple(attributes))


def cond(keyname, operand, value, replace = True):
    # value : variable name (key of args) if replace = True
    #         subquery statement if replace = False
    return Condition(keyname, operand, value, replace)


def setstate(keyname, value):
    return Setstate(keyname, value)


