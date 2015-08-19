#!/usr/bin/env python
# coding: utf-8

import sys
import datetime
import re

import fslib
import config

_config = config.common_config()

MONTH_NAME = ("Jan", "Feb", "Mar", "Apr", "May", "Jun",
        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")

RE_DATETIME = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}")
RE_YEAR = re.compile(r"^[12]\d{3}$")

RM_HEADER = []
for fn in _config.getlist("database", "remove_header_filename"):
    try:
        with open(fn, 'r'):
            for line in f:
                RM_HEADER.append(line.rstrip("\n"))
    except IOError:
        pass


def split_header(line):

    def pop_string(line):
        string, line = line.split(" ", 1)
        return string, line

    def str2month(string):
        return MONTH_NAME.index(string) + 1

    src_line = line
    if RE_DATETIME.match(line):
        date_str, line = pop_string(line)
        time_str, line = pop_string(line)
        dt = datetime.datetime.strptime(" ".join((date_str, time_str)),
                "%Y-%m-%d %H:%M:%S")
        host, line = pop_string(line)
        message = line
    else:
        string, line = pop_string(line)
        if RE_YEAR.match(string):
            year = int(string)
            month_str, line = pop_string(line)
            month = str2month(month_str)
        else:
            year = give_year()
            month = str2month(string)
        day_str, line = pop_string(line)
        day = int(day_str)
        time_str, line = pop_string(line)
        hour, minute, second = tuple(int(e) for e in time_str.split(":"))
        host, line = pop_string(line)
        message = line
        dt = datetime.datetime(year = year, month = month, day = day, 
                hour = hour, minute = minute, second = second, microsecond = 0)

    info = {"timestamp" : dt, "hostname" : host}

    if is_removed(message):
        return None, None
    else:
        return message, info 


def give_year():
    default_year = _config.getint("database", "default_year")
    if default_year is None:
        return datetime.datetime.today().year
    else:
        return default_year


def is_removed(line):
    for description in RM_HEADER:
        if description in line:
            return True
    else:
        return False


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit("usage: {0} targets".format(sys.argv[0]))
    for fp in fslib.rep_dir(sys.argv[1:]):
        with open(fp) as f:
            for line in f:
                print split_header(line.rstrip("\n"))

