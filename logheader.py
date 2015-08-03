#!/usr/bin/env python
# coding: utf-8

import sys
import datetime
import re

import fslib

MONTH_NAME = ("Jan", "Feb", "Mar", "Apr", "May", "Jun",
        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")

FREEWRITE_DESCRIPTION = ("KEY ",
                    "UI_CMDLINE_READ_LINE: ",
                    "UI_COMMIT_PROGRESS: ")

RE_DATETIME = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}")
RE_YEAR = re.compile(r"^[12]\d{3}$")

def split_header(line):
    l_line = line.split(None, 5)
    if RE_DATETIME.match(line):
        dt = datetime.datetime.strptime(" ".join(l_line[0:2]),
                "%Y-%m-%d %H:%M:%S")
        host = l_line[2]
        message = " ".join(l_line[3:])
    elif RE_YEAR.match(l_line[0]):
        year = int(l_line[0])
        month = MONTH_NAME.index(l_line[1]) + 1
        day = int(l_line[2])
        timestr = l_line[3]
        hour, minute, second = tuple(int(e) for e in timestr.split(":"))
        host = l_line[4]
        message = l_line[-1]
        dt = datetime.datetime(year = year, month = month, day = day, 
                hour = hour, minute = minute, second = second, microsecond = 0)
    else:
        year = datetime.datetime.today().year
        month = MONTH_NAME.index(l_line[0]) + 1
        day = int(l_line[1])
        timestr = l_line[2]
        hour, minute, second = tuple(int(e) for e in timestr.split(":"))
        host = l_line[3]
        message = l_line[-1]
        dt = datetime.datetime(year = year, month = month, day = day, 
                hour = hour, minute = minute, second = second, microsecond = 0)

    info = {"timestamp" : dt, "hostname" : host}

    if is_freewrite(message):
        return None, None
    else:
        return message, info 


def is_freewrite(line):
    for description in FREEWRITE_DESCRIPTION:
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

