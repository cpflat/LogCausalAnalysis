#!/usr/bin/env python
# coding: utf-8

import sys
import optparse
import time
import datetime

import config
import log_db

def view(conf, ltid, ltgid, top_dt, end_dt, host, area, oflag):

    ld = log_db.LogData(conf)
    for e in ld.iter_lines(ltid = ltid, ltgid = ltgid, top_dt = top_dt,
            end_dt = end_dt, host = host, area = area):
        if oflag:
            print e.restore_line()
        else:
            print e


if __name__ == "__main__":

    usage = "usage: {0} [options]".format(sys.argv[0])
    import optparse
    op = optparse.OptionParser(usage)
    op.add_option("-c", "--config", action="store",
            dest="conf", type="string", default=config.DEFAULT_CONFIG_NAME,
            help="configuration file path")
    op.add_option("-a", "--area", action="store", dest="area", type="string",
            default=None, help="Host area name")
    op.add_option("-l", "--ltid", action="store", dest="ltid", type="int",
            default=None, help="Log template identifier")
    op.add_option("-g", "--ltgid", action="store", dest="ltgid", type="int",
            default=None, help="Log template group identifier")
    op.add_option("-n", "--host", action="store", dest="host", type="string",
            default=None, help="Hostname")
    op.add_option("-d", "--date", action="store", dest="date", type="string",
            default=None, help="date")
    op.add_option("-m", "--month", action="store", dest="month", type="string",
            default=None, help="month")
    op.add_option("-o", "--original", action="store_true", dest="oflag",
            default=False, help="output as original log message")
    (options, args) = op.parse_args()
    if len(sys.argv) == 1: sys.exit(usage) # No option, avoid all dump

    conf = config.open_config(options.conf)
    if options.date is not None and options.month is not None:
        raise ValueError("date and month are competitive, use either")
    elif options.date is not None:
        top_dt = datetime.datetime.strptime(options.date, "%Y-%m-%d")
        end_dt = top_dt + datetime.timedelta(days = 1)
    elif options.month is not None:
        top_dt = datetime.datetime.strptime(options.month, "%Y-%m")
        end_dt = datetime.datetime.fromtimestamp(
                time.mktime((top_dt.year, top_dt.month + 1, 1,
                    0, 0, 0, 0, 0, 0)))
    else:
        top_dt = None; end_dt = None

    view(conf, options.ltid, options.ltgid, top_dt, end_dt, options.host,
            options.area, options.oflag)

