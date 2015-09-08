#!/usr/bin/env python
# coding: utf-8

import sys
import datetime
import optparse

import config
import fslib
import log_db
import logparser
import lt_shiso

def measure_ltgen(default_conf_name, section, option, l_value, sflag):

    l_secopt = [("general", "info_log"),
            #("database", "db_filename"),
            ("log_template", "db_filename")]
    conf = config.open_config(default_conf_name)
    d_default = {(sec, opt) : conf.get(sec, opt) for sec, opt in l_secopt}
    l_conf_name = []
    l_tempfile = [] # for cleaning

    for value in l_value:
        conf.set(section, option, value)
        for sec, opt in l_secopt:
            temp_path = ".".join((d_default[(sec, opt)], str(value)))
            conf.set(sec, opt, temp_path)
            l_tempfile.append(temp_path)
        conf_name = ".".join((default_conf_name, str(value)))
        with open(conf_name, 'w') as f:
            conf.write(f)
        l_conf_name.append(conf_name)
        l_tempfile.append(conf_name)

    print("default config : {0}".format(default_conf_name))
    print("section : {0}".format(section))
    print("option : {0}".format(option))

    l_logger = ["log_db", "lt_shiso", "lt_common"]
    for value, conf_name in zip(l_value, l_conf_name):
        start_dt = datetime.datetime.now()
        conf = config.open_config(conf_name)
        lch = config.set_common_logging(conf, None, l_logger)
        lt_shiso.test_ltgen(conf)
        config.release_common_logging(lch, None, l_logger)
        end_dt = datetime.datetime.now()
        print("trial with value({0}) : {1}".format(value, end_dt - start_dt))

    default_conf = config.open_config(default_conf_name)
    config.set_common_logging(default_conf, None, l_logger)

    start_dt = datetime.datetime.now()
    logparser.test_parse(default_conf)
    end_dt = datetime.datetime.now()
    print("header processing : {0}".format(end_dt - start_dt))

    start_dt = datetime.datetime.now()
    log_db.construct_db(default_conf)
    end_dt = datetime.datetime.now()
    print("db construction (ltgen + db add) : {0}".format(end_dt - start_dt))

    config.release_common_logging(lch, None, l_logger)
    if sflag == False:
        for fp in l_tempfile:
            fslib.rm(fp)
        print("temp data removed")

if __name__ == "__main__":
    usage = "usage: %s [options] section option values..." % sys.argv[0]
    op = optparse.OptionParser(usage)
    op.add_option("-c", "--config", action="store",
            dest="conf", type="string", default=config.DEFAULT_CONFIG_NAME,
            help="default configuration file path")
    op.add_option("-s", "--save", action="store_true",
            dest="sflag", default=False,
            help="do not remove temporaly data")
    (options, args) = op.parse_args()

    measure_ltgen(options.conf, args[0], args[1], args[2:], options.sflag)
    #config.set_common_logging(conf, _logger, [])
