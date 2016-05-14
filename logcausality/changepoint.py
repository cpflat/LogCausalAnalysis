#!/usr/bin/env python
# coding: utf-8

import sys
import os
import logging
import cPickle as pickle
import changefinder

import fslib
import dtutil
import config
import log_db
import log2event

_logger = logging.getLogger(__name__.rpartition(".")[-1])


class ChangePointData():

    def __init__(self, conf):
        self.fn = conf.get("changepoint", "temp_changepoint_data")
        self._binsize = conf.getdur("changepoint", "cf_bin")
        self._cf_r = conf.getfloat("changepoint", "cf_r")
        self._cf_smooth = conf.getint("changepoint", "cf_smooth")
        self._d_cf = {}  # key = evdef, val = changefinder.ChangeFinder
        self._d_data = {}  # key = evdef, val = [cnt, ...]
        self._d_score = {}  # key = evdef, val = [score, ...]
        self._top_dt = None
        self._end_dt = None
        self._dt_label = []

    def _new_cf(self):
        return changefinder.ChangeFinder(r = self._cf_r,
                order = 1, smooth = self._cf_smooth)

    def term(self):
        return self._top_dt, self._end_dt

    def binsize(self):
        return self._binsize

    def len_evdef(self):
        return len(self._d_cf)

    def iter_evdef(self):
        return self._d_cf.iterkeys()

    def get(self, evdef, top_dt = None, end_dt = None):
        if top_dt is None:
            top_dt = self._top_dt
        if end_dt is None:
            end_dt = self._end_dt
        l_label = self._dt_label
        l_data = self._d_data[evdef]
        l_score = self._d_score[evdef]
        if dtutil.is_sep(top_dt, self._binsize):
            top_index = l_label.index(top_dt)
        else:
            top_index = l_label.index(dtutil.adj_sep(top_dt, self._binsize))
        if dtutil.is_sep(end_dt, self._binsize):
            end_index = l_label.index(end_dt)
        else:
            end_index = l_label.index(dtutil.radj_sep(end_dt, self._binsize))
        return zip(self._dt_label, l_data, l_score)[top_index:end_index]

    def update(self, conf):
        ld = log_db.LogData(conf)
        db_top_dt, db_end_dt = ld.dt_term()
        if self._end_dt is not None and \
                self._end_dt + self._binsize < db_end_dt:
            _logger.warning("New data is too small or not found")
            return

        edict, evmap = log2event.log2event(conf, ld, top_dt = self._end_dt,
                end_dt = db_end_dt, area = "all")

        if self._end_dt is None:
            top_dt = dtutil.adj_sep(db_top_dt, self._binsize)
        else:
            top_dt = self._end_dt
        # The last bin will not be added, because it may be uncompleted
        end_dt = dtutil.adj_sep(db_end_dt, self._binsize)
        l_label = dtutil.label(top_dt, end_dt, self._binsize)
        _logger.info("new data : {0} - {1}".format(top_dt, end_dt))

        for eid, l_dt in edict.iteritems():
            evdef = evmap.info(eid)
            _logger.info("processing {0}".format(evmap.info_str(eid)))
            if not self._d_cf.has_key(evdef):
                self._d_cf[evdef] = self._new_cf()
                self._d_data[evdef] = []
                self._d_score[evdef] = []

            l_val = dtutil.discretize(l_dt, l_label, binarize = False)
            for val in l_val:
                self._d_data[evdef].append(val)
                score = self._d_cf[evdef].update(val)
                self._d_score[evdef].append(score)

        self._end_dt = end_dt
        self._dt_label += l_label
        if self._top_dt is None:
            self._top_dt = top_dt
        _logger.info("task completed")

    def dump(self, fn = None):
        if fn is None:
            fn = self.fn
        obj = self.__dict__
        with open(fn, "w") as f:
            pickle.dump(obj, f)

    def load(self, fn = None):
        if fn is None:
            fn = self.fn
        if os.path.exists(fn):
            with open(fn, "r") as f:
                obj = pickle.load(f)
            self.__dict__.update(obj)
        return self


def graph_cp(conf, dur, dirname):
    fslib.mkdir(dirname)
    length = config.str2dur(dur)
    cpd = ChangePointData(conf)
    cpd.load()
    cpd_top_dt, cpd_end_dt = cpd.term()
    top_dt = cpd_end_dt - length
    if top_dt < cpd.term()[0]:
        top_dt = cpd.term()[0]
    end_dt = cpd_end_dt

    for evdef in cpd.iter_evdef():
        fn = "{0}_{1}.pdf".format(evdef.host, evdef.gid)
        l_label, l_data, l_score = zip(*cpd.get(evdef, top_dt, end_dt))

        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        fig = plt.figure()
        ax = fig.add_subplot(111)
        ax.plot(l_label, l_data, "r")
        ax2 = ax.twinx()
        ax2.plot(l_label, l_score)
        import matplotlib.dates as mdates
        days = mdates.WeekdayLocator()
        daysFmt = mdates.DateFormatter('%m-%d')
        ax.xaxis.set_major_locator(days)
        ax.xaxis.set_major_formatter(daysFmt)
        plt.savefig(dirname + "/" + fn)
        plt.close()


def heat_score(conf, dur, filename):
    import numpy as np
    length = config.str2dur(dur)
    cpd = ChangePointData(conf)
    cpd.load()
    cpd_top_dt, cpd_end_dt = cpd.term()
    top_dt = cpd_end_dt - length
    if top_dt < cpd.term()[0]:
        top_dt = cpd.term()[0]
    end_dt = cpd_end_dt

    result = []
    for evdef in cpd.iter_evdef():
        l_label, l_data, l_score = zip(*cpd.get(evdef, top_dt, end_dt))
        result.append(l_score)
    else:
        xlen = len(l_label)
        ylen = cpd.len_evdef()
    data = np.array(result)
    #data = np.array([[np.log(float(score)) for score in l_score]
    #        for l_score in result])

    length = len(l_label)
    print xlen
    print ylen
    x, y = np.meshgrid(np.arange(xlen + 1), np.arange(ylen + 1))
    print x
    print y

    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import explot
    import matplotlib.colors
    cm = explot.generate_cmap(["orangered", "white"])
    #plt.pcolormesh(x, y, data, cmap = cm)
    plt.pcolormesh(x, y, data, norm=matplotlib.colors.LogNorm(
            vmin=max(data.min(), 1.0), vmax=data.max()), cmap = cm)
    xt_v, xt_l = explot.dt_ticks((0, xlen), (top_dt, end_dt),
            cpd.binsize(), recent = True)
    #import pdb; pdb.set_trace()
    plt.xticks(xt_v, xt_l, rotation = 336)
    plt.xlim(xmax = xlen)
    plt.ylim(ymax = ylen)
    plt.colorbar()
    plt.savefig(filename)


if __name__ == "__main__":
    usage = """
usage: {0} [options] args...
args:
    make : make / update changepoint detection object
    graph : generate graphs for loaded changepoint detection object
    """.strip().format(sys.argv[0])
    import optparse
    op = optparse.OptionParser(usage)
    op.add_option("-c", "--config", action="store",
            dest="conf", type="string", default=config.DEFAULT_CONFIG_NAME,
            help="configuration file path")
    options, args = op.parse_args()

    conf = config.open_config(options.conf)
    config.set_common_logging(conf, _logger)

    if len(args) == 0:
        sys.exit(usage)
    mode = args.pop(0)
    if mode == "make":
        cpd = ChangePointData(conf)
        cpd.update(conf)
        cpd.dump()
    elif mode == "update":
        cpd = ChangePointData(conf)
        cpd.load()
        cpd.update(conf)
        cpd.dump()
    elif mode == "graph":
        if len(args) < 2:
            sys.exit("give me term length and output directory name of graphs")
        graph_cp(conf, args[0], args[1])
    elif mode == "heat":
        if len(args) < 2:
            sys.exit("give me term length and output filename of graphs")
        heat_score(conf, args[0], args[1])
    else:
        raise NotImplementedError


