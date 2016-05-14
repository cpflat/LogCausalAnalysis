#!/usr/bin/env python
# coding: utf-8

import datetime
import numpy as np
from matplotlib.colors import LinearSegmentedColormap
import cPickle as pickle

def generate_cmap(colors):
    values = range(len(colors))

    vmax = np.ceil(np.max(values))
    color_list = []
    for v, c in zip(values, colors):
        color_list.append( ( v/ vmax, c) )
    return LinearSegmentedColormap.from_list('custom_cmap', color_list)


def dt_ticks(value_term, dt_term, dt_bin, duration = None, recent = False):
    """
    Args:
        value_term (int, int)
        dt_term (datetime.datetime, datetime.datetime):
                The range of datetime for ticks, corresponding to value_term.
        dt_bin (datetime.timedelta): Bin length in datetime.
        duration (datetime.timedelta): Duration of ticks. If None,
                automatically decided in this function.
        recent (bool): In default, ticks are decided from old datetime.
                If recent is true, ticks are decided from recent datetime.
    """
    import dtutil
    top_dt, end_dt = dt_term
    if duration is None:
        whole_term = end_dt - top_dt
        if whole_term <= datetime.timedelta(minutes = 3):
            duration = datetime.timedelta(seconds = 10)
        elif whole_term <= datetime.timedelta(minutes = 10):
            duration = datetime.timedelta(minutes = 1)
        elif whole_term <= datetime.timedelta(minutes = 30):
            duration = datetime.timedelta(minutes = 3)
        elif whole_term <= datetime.timedelta(hours = 1):
            duration = datetime.timedelta(minutes = 5)
        elif whole_term <= datetime.timedelta(hours = 6):
            duration = datetime.timedelta(minutes = 10)
        elif whole_term <= datetime.timedelta(days = 1):
            duration = datetime.timedelta(hours = 6)
        elif whole_term <= datetime.timedelta(days = 7):
            duration = datetime.timedelta(days = 1)
        elif whole_term <= datetime.timedelta(days = 14):
            duration = datetime.timedelta(days = 2)
        elif whole_term <= datetime.timedelta(days = 60):
            duration = datetime.timedelta(days = 7)
        elif whole_term <= datetime.timedelta(days = 300):
            duration = datetime.timedelta(days = 20)
        else:
            duration = datetime.timedelta(days = 50)

    if duration >= datetime.timedelta(days = 1):
        adjsearch_dur = datetime.timedelta(days = 1)
        dtstr = lambda dt: dt.strftime("%Y-%m-%d")
    else:
        adjsearch_dur = duration
        dtstr = lambda dt: dt.strftime("%H:%M:%S")

    ticks_label = []
    if recent is False:
        temp_dt = dtutil.radj_sep(top_dt, adjsearch_dur)
        assert(temp_dt <= end_dt)
        ticks_label.append(temp_dt)
        while 1:
            temp_dt = temp_dt + duration
            if temp_dt > end_dt:
                break
            else:
                ticks_label.append(temp_dt)
    else:
        temp_dt = dtutil.adj_sep(end_dt, adjsearch_dur)
        assert(temp_dt >= top_dt)
        ticks_label.append(temp_dt)
        while 1:
            temp_dt = temp_dt - duration
            if temp_dt < top_dt:
                break
            else:
                ticks_label.append(temp_dt)
        ticks_label.sort()

    val_label = range(value_term[0], value_term[1] + 1, 1)
    dt_label = dtutil.dtrange(top_dt, end_dt, dt_bin, include_end = True)
    ticks_values = []
    ticks_dts = ticks_label[:]
    for val, dt in zip(val_label, dt_label):
        if dt == ticks_dts[0]:
            ticks_dts.pop(0)
            ticks_values.append(val)
            if len(ticks_dts) == 0:
                break
    return ticks_values, [dtstr(l) for l in ticks_label]


def dump(fn, data):
    with open(fn, "w") as f:
        pickle.dump(data, f)


def load(fn):
    with open(fn, "r") as f:
        r = pickle.load(f)
    return r





