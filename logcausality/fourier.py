#!/usr/bin/env python
# coding: utf-8

"""Remove or replace periodic events based on fourier analysis."""

import datetime
import logging

import numpy as np
import scipy.fftpack
import scipy.signal

round_int = lambda x: int(x + 0.5)
_logger = logging.getLogger(__name__.rpartition(".")[-1])


def remove(conf, l_stat, binsize):
    if sum(l_stat) == 0:
        return False, None
    p_cnt = conf.getint("filter", "periodic_count")
    p_term = conf.getdur("filter", "periodic_term")
    if not is_enough_long(l_stat, p_cnt, p_term, binsize):
        return False, None

    th_spec = conf.getfloat("filter", "threshold_spec")
    th_std = conf.getfloat("filter", "threshold_eval")
    data = l_stat[-power2(len(l_stat)):]
    fdata = scipy.fftpack.fft(data)
    flag, interval = is_periodic(data, fdata, binsize, th_spec, th_std)

    return flag, interval


def replace(conf, l_stat, binsize):
    if sum(l_stat) == 0:
        return False, None, None
    p_cnt = conf.getint("filter", "periodic_count")
    p_term = conf.getdur("filter", "periodic_term")
    if not is_enough_long(l_stat, p_cnt, p_term, binsize):
        return False, None, None

    th_spec = conf.getfloat("filter", "threshold_spec")
    th_std = conf.getfloat("filter", "threshold_eval")
    th_restore = conf.getfloat("filter", "threshold_restore")

    data = l_stat[-power2(len(l_stat)):]
    fdata = scipy.fftpack.fft(data)
    flag, interval = is_periodic(data, fdata, binsize, th_spec, th_std)
    if flag:
        data_filtered = part_filtered(data, fdata, binsize, th_spec)
        data_remain = restore_data(data, data_filtered, th_restore)
        return True, data_remain, interval
    else:
        return False, None, None


def is_periodic(data, fdata, binsize, th_spec, th_std):
    peak_order = 1
    peaks = 101

    dt = binsize.total_seconds()
    a_label = scipy.fftpack.fftfreq(len(data), d = dt)[1:int(0.5 * len(data))]
    a_spec = np.abs(fdata)[1:int(0.5 * len(data))]
    max_spec = max(a_spec)
    a_peak = scipy.signal.argrelmax(a_spec, order = peak_order)

    l_interval = []
    prev_freq = 0.0
    for freq, spec in (np.array([a_label, a_spec]).T)[a_peak]:
        if spec > th_spec * max_spec:
            interval = freq - prev_freq
            l_interval.append(interval)
            prev_freq = freq
        else:
            pass
    if len(l_interval) == 0:
        return False, None

    dist = np.array(l_interval[:(peaks - 1)])
    std = np.std(dist)
    mean = np.mean(dist)
    val = 1.0 * std / mean
    interval = round_int(1.0 / np.median(dist)) * datetime.timedelta(
            seconds = 1)
    return val < th_std, interval


def part_filtered(data, fdata, binsize, th_spec):
    sf_filtered = set()
    dt = binsize.total_seconds()
    
    a_label = scipy.fftpack.fftfreq(len(data), d = dt)
    a_spec = np.abs(fdata)
    max_spec = max(allspec)
    sf_filtered = {freq for freq, spec in zip(a_label, a_spec)
            if spec > th_spec * max_spec}

    fdata_filtered = np.array([])
    for freq, spec in zip(a_label, a_spec): 
        if freq in sf_filtered:
            fdata_filtered = np.append(fdata_filtered, fcond)
        else:
            fdata_filtered = np.append(fdata_filtered, np.complex(0))
    data_filtered = np.real(scipy.fftpack.ifft(fdata_filtered))
    return data_filtered


def restore_data(data, data_filtered, th_restore):
    threshold_restore = th_restore * max(data_filtered)

    l_ind = []
    for ind, (d1, d2) in enumerate(zip(data, data_filtered)):
        if d1 == 0:
            pass
        elif d2 >= threshold_restore:
            l_ind.append(ind)
        else:
            pass
    # original values on places of periodic appearance
    l_val = np.array(data)[l_ind]

    periodic_cnt = np.median(l_val)
    data_periodic = np.array([0] * len(data))
    data_periodic[l_ind] = periodic_cnt
    data_remain = data - data_periodic
    return data_remain


def is_enough_long(l_stat, p_cnt, p_term, binsize):
    if sum(l_stat) < p_cnt:
        _logger.debug(
                "Event appearance is too small, skip periodicity test")
        return False
    #elif max(l_dt) - min(l_dt) < p_term:
    l_index = [ind for ind, val in enumerate(l_stat) if val > 0]
    length = (max(l_index) - min(l_index)) * binsize
    if length < p_term:
        _logger.debug(
                "Event appearing term is too short, skip periodicity test")
        return False
    else:
        return True


def power2(length):
    return 2 ** int(np.log2(length))
