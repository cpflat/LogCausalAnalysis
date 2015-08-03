#!/usr/bin/env python
# coding: utf-8

import math

# data : key = datetime, value = float or int
# delta : timedelta of datetime
# length : number of time window

def cross_corr(data1, data2, length):
    s_t1 = set(data1.keys())
    s_t2 = set(data2.keys())
    avg1 = 1.0 * sum(data1.values()) / length
    avg2 = 1.0 * sum(data2.values()) / length

    cov = 0.0 # covariance
    sdev1 = 0.0 # (standard deviation)^2
    sdev2 = 0.0

    for t1, v1 in data1.iteritems():
        if data2.has_key(t1):
            # v1 & v2
            v2 = data2[t1]
            cov += 1.0 * (v1 - avg1) * (v2 - avg2)
            sdev1 += 1.0 * (v1 - avg1) ** 2
            sdev2 += 1.0 * (v2 - avg2) ** 2
        else:
            # v1 & not v2
            cov += 1.0 * (v1 - avg1) * (0 - avg2)
            sdev1 += 1.0 * (v1 - avg1) ** 2
            sdev2 += 1.0 * (0 - avg2) ** 2

    for t2 in (s_t2 - s_t1):
        # not vt1 & vt2
        v2 = data2[t2]
        cov += 1.0 * (0 - avg1) * (v2 - avg2)
        sdev1 += 1.0 * (0 - avg1) ** 2
        sdev2 += 1.0 * (v2 - avg2) ** 2

    other = length - len(s_t1 | s_t2)
    cov += 1.0 * avg1 * avg2 * other
    sdev1 += 1.0 * avg1 ** 2 * other
    sdev2 += 1.0 * avg2 ** 2 * other

    return 1.0 * cov / (math.sqrt(sdev1) * math.sqrt(sdev2))

def self_corr(data, delta, length):
    data2 = {}
    for dt, v in data.iteritems():
        data2[dt + delta] = v
    return cross_corr(data, data2, length)


