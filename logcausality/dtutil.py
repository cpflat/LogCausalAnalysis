#!/usr/bin/env python
# coding: utf-8

import datetime


def discretize(l_dt, l_label, binarize = False):
    l_val = []
    l_dt_temp = l_dt[:]
    if len(l_dt_temp) > 0:
        new_dt = l_dt_temp.pop(0)
    for label in l_label:
        cnt = 0
        while new_dt is not None and new_dt < label:
            cnt += 1
            if len(l_dt_temp) > 0:
                new_dt = l_dt_temp.pop(0)
            else:
                new_dt = None
                break
        
        if cnt > 0:
            if binarize:
                l_val.append(cnt)
            else:
                l_val.append(1)
        else:
            l_val.append(0)
    return l_val


def label(top_dt, end_dt, duration):
    l_label = []
    temp_dt = top_dt + duration
    while temp_dt < end_dt:
        l_label.append(temp_dt)
        temp_dt += duration
    l_label.append(end_dt)
    return l_label


def adj_sep(dt, duration):
    # return nearest time bin before given dt
    # duration must be smaller than 1 day
    date = datetime.datetime.combine(dt.date(), datetime.time())
    diff = dt - date
    if diff == datetime.timedelta(0):
        return dt
    else:
        return date + duration * int(diff.total_seconds() // \
                                  duration.total_seconds())


def radj_sep(dt, duration):
    return adj_sep(dt, duration) + duration


def iter_term(whole_term, term_length, term_diff):
    # whole_term : tuple(datetime.datetime, datetime.datetime)
    # term_length : datetime.timedelta
    # term_diff : datetime.timedelta
    w_top_dt, w_end_dt = whole_term
    top_dt = w_top_dt
    while top_dt < w_end_dt:
        end_dt = top_dt + term_length
        yield (top_dt, end_dt)
        top_dt = top_dt + term_diff

