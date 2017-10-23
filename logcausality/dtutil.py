#!/usr/bin/env python
# coding: utf-8

import datetime
import logging
import random
import numpy as np
#from itertools import chain

TIMEFMT = "%Y-%m-%d %H:%M:%S"
_logger = logging.getLogger(__package__)


def dtrange(top_dt, end_dt, duration, include_end = False):
    """
    """
    temp_dt = top_dt
    while temp_dt < end_dt or (include_end is True and temp_dt == end_dt):
        yield temp_dt
        temp_dt = temp_dt + duration


#def dtrange_term(top_dt, end_dt, duration):
#    temp_top_dt = top_dt
#    while temp_top_dt < end_dt:
#        temp_end_dt = temp_top_dt + duration
#        yield (temp_top_dt, temp_end_dt)
#        temp_top_dt = temp_end_dt
# -> iter_term


def discretize(l_dt, l_label, method = "count", binarize = False):
    """
    Args:
        l_dt (List[datetime.datetime]): An input datetime sequence.
        l_label (List[datetime.datetime]): A sequence of separating times
                of data bins. The number of labels is equal to
                number of bins + 1. (Including the end of data term)
        method (str): Returned data style. "count" returns the number of
                object in each bin. "binary" returns 0 or 1 for each bin.
                (1 means some object is in the bin.) "datetime" returns
                the list of datetime object in each bin.
        binarize (bool): If True, return 0 or 1 for each bin. 1 means
                some datetime found in l_dt.
                This is argument only for comparibility.
                Use "method" instead of this argument.
    """

    def return_empty(size, method):
        if method in ("count", "binary"):
            return [0] * bin_num
        elif method == "datetime":
            return [[] for i in range(bin_num)]
        else:
            raise NotImplementedError(
                "Invalid method name ({0})".format(method))

    def init_tempobj(method):
        if method == "count":
            return 0
        elif method == "binary":
            return 0
        elif method == "datetime":
            return []
        else:
            raise NotImplementedError(
                "Invalid method name ({0})".format(method))

    def update_tempobj(temp, new_dt, method):
        if method == "count":
            return temp + 1
        elif method == "binary":
            return 1
        elif method == "datetime":
            temp.append(new_dt)
            return temp
        else:
            raise NotImplementedError(
                "Invalid method name ({0})".format(method))

    if binarize:
        method = "binary"

    bin_num = len(l_label) - 1
    l_dt_temp = sorted(l_dt)
    if len(l_dt_temp) <= 0:
        return_empty(bin_num, method)

    iterobj = iter(l_dt_temp)
    try:
        new_dt = next(iterobj)
    except StopIteration:
        raise ValueError("Not empty list, but failed to get initial value")
    while new_dt < l_label[0]:
        try:
            new_dt = next(iterobj)
        except StopIteration:
            return_empty(bin_num, method)

    ret = []
    stop = False
    for label_dt in l_label[1:]:
        temp = init_tempobj(method)
        if stop:
            ret.append(temp)
            continue
        while new_dt < label_dt:
            temp = update_tempobj(temp, new_dt, method)
            try:
                new_dt = next(iterobj)
            except StopIteration:
                # "stop" make data after label term be ignored
                stop = True
                break
        ret.append(temp)
    return ret


#def discretize(l_dt, l_label, binarize = False):
#    """
#    Args:
#        l_dt (List[datetime.datetime]): An input datetime sequence.
#        l_label (List[datetime.datetime]): A sequence of separating times
#                of data bins. The number of labels is equal to
#                number of bins + 1. (Including the end of data term)
#        binarize (bool): If True, return 0 or 1 for each bin. 1 means
#                some datetime found in l_dt.
#    """
#    l_val = []
#    l_dt_temp = sorted(l_dt)
#    if len(l_dt_temp) > 0:
#        new_dt = l_dt_temp.pop(0)
#    else:
#        _logger.warning("l_dt is empty")
#        return None
#
#    # remove data before label term
#    while new_dt < l_label[0]:
#        if len(l_dt_temp) > 0:
#            new_dt = l_dt_temp.pop(0)
#        else:
#            _logger.warning("all datetime values are out of given label term")
#            return []
#    for label_dt in l_label[1:]:
#        cnt = 0
#        while new_dt is not None and new_dt < label_dt:
#            cnt += 1
#            if len(l_dt_temp) > 0:
#                new_dt = l_dt_temp.pop(0)
#            else:
#                new_dt = None
#                break
#        
#        if cnt > 0:
#            if binarize:
#                l_val.append(1)
#            else:
#                l_val.append(cnt)
#        else:
#            l_val.append(0)
#    # data after label term is ignored
#    return l_val


def auto_discretize(l_dt, binsize, dt_range = None, binarize = False):
    """
    Args:
        l_dt (List[datetime.datetime])
        binsize (datetime.timedelta)
    """
    if binsize == datetime.timedelta(seconds = 1):
        l_label = l_dt
    else:
        if dt_range is None:
            top_dt = adj_sep(min(l_dt), binsize)
            end_dt = radj_sep(max(l_dt), binsize)
        else:
            top_dt, end_dt = dt_range
        l_label = label((top_dt, end_dt), binsize)
    return discretize(l_dt, l_label, binarize = binarize)


def auto_discretize_slide(l_dt, binsize, slide,
                          dt_range = None, method = "count", binarize = False):
    #assert slide <= binsize
    if dt_range is None:
        top_dt = adj_sep(min(l_dt), binsize)
        end_dt = radj_sep(max(l_dt), binsize)
    else:
        top_dt, end_dt = dt_range
    if binarize:
        method = "binary"

    if binsize < slide:
        _logger.warning("binsize is smaller than slide, "
                        "which means there is time-series sampling gap")
    slide_width = max(int(binsize.total_seconds() / slide.total_seconds()), 1)
    l_top = label((top_dt, end_dt), slide)[:-1]
    l_end = [min(t + binsize, end_dt) for t in l_top]

    ret = []
    noslide = discretize(l_dt, l_top + [end_dt], method = "datetime")

    for i, bin_end in enumerate(l_end):
        #slide_area = chain.from_iterable(noslide[i:i+slide_width])
        l_dt_temp = []
        for b in noslide[i:i+slide_width]:
            l_dt_temp.extend([dt for dt in b if dt <= bin_end])
        
        if method == "count":
            ret.append(len(l_dt_temp))
        elif method == "binary":
            if len(l_dt_temp) > 0:
                ret.append(1)
            else:
                ret.append(0)
        else:
            raise NotImplementedError(
                "Invalid method name ({0})".format(method))

    return ret


def periodic(dt_range, interval):
    top_dt, end_dt = dt_range
    l_label = []
    #temp_dt = top_dt + duration
    temp_dt = top_dt
    while temp_dt < end_dt:
        l_label.append(temp_dt)
        temp_dt += interval
    l_label.append(end_dt)
    return l_label


def label(dt_range, duration):
    return periodic(dt_range, duration)


def is_sep(dt, duration):
    return adj_sep(dt, duration) == dt


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
    if is_sep(dt, duration):
        return dt
    else:
        return adj_sep(dt, duration) + duration


def limit_dt_seq(l_dt, top_dt, end_dt):
    """List[dt]: Get a sequence of dt that satisfy top_dt <= dt < end_dt."""
    if len(l_dt) == 0:
        return

    ret = []
    l_dt = l_dt[:]
    dt = l_dt.pop(0)
    # pass dts before top_dt
    while dt < top_dt:
        if len(l_dt) == 0:
            return []
        else:
            dt = l_dt.pop(0)

    while dt < end_dt:
        ret.append(dt)
        if len(l_dt) == 0:
            break
        else:
            dt = l_dt.pop(0)
    return ret


def shortstr(dt):
    date = datetime.datetime.combine(dt.date(), datetime.time())
    if date == dt:
        return dt.strftime("%Y%m%d")
    else:
        return dt.strftime("%Y%m%d_%H%M%S")


def iter_term(whole_term, term_length, term_diff = None):
    # whole_term : tuple(datetime.datetime, datetime.datetime)
    # term_length : datetime.timedelta
    # term_diff : datetime.timedelta
    if term_diff is None:
        term_diff = term_length
    w_top_dt, w_end_dt = whole_term
    top_dt = w_top_dt
    while top_dt < w_end_dt:
        end_dt = top_dt + term_length
        yield (top_dt, end_dt)
        top_dt = top_dt + term_diff


def separate_periodic_dup(data, dur, err):
    """Separate periodic components from a sequence of timestamps that allow
    duplication of timestamps in 1 periodic sequence. 

    Args:
        data (List[datetime.datetime]): A sequence of timestamps
            to be classified into periodic and non-periodic timestamps.
        dur (datetime.timedelta): Duration of periodicity.
        err (float): Allowed error of periodicity duration.

    Returns:
        List[List[datetime.datetime]]: A set of sequences
            of periodic timestamps.
        List[datetime.datetime]: A set of timestamps that do not belong to
            periodic timestamp sequences.
    
    """
    def _separate_same(target_dt, l_dt, err_dur):
        # return same, others
        for cnt, dt in enumerate(l_dt):
            if target_dt + err_dur <= dt:
                # No other (almost) same dt
                return l_dt[:cnt], l_dt[cnt:]
        else:
            # all dt in l_dt are (almost) same
            return l_dt, []


    def _has_adjacent(target_dt, l_dt, max_dur, min_dur):
        for cnt, dt in enumerate(l_dt):
            if target_dt + max_dur < dt:
                # No adjacent timestamp
                return None
            elif target_dt + min_dur <= dt:
                # Adjacent timestamp found
                return cnt
            else:
                # Search continues
                pass
        else:
            # No adjacent timestamp
            return None

    ret = []
    err_dur = datetime.timedelta(seconds = int(dur.total_seconds() * err))
    max_dur = dur + err_dur
    min_dur = dur - err_dur

    remain_dt = sorted(data)
    while True:
        l_dt = remain_dt
        length = len(l_dt)
        seq = []
        remain_dt = []
        while len(l_dt) > 0:
            target_dt = l_dt.pop(0)
            adj = _has_adjacent(target_dt, l_dt, max_dur, min_dur)
            if adj is None:
                if len(seq) == 0:
                    # No adjacent dt with target_dt
                    remain_dt.append(target_dt)
                else:
                    # No other adjacent members of this seq
                    seq.append(target_dt)
                    l_same, l_others = _separate_same(target_dt, l_dt, err_dur)
                    seq += l_same
                    remain_dt += l_others
                    break
            else:
                seq.append(target_dt)
                cand = l_dt[:adj]
                    # Candidate of same dt (no adjacent dt in cand)
                l_same, l_others = _separate_same(target_dt, cand, err_dur)
                seq += l_same
                remain_dt += l_others
                l_dt = l_dt[adj:]
        
        assert len(seq) + len(remain_dt) == length
        if len(seq) == 0:
            break
        else:
            ret.append(seq)

    return ret, remain_dt


def separate_periodic(data, dur, err):
    """Separate periodic components from a sequence of timestamps that do not
    allow duplication of timestamps in 1 periodic sequence. 

    Args:
        data (List[datetime.datetime]): A sequence of timestamps
            to be classified into periodic and non-periodic timestamps.
        dur (datetime.timedelta): Duration of periodicity.
        err (float): Allowed error of periodicity duration.

    Returns:
        List[List[datetime.datetime]]: A set of sequences
            of periodic timestamps.
        List[datetime.datetime]: A set of timestamps that does not belong to
            periodic timestamp sequences.
    
    """
    def _adjacents(target_dt, l_dt, max_dur, min_dur):
        top_id = None; end_id = None
        for cnt, dt in enumerate(l_dt):
            if target_dt + max_dur < dt:
                end_id = cnt
                break
            elif (target_dt + min_dur <= dt) and (top_id is None):
                top_id = cnt
            else:
                pass
        if top_id is None:
            return []
        elif end_id is None:
            return list(enumerate(l_dt))[top_id:]
        else:
            return list(enumerate(l_dt))[top_id:end_id]

    def _adj_small_err(target_dt, l_adj, dur):
        if len(l_adj) == 1:
            return l_adj[0][0]
        else:
            return min(l_adj, key = lambda x: abs(
                (target_dt + dur - x[1]).total_seconds()))[0]

    ret = []
    err_dur = datetime.timedelta(seconds = int(dur.total_seconds() * err))
    max_dur = dur + err_dur
    min_dur = dur - err_dur

    remain_dt = sorted(data)
    while True:
        l_dt = remain_dt
        length = len(l_dt)
        seq = []
        remain_dt = []
        while len(l_dt) > 0:
            target_dt = l_dt.pop(0)
            l_adj = _adjacents(target_dt, l_dt, max_dur, min_dur)
            if len(l_adj) == 0:
                if len(seq) == 0:
                    # No adjacent dt with target_dt
                    remain_dt.append(target_dt)
                else:
                    # No other adjacent members of this seq
                    seq.append(target_dt)
                    remain_dt += l_dt
                    break
            else:            
                seq.append(target_dt)
                adj = _adj_small_err(target_dt, l_adj, dur)
                remain_dt += l_dt[:adj]
                l_dt = l_dt[adj:]

        assert len(seq) + len(remain_dt) == length
        if len(seq) == 0:
            break
        else:
            ret.append(seq)

    return ret, remain_dt


def convert_binsize(array, org_binsize, new_binsize):
    assert org_binsize <= new_binsize, \
        "new_binsize needs to be larger than org_binsize"
    if org_binsize == new_binsize:
        return array

    ratio = int(new_binsize.total_seconds() / org_binsize.total_seconds())
    l = []
    i = 0
    while i < array.size:
        l.append(array[i:i+ratio].sum())
        i += ratio
    return np.array(l)


def rand_uniform(top_dt, end_dt, lambd):
    """Generate a random event that follows uniform distribution of
    LAMBD times a day on the average.

    Args:
        top_dt (datetime.datetime): The start time of generated event.
        end_dt (datetime.datetime): The end time of generated event.
        lambd (float): The average of appearance of generated event per a day.

    Returns:
        list[datetime.datetime]
    """
    ret = []
    total_dt = (end_dt - top_dt)
    avtimes = 1.0 * lambd * total_dt.total_seconds() / (24 * 60 * 60)
    times = int(np.random.poisson(avtimes))
    for i in range(times):
        deltasec = int(total_dt.total_seconds() * random.random())
        dt = top_dt + datetime.timedelta(seconds = deltasec)
        ret.append(dt)
    return ret


def rand_exp(top_dt, end_dt, lambd):
    """Generate a random event that follows Poisson process.
    The event interval matches exponential distribution of
    LAMBD times a day on the average.

    Args:
        top_dt (datetime.datetime): The start time of generated event.
        end_dt (datetime.datetime): The end time of generated event.
        lambd (float): The average of appearance of generated event per a day.

    Yields:
        datetime.datetime
    """
    temp_dt = top_dt
    temp_dt = rand_next_exp(temp_dt, lambd)
    while temp_dt < end_dt:
        yield temp_dt
        temp_dt = rand_next_exp(temp_dt, lambd)


def rand_next_exp(dt, lambd):
    return dt + datetime.timedelta(seconds = 1) * int(
            24 * 60 * 60 * random.expovariate(lambd))


def test_discretize():
    test_data = [
            "2112-07-16 00:00:00",
            "2112-07-16 00:00:00",
            "2112-07-16 00:00:01",
            "2112-07-16 00:57:18",
            "2112-07-16 01:57:18",
            "2112-07-16 02:57:17",
            "2112-07-16 02:57:18",
            "2112-07-16 03:57:18",
            "2112-07-16 04:57:18",
            "2112-07-16 15:17:01",
            "2112-07-16 16:17:01",
            "2112-07-16 18:17:01",
            "2112-07-16 19:17:01",
            "2112-07-16 20:17:01",
            "2112-07-16 20:17:01",
            "2112-07-16 20:17:02",
            "2112-07-16 20:17:21",
            "2112-07-16 21:00:00",
            "2112-07-16 21:17:01",
            "2112-07-16 22:17:01",
            "2112-07-16 23:17:01",
            "2112-07-17 00:00:00",
            "2112-07-17 00:00:04"]
    l_dt = [datetime.datetime.strptime(i, '%Y-%m-%d %H:%M:%S')
            for i in test_data]
    binsize = datetime.timedelta(hours = 1)
    top_dt = adj_sep(min(l_dt), binsize)
    end_dt = radj_sep(max(l_dt), binsize)
    l_label = label((top_dt, end_dt), binsize)
    #data = discretize(l_dt, l_label, binarize = False)
    data = auto_discretize_slide(l_dt,
                                 binsize + datetime.timedelta(minutes = 30),
                                 #binsize,
                                 binsize,
                                 dt_range = (top_dt, end_dt),
                                 method = "count")
    print("result")
    for l, cnt in zip(l_label, data):
        print(l, cnt)


def test_separate_periodic():
    test_data = [
            "2112-07-16 00:00:00",
            "2112-07-16 00:00:00",
            "2112-07-16 00:00:01",
            "2112-07-16 00:57:18",
            "2112-07-16 01:57:18",
            "2112-07-16 02:57:17",
            "2112-07-16 02:57:18",
            "2112-07-16 03:57:18",
            "2112-07-16 04:57:18",
            "2112-07-16 15:17:01",
            "2112-07-16 16:17:01",
            "2112-07-16 18:17:01",
            "2112-07-16 19:17:01",
            "2112-07-16 20:17:01",
            "2112-07-16 20:17:01",
            "2112-07-16 20:17:02",
            "2112-07-16 20:17:21",
            "2112-07-16 20:18:00",
            "2112-07-16 21:17:01",
            "2112-07-16 22:17:01",
            "2112-07-16 23:17:01",
            "2112-07-17 00:00:00",
            "2112-07-17 00:00:04"]
    data = [datetime.datetime.strptime(i, '%Y-%m-%d %H:%M:%S')
            for i in test_data]
    dur = datetime.timedelta(hours = 1)
    err = 0.01 # 1hour -> err 36sec
    l_seq, remain_dt = separate_periodic(data, dur, err)
    #l_seq, remain_dt = separate_periodic_dup(data, dur, err)
    for cnt, seq in enumerate(l_seq):
        print("sequence", cnt)
        for dt in seq:
            print(dt)
        print()
    print("remains")
    for dt in remain_dt:
        print(dt)


def test_randlog_exp():
    top_dt = datetime.datetime.strptime("2112-07-16 00:00:00", TIMEFMT)
    end_dt = datetime.datetime.strptime("2112-07-17 00:00:00", TIMEFMT)
    lambd = 10000.0
    for i in range(10):
        print("exp 1")
        for dt in rand_exp(top_dt, end_dt, lambd):
            print(dt)
        print()


if __name__ == "__main__":
    #test_separate_periodic()
    test_discretize()
    #test_randlog_exp()


