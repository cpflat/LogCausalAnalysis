#!/usr/bin/env python
# coding: utf-8

import datetime


class TimeLabel():

    def __init__(self, top_dt, end_dt, dur):
        self.top_dt = top_dt
        self.end_dt = end_dt
        self.dur = dur

        self.label = []
        temp_dt = top_dt
        while temp_dt + dur < end_dt:
            self.label.append(temp_dt)
            temp_dt += dur
        else:
            if not temp_dt == end_dt:
                self.label.append(temp_dt)

    def __len__(self):
        return len(self.label)

    def __iter__(self):
        return self.label.__iter__()


class TimeSeries():

    def __init__(self, top_dt, end_dt, dur, default = None):
        self.label = TimeLabel(top_dt, end_dt, dur)
        self.length = len(self.label)
        self.data = {dt: default for dt in self.label}

    def __iter__(self):
        return self._generator()

    def _generator(self):
        for dt in self.label:
            yield self.data[dt]

    def setdata(self, data, sortflag = True):
        if type(data) == dict:
            # data = {datetime : val, ...}
            l_data = [i for i in data.iteritems()]
        elif type(data) == list:
            # data = [(datetime, val), ...]
            l_data = data[:]
        else:
            raise TypeError
        if len(l_data) == 0:
            return
        if sortflag:
            l_data.sort(key=lambda x: x[0])

        for prev_dt, dt in zip(self.label.label[:-1], self.label.label[1:]):
            while l_data[0][0] < dt:
                self.data[prev_dt] = l_data.pop(0)[1]
                if len(l_data) == 0:
                    return
        else:
            for d, v in l_data:
                if d < self.label.end_dt: 
                    self.data[self.label.label[-1]] = v
                else:
                    raise RuntimeWarning("some data is out of timelabel")
        return

    def countdata(self, l_event, sortflag = True):
        # l_event = [datetime, ...]
        if len(l_event) == 0:
            return
        if sortflag:
            l_dt = l_event[:]
            l_dt.sort()

        for prev_dt, dt in zip(self.label.label[:-1], self.label.label[1:]):
            #import pdb; pdb.set_trace()
            while l_dt[0] < dt:
                self.data[prev_dt] += 1
                l_dt.pop(0)
                if len(l_dt) == 0:
                    return
        else:
            for dt in l_dt:
                if dt < self.label.end_dt:
                    self.data[self.label.label[-1]] += 1
                else:
                    raise RuntimeWarning("some data is out of timelabel")
        return


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


def test_ts():
    top_dt = datetime.datetime(2012, 4, 1, 0, 0, 0)
    end_dt = datetime.datetime(2012, 4, 2, 0, 0, 0)
    dur = datetime.timedelta(hours = 1)

    data = [
        (datetime.datetime(2012, 4, 1, 0, 0, 0), 1),
        (datetime.datetime(2012, 4, 1, 2, 11, 0), 1),
        (datetime.datetime(2012, 4, 1, 2, 12, 0), 1),
        (datetime.datetime(2012, 4, 1, 2, 15, 0), 1),
        (datetime.datetime(2012, 4, 1, 22, 0, 0), 1),
        (datetime.datetime(2012, 4, 1, 23, 56, 0), 1),
        (datetime.datetime(2012, 4, 1, 23, 59, 0), 1),
            ]

    ts = TimeSeries(top_dt, end_dt, dur, 0)
    ts.setdata(data, False)
    for t, v in zip(ts.label, ts):
        print t, v
    print
    ts2 = TimeSeries(top_dt, end_dt, dur, 0)
    ts2.countdata([dt for dt, v in data], False)
    for t, v in zip(ts2.label, ts2):
        print t, v

if __name__ == "__main__":
    test_ts()
