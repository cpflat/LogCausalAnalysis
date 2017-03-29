#!/usr/bin/env python
# coding: utf-8

import datetime
import logging
import math
import numpy as np

import config
import dtutil

_logger = logging.getLogger(__name__.rpartition(".")[-1])


def remove_dist(l_dt, top_dt, end_dt, binsize, threshold):
    length = (end_dt - top_dt).total_seconds()
    bin_length = binsize.total_seconds()
    bins = math.ceil(1.0 * length / bin_length)
    a_stat = np.array([0] * int(bins))
    for dt in l_dt:
        cnt = int((dt - top_dt).total_seconds() / bin_length)
        assert cnt < len(a_stat)
        a_stat[cnt:] += 1

    a_linear = (np.array(range(int(bins))) + 1) * (1.0 * len(l_dt) / bins)
    val = sum((a_stat - a_linear) ** 2) / bins

    _logger.info("Linear filter evaluation value {0}".format(val))
    return val < threshold


def remove_corr(conf, l_stat, binsize):
    corr_th = conf.getfloat("filter", "self_corr_th")
    corr_diff = [config.str2dur(diffstr) for diffstr
            in conf.gettuple("filter", "self_corr_diff")]

    l_result = []
    for diff in corr_diff:
        c = self_corr(l_stat, diff, binsize)
        l_result.append([c, diff])
    max_c, max_diff = max(l_result, key = lambda x: x[0])

    if max_c >= corr_th:
        return True, max_diff
    else:
        return False, None


def periodic_events(conf, ld, top_dt, end_dt, area, edict, evmap):
    l_sampling_term = [config.str2dur(diffstr) for diffstr
            in conf.gettuple("filter", "sampling_term")]
    if len(l_sampling_term) == 0:
        raise ValueError("configuration error in filter.sampling_term")
    search_interval = conf.getboolean("filter", "search_interval")
    corr_th = conf.getfloat("filter", "self_corr_th")
    corr_bin = conf.getdur("filter", "self_corr_bin")
    corr_diff = [config.str2dur(diffstr) for diffstr
            in conf.gettuple("filter", "self_corr_diff")]

    l_sample_top_dt = [end_dt - tdelta for tdelta in sorted(l_sampling_term)]
    sample_top_dt_max = l_sample_top_dt[-1]

    # prepare time-series of sampling term
    sample_edict = {}
    iterobj = ld.iter_lines(top_dt = sample_top_dt_max, end_dt = end_dt,
            area = area)
    for line in iterobj:
        eid = evmap.process_line(line)
        sample_edict.setdefault(eid, []).append(line.dt)

    # determine interval candidate 
    if search_interval:
        new_corr_diff = set()
        p_cnt = conf.getint("filter", "periodic_count")
        p_term = conf.getdur("filter", "periodic_term")
        p_th = conf.getfloat("filter", "periodic_th")
        for sample_top_dt in l_sample_top_dt: 
            for eid, l_dt in sample_edict.iteritems():
                l_dts = dtutil.limit_dt_seq(l_dt, sample_top_dt, end_dt)
                if is_enough_long(l_dts, p_cnt, p_term):
                    diff = interval(l_dts, p_th)
                    if diff is not None:
                        new_corr_diff.add(datetime.timedelta(seconds = diff))
        new_corr_diff.update(set(corr_diff))
        corr_diff = list(new_corr_diff)

    _logger.info("correlation interval : {0}".format(
            " ".join([str(i) for i in corr_diff])))

    # get periodic events
    ret = []
    for eid, l_dt in sample_edict.iteritems():
        l_result = []
        for sample_top_dt in l_sample_top_dt:
            for diff in corr_diff:
                l_dts = dtutil.limit_dt_seq(l_dt, sample_top_dt, end_dt)
                if len(l_dts) == 0:
                    continue
                data = dtutil.auto_discretize(l_dts, corr_bin)
                corr = self_corr(data, diff, corr_bin)
                l_result.append((corr, diff))
        max_corr, max_diff = max(l_result)
        if max_corr >= corr_th:
            _logger.debug("Event {0} is periodic (interval: {1})".format(
                    eid, max_diff))
            ret.append((eid, max_diff))
    return ret


def is_enough_long(l_dt, p_cnt, p_term):
    if len(l_dt) < p_cnt:
        _logger.debug(
                "Event appearance is too small, skip periodicity test")
        return False
    elif max(l_dt) - min(l_dt) < p_term:
        _logger.debug(
                "Event appearing term is too short, skip periodicity test")
        return False
    else:
        return True


def interval(l_dt, threshold):
    """int: get interval time (seconds), or None if it seems not periodic.
    """
    if len(l_dt) <= 2:
        # len(l_dt) < 2 : no interval will be found
        # len(l_dt) == 2 : only 1 interval that not seem periodic...
        return None
    
    l_interval = []
    prev_dt = None
    for dt in sorted(l_dt):
        if prev_dt is not None:
            diff = (dt - prev_dt).total_seconds()
            l_interval.append(diff)
        prev_dt = dt

    dist = np.array(l_interval)
    std = np.std(dist)
    mean = np.mean(dist)
    _logger.debug("std {0}, mean {1}, median {2}".format(std, mean,
            np.median(dist)))

    if mean == 0:
        # mean == 0 : multiple message in 1 time, not seem periodic
        return None
    if (std / mean) < threshold:
        return int(np.median(dist))
    else:
        return None


def self_corr(data, diff, binsize):
    """
    Args:
        data (List[datetime.datetime])
        diff (datetime.timedelta)
        binsize (datetime.timedelta)

    Returns:
        float: Self-correlation coefficient with given lag.
    """
    binnum = int(diff.total_seconds() / binsize.total_seconds())
    if len(data) <= binnum * 2:
        return 0.0
    else:
        data1 = data[:len(data) - binnum]
        data2 = data[binnum:]
        assert len(data1) == len(data2)
        return np.corrcoef(np.array(data1), np.array(data2))[0, 1]


def test_evfilter(conf, fn = "evf_filtered"):

    def dump_event(buf, eid, evmap, max_diff):
        import log2event
        buf.append(("Event {0} : {1} (diff: {2})".format(eid,
                evmap.info_str(eid), max_diff)))
        #if evmap.info(eid).type == log2event.EventDefinitionMap.type_normal:
        if evmap.info(eid).type == evmap.type_normal:
            buf.append(evmap.info_repr(ld, eid))
        else:
            buf.append("\n".join([str(dt) for dt in edict[eid]]))
            buf.append("\n".join(["#" + w for w
                    in evmap.info_repr(ld, eid).split("\n")]))
        buf.append("")
        return buf


    import pc_log
    import log_db
    buf = []
    ld = log_db.LogData(conf)
    for args in pc_log.pc_all_args(conf):
        top_dt = args[1]
        end_dt = args[2]
        dur = args[3]
        area = args[4]
        buf.append("testing evfilter({0} - {1} in {2})\n".format(
                top_dt, end_dt, area))
        edict, evmap = log2event.get_edict(conf, top_dt, end_dt, dur, area)

        l_result = periodic_events(conf, ld, top_dt, end_dt, area,
                edict, evmap)
        for eid, max_diff in l_result:
            buf = dump_event(buf, eid, evmap, max_diff)
    with open(fn, "w") as f:
        f.write("\n".join(buf))


def test_interval(conf):
    import pc_log
    import log_db
    ld = log_db.LogData(conf)
    for args in pc_log.pc_all_args(conf):
        top_dt = args[1]
        end_dt = args[2]
        dur = args[3]
        area = args[4]
        print("testing evfilter interval({0} - {1} in {2})\n".format(
                top_dt, end_dt, area))
        edict, evmap = log2event.get_edict(conf, top_dt, end_dt, dur, area)

        new_corr_diff = set()
        p_cnt = conf.getint("filter", "periodic_count")
        p_term = conf.getdur("filter", "periodic_term")
        p_th = conf.getfloat("filter", "periodic_th")
        for sample_top_dt in l_sample_top_dt: 
            for eid, l_dt in sample_edict.iteritems():
                l_dts = dtutil.limit_dt_seq(l_dt, sample_top_dt, end_dt)
                if is_enough_long(l_dts, p_cnt, p_term):
                    diff = interval(l_dts, p_th)
                    if diff is not None:
                        new_corr_diff.add(datetime.timedelta(seconds = diff))
        print("found interval : {0}".format(", ".join(
                [str(diff) for diff in new_corr_diff])))


if __name__ == "__main__":
    import sys
    import optparse
    usage = "usage: {0} [options]".format(sys.argv[0])
    op = optparse.OptionParser(usage)
    op.add_option("-c", "--config", action="store",
            dest="conf", type="string", default=config.DEFAULT_CONFIG_NAME,
            help="configuration file path")
    op.add_option("--debug", action="store_true", dest="debug",
            default=False, help="set logging level to DEBUG")
    (options, args) = op.parse_args()

    conf = config.open_config(options.conf)
    lv = logging.DEBUG if options.debug else logging.INFO
    config.set_common_logging(conf, _logger, [], lv = lv)
    #test_evfilter(conf)
    test_interval(conf)
