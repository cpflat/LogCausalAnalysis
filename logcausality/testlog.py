#!/usr/bin/env python
# coding: utf-8

import sys
import os
import datetime
import random
import re

import config

#SEED = None
SEED = 3
CONF = "/".join((os.path.dirname(__file__), "testlog.conf.sample"))

class TestLogGenerator():

    _var_re = re.compile(r"\$.+?\$")

    def __init__(self, conf_fn):
        if SEED is None:
            random.seed()
        else:
            random.seed(SEED)

        self.conf = config.ExtendedConfigParser(noopterror = False)
        self.conf.read(conf_fn)
        self.term = self.conf.getterm("main", "term")
        self.top_dt, self.end_dt = self.term
        self.d_host = {}
        for group in self.conf.gettuple("main", "host_groups"):
            for host in self.conf.gettuple("main", "group_" + group):
                self.d_host.setdefault(group, []).append(host)

        self.l_event = []
        for event_name in self.conf.gettuple("main", "events"):
            self._generate_event(event_name)

        self.l_log = []
        for event in self.l_event:
            self._generate_log(event)

    def _dt_rand(self, top_dt, end_dt):
        return self._dt_delta_rand(top_dt,
                datetime.timedelta(0), end_dt - top_dt)

    @staticmethod
    def _dt_delta_rand(top_dt, durmin, durmax):
        deltasec = 24 * 60 * 60 * (durmax.days - durmin.days) + \
                durmax.seconds - durmin.seconds
        seconds = datetime.timedelta(seconds = random.randint(0, deltasec - 1))
        return top_dt + durmin + seconds

    def _generate_event(self, event_name):
        
        section = "event_" + event_name
        
        def _recur(dt, host, event_name):
            if self.conf.getboolean(section, "recurrence"):
                if random.random() < self.conf.getfloat(section, "recur_p"):
                    durmin = self.conf.getdur(section, "recur_dur_min")
                    durmax = self.conf.getdur(section, "recur_dur_max")
                    new_dt = self._dt_delta_rand(dt, durmin, durmax)
                    _add_event(new_dt, host, event_name)

        def _add_event(dt, host, event_name):
            info = {}
            for i in self.conf.gettuple(section, "info"):
                if i == "ifname":
                    info[i] = random.choice(
                            self.conf.gettuple(section, "ifname"))
                elif i == "user":
                    info[i] = random.choice(
                            self.conf.gettuple(section, "user"))
            self.l_event.append((dt, host, event_name, info))
            _recur(dt, host, event_name)


        for group in self.conf.gettuple(section, "groups"):
            for host in self.d_host[group]:
                occ = self.conf.get(section, "occurrence")
                if occ == "random":
                    freq = self.conf.getfloat(section, "frequency")
                    times = int(freq * (self.end_dt - self.top_dt).days + 0.5)
                    for i in range(times):
                        dt = self._dt_rand(self.top_dt, self.end_dt)
                        _add_event(dt, host, event_name)
                elif occ == "hourly":
                    dursec = 60 * 60
                    first_dt = self.top_dt + datetime.timedelta(\
                            seconds = random.randint(0, dursec - 1))
                    now_dt = first_dt
                    while now_dt < self.end_dt:
                        _add_event(now_dt, host, event_name)
                        now_dt += datetime.timedelta(seconds = dursec)
                elif occ == "daily":
                    dursec = 24 * 60 * 60
                    first_dt = self.top_dt + datetime.timedelta(\
                            seconds = random.randint(0, dursec - 1))
                    now_dt = first_dt
                    while now_dt < self.end_dt:
                        _add_event(now_dt, host, event_name)
                        now_dt += datetime.timedelta(seconds = dursec)

    def _generate_log(self, event):
        dt = event[0]
        host = event[1]
        event_name = event[2]
        info = event[3]
        for log_name in self.conf.gettuple("event_" + event_name, "logs"):
            section = "log_" + log_name
            mode = self.conf.get(section, "mode")
            form = self.conf.get(section, "format")

            mes = form
            while True:
                match = self._var_re.search(mes)
                if match is None:
                    break
                var_type = match.group().strip("$")
                if var_type in info.keys():
                    var_string = info[var_type]
                elif var_type == "pid":
                    var_string = str(random.randint(1, 65535))
                elif var_type == "host":
                    var_string = host
                mes = "".join((mes[:match.start()] + var_string +
                        mes[match.end():]))

            if mode == "each":
                self.l_log.append((dt, host, mes))
            elif mode == "delay_rand":
                delay_min = self.conf.getdur(section, "delay_min")
                delay_max = self.conf.getdur(section, "delay_max")
                log_dt = self._dt_delta_rand(dt, delay_min, delay_max)
                self.l_log.append((log_dt, host, mes))
            elif mode == "drop_rand":
                drop_p = self.conf.getfloat(section, "drop_p")
                if random.random() > drop_p:
                    self.l_log.append((dt, host, mes))
            elif mode == "other_host_rand":
                l_host = []
                for t_group in self.conf.gettuple(section, "groups"):
                    for t_host in self.d_host[t_group]:
                        if not t_host == host:
                            l_host.append(t_host)
                self.l_log.append((dt, random.choice(l_host), mes))

    def dump_log(self, output):
        l_line = sorted(self.l_log, key = lambda x: x[0])
        if output is None:
            for line in l_line:
                print " ".join((line[0].strftime("%Y-%m-%d %H:%M:%S"), 
                        line[1], line[2]))
        else:
            with open(output, 'w') as f:
                for line in l_line:
                    f.write(" ".join((line[0].strftime("%Y-%m-%d %H:%M:%S"), 
                        line[1], line[2])) + "\n")


def test_make(fn = CONF, output = None):
    tlg = TestLogGenerator(fn)
    tlg.dump_log(output)


if __name__ == "__main__":
    test_make()

