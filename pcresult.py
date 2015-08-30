#!/usr/bin/env python
# coding: utf-8

import sys
import os
import datetime
import optparse
import cPickle as pickle

import fslib
import config
import log_db
import pc_log

DETAIL_SHOW_LIMIT = 10
_config = config.common_config()


class PCOutput():

    __module__ = os.path.splitext(os.path.basename(__file__))[0]

    def __init__(self, conf):
        self.conf = conf
        self.ldb = None
    
    def make(self, graph, evmap, top_dt, end_dt, dur, area):

        self.dirname = self.conf.get("dag", "output_dir")
        self.graph = graph
        self.d_edges, self.ud_edges = self._init_edges()
        
        self.evmap = evmap  # log2event.LogEventIDMap
        self.top_dt = top_dt
        self.end_dt = end_dt
        self.dur = dur
        self.area = area
        self.threshold = self.conf.getfloat("dag", "threshold")
        
        self.filename = self._get_fn()

    def load(self, fn = None):
        if not fn: fn = self.filename
        with open(fn, 'r') as f:
            d = pickle.load(f)
        self.__dict__.update(d)
        return self

    def dump(self, fn = None):
        if not fn: fn = self.filename
        with open(fn, 'w') as f:
            pickle.dump(self.__dict__, f)

    def _none_caution(self):
        if self.graph is None:
            raise ValueError("use make() or load() before showing")

    def _init_edges(self):
        temp = []
        udedges = []
        for edge in self.graph.edges():
            for cnt, temp_edge in enumerate(temp):
                if edge[0] == temp_edge[1] and edge[1] == temp_edge[0]:
                    temp.pop(cnt)
                    udedges.append((min(edge[0], edge[1]),
                            max(edge[0], edge[1])))
                    break
            else:
                temp.append(edge)
        else:
            dedges = temp
        return dedges, udedges

    def _init_ldb(self):
        if self.ldb is None:
            self.ldb = log_db.ldb_manager(self.conf)
            self.ldb.open_lt()

    def _get_fn(self):
        return pc_log.thread_name(self.conf, self.top_dt, self.end_dt,
                self.dur, self.area)

    def _print_lt(self, eid, header = "lt"):
        ltgid, host = self.evmap.info(eid)
        print("{0}>")
        print("\n ".join(
            [str(ltline) for ltline in self.ldb.lt.ltgroup[ltgid]]))

    def _print_edge(self, edge):
        src_ltgid, src_host = self.evmap.info(edge[0])
        dst_ltgid, dst_host = self.evmap.info(edge[1])
        print("{0}({1}) -> {2}({3})".format(src_ltgid, src_host, \
                dst_ltgid, dst_host)) 

    def _print_edge_lt(self, edge):
        for eid, header in zip(edge, ("src", "dst")):
            ltgid, host = self.evmap.info(eid)
            print("{0}>".format(header))
            print("\n ".join(
                [str(ltline) for ltline in self.ldb.lt.ltgroup[ltgid]]))
            print
    
    def _print_edge_detail(self, edge, limit = None):
        if self.area == "all":
            area = None
        else:
            area = self.area
        
        for eid, header in zip(edge, ("src", "dst")):
            ltgid, host = self.evmap.info(eid)
            print("{0}>".format(header))
            for ltline in self.ldb.lt.ltgroup[ltgid]:
                buf = []
                cnt = 0
                for line in self.ldb.generate(ltline.ltid,
                        self.top_dt, self.end_dt, host, area):
                    buf.append(line.restore_line())
                    cnt += 1
                    if limit is not None and cnt >= limit:
                        buf.append("...")
                        break
                if cnt > 0:
                    print " " + str(ltline)
                    print "\n".join(buf)
        print

    def print_env(self):
        self._none_caution()
        print("term : {0} - {1}".format(self.top_dt, self.end_dt))
        print("duration : {0}".format(self.dur))
        if self.area is None:
            print("area : whole")
        else:
            print("area : {0}".format(self.area))
        print("threshold : {0}".format(self.threshold))
        print("events : {0}".format(len(self.evmap)))
        print

    def print_result(self):
        self._none_caution()
        for edge in self.d_edges:
            self._print_edge(edge)
        for edge in self.ud_edges:
            self._print_edge(edge)
        print

    def print_result_lt(self):
        self._none_caution()
        self._init_ldb()
        print("### directed ###")
        for edge in self.d_edges:
            self._print_edge(edge)
            self._print_edge_lt(edge)
        print
        print("### undirected ###")
        for edge in self.ud_edges:
            self._print_edge(edge)
            self._print_edge_lt(edge)

    def print_result_detail(self, limit = None):
        self._none_caution()
        self._init_ldb()
        print("### directed ###")
        for edge in self.d_edges:
            self._print_edge(edge)
            self._print_edge_detail(edge, limit)
        print
        print("### undirected ###")
        for edge in self.ud_edges:
            self._print_edge(edge)
            self._print_edge_detail(edge, limit)

    def show_graph(self, fn):
        import networkx as nx
        g = nx.to_agraph(self.graph)
        g.draw(fn, prog='circo')
        for node in self.graph.nodes():
            ltid, host = self.evmap.info(node)
            print "Node {0} : LtID {1} , Host {2}".format(node, ltid, host)
        print ">", fn


def list_results(conf):
    src_dir = conf.get("dag", "output_dir")
 
    print "datetime\t\tarea\tnodes\tedges\tfilepath"
    for fp in fslib.rep_dir(src_dir):
        output = PCOutput(conf).load(fp)
        print "\t".join((str(output.top_dt), output.area,
                str(len(output.graph.nodes())),
                str(len(output.graph.edges())), fp))


if __name__ == "__main__":

    src_dir = _config.get("dag", "default_output_dir")

    usage = "usage: %s [options] <filename>\n" % sys.argv[0] + \
            "if no filename given, show abstraction of results"
    op = optparse.OptionParser(usage)
    op.add_option("-c", "--config", action="store",
            dest="conf", type="string", default=config.DEFAULT_CONFIG_NAME,
            help="configuration file path")
    op.add_option("-g", action="store", dest="graph_fn", type="string",
            default=None, help="output graph view")
    op.add_option("-l", action="store_true", dest="detail",
            default=False, help="output examples of log events")
    (options, args) = op.parse_args()

    conf = config.open_config(options.conf)
    if len(args) == 0:
        list_results(conf)
    else:
        output = PCOutput(conf).load(args[0])
        output.print_env() 
        output.print_result() 
        if options.detail:
            output.print_result_detail(DETAIL_SHOW_LIMIT)
        else:
            output.print_result_lt()
        if options.graph_fn:
            output.show_graph(options.graph_fn)

