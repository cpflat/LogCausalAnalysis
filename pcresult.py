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

    def __init__(self, dirname):
        self.dirname = dirname
        self.ldb = None
    
    def make(self, graph, evmap, top_dt, end_dt, dur, area, threshold):
        self.graph = graph
        self.d_edges, self.ud_edges = self._init_edges()
        
        self.evmap = evmap  # log2event.LogEventIDMap
        self.top_dt = top_dt
        self.end_dt = end_dt
        self.dur = dur
        self.area = area
        self.threshold = threshold
        
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
            self.ldb = log_db.ldb_manager()
            self.ldb.open_lt()

    def _get_fn(self):
        return pc_log.thread_name(self.dirname, self.top_dt, self.end_dt,
                self.dur, self.area, self.threshold)

    def _print_edge(self, edge):
        src_ltid, src_host = self.evmap.info(edge[0])
        dst_ltid, dst_host = self.evmap.info(edge[1])
        print("{0}({1}) -> {2}({3})".format(src_ltid, src_host, \
                dst_ltid, dst_host)) 

    def _print_edge_lt(self, edge):
        src_ltid, src_host = self.evmap.info(edge[0])
        dst_ltid, dst_host = self.evmap.info(edge[1])
        print("src> " + str(self.ldb.lt.table[src_ltid]))
        print("dst> " + str(self.ldb.lt.table[dst_ltid]))
        print
    
    def _print_edge_detail(self, edge, limit = None):
        src_ltid, src_host = self.evmap.info(edge[0])
        dst_ltid, dst_host = self.evmap.info(edge[1])
 
        print("src> " + str(self.ldb.lt.table[src_ltid]))
        cnt = 0
        if self.area == "all":
            area = None
        else:
            area = self.area
        for line in self.ldb.generate(src_ltid, self.top_dt, self.end_dt,
                src_host, area):
            print line.restore_message()
            cnt += 1
            if limit is not None and cnt >= limit:
                print("...")
                break
        
        print("dst> " + str(self.ldb.lt.table[dst_ltid]))
        cnt = 0
        for line in self.ldb.generate(dst_ltid, self.top_dt, self.end_dt,
                dst_host, area):
            print line.restore_message()
            cnt += 1
            if limit is not None and cnt >= limit:
                print("...")
                break
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

    def print_result_detail(self):
        self._none_caution()
        self._init_ldb()
        print("### directed ###")
        for edge in self.d_edges:
            self._print_edge(edge)
            self._print_edge_detail(edge, DETAIL_SHOW_LIMIT)
        print
        print("### undirected ###")
        for edge in self.ud_edges:
            self._print_edge(edge)
            self._print_edge_detail(edge, DETAIL_SHOW_LIMIT)

    def show_graph(self, fn):
        import networkx as nx
        g = nx.to_agraph(self.graph)
        g.draw(fn, prog='circo')
        for node in self.graph.nodes():
            ltid, host = self.evmap.info(node)
            print "Node {0} : LtID {1} , Host {2}".format(node, ltid, host)
        print ">", fn


def list_results(src_dir):
    print "datetime\t\tarea\tnodes\tedges\tfilepath"
    for fp in fslib.rep_dir(src_dir):
        output = PCOutput(src_dir).load(fp)
        print "\t".join((str(output.top_dt), output.area,
                str(len(output.graph.nodes())),
                str(len(output.graph.edges())), fp))


if __name__ == "__main__":

    src_dir = _config.get("dag", "default_output_dir")

    usage = "usage: %s [options] <fn>" % sys.argv[0]
    op = optparse.OptionParser(usage)
    op.add_option("-d", action="store", dest="src_dir", type="string",
            default=src_dir, help="output directory")
    op.add_option("-g", action="store", dest="graph_fn", type="string",
            default=None, help="graph output")
    op.add_option("-l", action="store_true", dest="detail",
            default=False, help="detail output")
    (options, args) = op.parse_args()
    
    if len(args) == 0:
        list_results(options.src_dir)
    else:
        output = PCOutput(options.src_dir).load(args[0])
        output.print_env() 
        output.print_result() 
        if options.detail:
            output.print_result_detail()
        else:
            output.print_result_lt()
        if options.graph_fn:
            output.show_graph(options.graph_fn)

