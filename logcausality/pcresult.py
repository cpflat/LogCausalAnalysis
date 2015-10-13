#!/usr/bin/env python
# coding: utf-8

import sys
import os
import datetime
import itertools
import logging
import cPickle as pickle
import networkx as nx

import fslib
import config
import lt_label

_logger = logging.getLogger(__name__)


class PCOutput():

    def __init__(self, conf):
        self.conf = conf
        self.ld = None
    
    def make(self, graph, evmap, top_dt, end_dt, dur, area):

        self.dirname = self.conf.get("dag", "output_dir")
        self.graph = graph
        self.evmap = evmap  # log2event.LogEventIDMap
        self.top_dt = top_dt
        self.end_dt = end_dt
        self.dur = dur
        self.area = area
        self.threshold = self.conf.getfloat("dag", "threshold")
        
        self.filename = self._get_fn()

    def load(self, fn = None):
        if not fn: fn = self.filename
        # do not use old configuration
        c = self.conf
        with open(fn, 'r') as f:
            d = pickle.load(f)
        self.__dict__.update(d)
        self.conf = c
        return self

    def dump(self, fn = None):
        if not fn: fn = self.filename
        with open(fn, 'w') as f:
            pickle.dump(self.__dict__, f)

    def _none_caution(self):
        if self.graph is None:
            raise ValueError("use make() or load() before showing")

    def _init_ld(self):
        if self.ld is None:
            import log_db
            self.ld = log_db.LogData(self.conf)

    def _get_fn(self):
        import pc_log
        return pc_log.thread_name(self.conf, self.top_dt, self.end_dt,
                self.dur, self.area)

    def result_fn(self):
        return self.filename.rpartition("/")[-1]

    def _print_lt(self, eid, header = "lt"):
        ltgid, host = self.evmap.info(eid)
        print("{0}>")
        print("\n ".join(
            [str(ltline) for ltline in self.ld.ltg_members(ltgid)]))

    def _print_edge(self, edge):
        src_ltgid, src_host = self.evmap.info(edge[0])
        dst_ltgid, dst_host = self.evmap.info(edge[1])
        print("{0}({1}) -> {2}({3})".format(src_ltgid, src_host, \
                dst_ltgid, dst_host)) 

    def _print_edge_lt(self, edge):
        for eid, header in zip(edge, ("src", "dst")):
            ltgid, host = self.evmap.info(eid)
            print("{0}> ltgid {1} (host {2})".format(header, ltgid, host))
            print("\n".join(
                [str(ltline) for ltline in self.ld.ltg_members(ltgid)]))
            print
    
    def _print_edge_detail(self, edge, limit = None):
        if self.area == "all":
            area = None
        else:
            area = self.area
        
        for eid, header in zip(edge, ("src", "dst")):
            ltgid, host = self.evmap.info(eid)
            print("{0}> ltgid {1} (host {2})".format(header, ltgid, host))
            self.ld.show_log_repr(limit, None, ltgid,
                    self.top_dt, self.end_dt, host, area)
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

    def print_result(self, graph = None):
        if graph is None:
            graph = self.graph
        self._none_caution()
        for edges in self._separate_edges(graph): # directed, undirected
            for edge in edges:
                self._print_edge(edge)
        print

    def print_result_lt(self, graph = None):
        if graph is None:
            graph = self.graph
        self._none_caution()
        self._init_ld()
        for edges, label in zip(self._separate_edges(graph),
                ("directed", "undirected")):
            print("### {0} ###").format(label)
            for edge in edges:
                self._print_edge(edge)
                self._print_edge_lt(edge)
            print

    def print_result_detail(self, graph = None, limit = None):
        if graph is None:
            graph = self.graph
        self._none_caution()
        self._init_ld()
        for edges, label in zip(self._separate_edges(graph),
                ("directed", "undirected")):
            print("### {0} ###").format(label)
            for edge in edges:
                self._print_edge(edge)
                self._print_edge_detail(edge, limit)
            print

    def _node_info(self, node):
        # return labeled information of given node
        return self.evmap.info(node)

    def _edge_info(self, edge):
        # return labeled information of given edge
        return tuple(self._node_info(node) for node in edge)

    def _has_node(self, info):
        return self.evmap.ermap.has_key(info)

    def _node_id(self, info):
        # return node id of given labeled information
        # info : (ltgid, host)
        return self.evmap.get_eid(info)

    def _edge_id(self, t_info):
        # return numbered edge of given labeled information
        # t_info : (src_info, dst_info)
        return tuple(self._node_id(info) for info in t_info)

    def _separate_edges(self, graph = None):
        # separate directed edges and undirected edges
        if graph is None:
            graph = self.graph
        temp = []
        udedges = []
        for edge in graph.edges():
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

    def _edge_across_host(self, l_edge = None, rest = False):
        if l_edge is None:
            l_edge = self.graph.edges()
        l_same = []
        l_diff = []
        for edge in l_edge:
            src_ltgid, src_host = self._node_info(edge[0])
            dst_ltgid, dst_host = self._node_info(edge[1])
            if src_host == dst_host:
                l_same.append(edge)
            else:
                l_diff.append(edge)
        if rest:
            return l_same, l_diff
        else:
            return l_diff

    def _edge_across_label(self, l_edge = None, rest = False):
        if l_edge is None:
            l_edge = self.graph.edges()
        
        ltconf_path = self.conf.get("visual", "ltlabel")
        if ltconf_path == "":
            ltconf_path = lt_label.DEFAULT_LABEL_CONF
        ll = lt_label.LTLabel(ltconf_path)
        self._init_ld()

        l_same = []
        l_diff = []
        for edge in l_edge:
            src_node, dst_node = edge
            src_ltgid, src_host = self._node_info(src_node)
            src_label = ll.get_ltg_label(src_ltgid,
                    self.ld.ltg_members(src_ltgid))
            dst_ltgid, dst_host = self._node_info(dst_node)
            dst_label = ll.get_ltg_label(dst_ltgid,
                    self.ld.ltg_members(dst_ltgid))
            if src_label == dst_label:
                l_same.append(edge)
            else:
                l_diff.append(edge)
        if rest:
            return l_same, l_diff
        else:
            return l_diff

    def relabel_graph(self, graph = None):
        if graph is None:
            graph = self.graph
        ltconf_path = self.conf.get("visual", "ltlabel")
        if ltconf_path == "":
            ltconf_path = lt_label.DEFAULT_LABEL_CONF
        ll = lt_label.LTLabel(ltconf_path)
        self._init_ld()

        mapping = {}
        for node in graph.nodes():
            ltgid, host = self._node_info(node)
            label = ll.get_ltg_label(ltgid, self.ld.ltg_members(ltgid))
            if label is None:
                mapping[node] = "{0}, {1}".format(ltgid, host)
            else:
                mapping[node] = "{0}({1}), {2}".format(ltgid, label, host)
        return nx.relabel_nodes(self.graph, mapping, copy=True)

    def show_graph(self, fn, eflag):
        if eflag:
            graph = graph_no_orphan(self.graph)
        else:
            graph = self.graph
        rgraph = self.relabel_graph(graph)
        g = nx.to_agraph(rgraph)
        g.draw(fn, prog='circo')
        print ">", fn


# functions for graph

def graph_no_orphan(src):
    g = nx.DiGraph()
    g.add_edges_from([edge for edge in src.edges()])
    return g


def mcs_size_ratio(r1, r2, ig_direction):
    mcs = maximum_common_subgraph(r1, r2, ig_direction)
    _logger.info("mcs size : {0} ({1}, {2})".format(len(mcs.edges()),
            r1.filename, r2.filename))
    ret = 2.0 * len(mcs.edges()) / \
            (len(r1.graph.edges()) + len(r2.graph.edges()))
    #ret = 1.0 / len(mcs.edges())
    return ret


def maximum_common_subgraph(r1, r2, ig_direction = False):
    
    g1_edges = [r1._edge_info(edge) for edge in r1.graph.edges()]
    g2_edges = [r2._edge_info(edge) for edge in r2.graph.edges()]

    if ig_direction:
        g = nx.Graph()
        for edge in g1_edges:
            if edge in g2_edges or (edge[1], edge[0]) in g2_edges:
                g.add_edge(r1._node_id(edge[0]), r2._node_id(edge[1]))
        return nx.DiGraph(g)
    else:
        g = nx.DiGraph()
        for edge in g1_edges:
            if edge in g2_edges:
                g.add_edge(edge[0], edge[1])
        return g


def graph_edit_distance(r1, r2, ig_direction = False):
    # ignore orphan nodes, only see edges

    g1_edges = [r1._edge_info(edge) for edge in r1.graph.edges()]
    g2_edges = [r2._edge_info(edge) for edge in r2.graph.edges()]

    l_edit = []
    # counting "add edges only in g2"
    l_edit = l_edit + [edge for edge in g2_edges if not edge in g1_edges]
    # counting "remove edges only in g1"
    l_edit = l_edit + [edge for edge in g1_edges if not edge in g2_edges]

    # get combination in l_edit
    s = set()
    for edit in l_edit:
        for e in s:
            if edit[0] == e[1] and edit[1] == e[0]:
                break
        else:
            s.add(edit)

    def get_direction(r, edge):
        # edge : edge_info
        if (not r._has_node(edge[0])) or (not r._has_node(edge[1])):
            return "none"
        src_node = r._node_id(edge[0])
        dst_node = r._node_id(edge[1])
        if (src_node, dst_node) in r.graph.edges():
            if (dst_node, src_node) in r.graph.edges():
                return "both"
            else:
                return "src_to_dst"
        elif (dst_node, src_node) in r.graph.edges():
            return "dst_to_src"
        else:
            return "none"

    if ig_direction:
        # count only if either graph is "none"
        ret = 0.0
        for edge in s:
            d1 = get_direction(r1, edge)
            d2 = get_direction(r2, edge)
            if "none" in (d1, d2):
                ret += 1.0
    else:
        ret = len(s)

    #_logger.info("calculating edit distance between {0} and {1}".format(
    #        r1.filename, r2.filename))
    #for edge in s:
    #    _logger.info("{0}, {1} : {2} -> {3}".format(edge[0], edge[1], 
    #            get_direction(r1, edge), get_direction(r2, edge)))
    _logger.info("edit distance : {0}".format(ret))

    return ret


def graph_network(graph):
    
    l_net = []
    d_rnet = {}
    for node in graph.nodes():
        l_net.append([node])
        d_rnet[node] = len(l_net) - 1

    for edge in graph.edges():
        src_node, dst_node = edge
        if d_rnet[src_node] == d_rnet[dst_node]:
            pass
        else:
            # remove network with dst, save as temp,
            # and add to network with src
            src_netid = d_rnet[src_node]
            dst_netid = d_rnet[dst_node]
            l_net[src_netid] += l_net[dst_netid][:]
            l_net[dst_netid] = []

    ret = [net for net in l_net if len(net) >= 1]
    ret.sort(key = lambda x: len(x), reverse = True)
    return ret


# functions for visualization

def list_results(conf):
    src_dir = conf.get("dag", "output_dir")
 
    print "datetime\t\tarea\tnodes\tedges\tfilepath"
    for fp in fslib.rep_dir(src_dir):
        r = PCOutput(conf).load(fp)
        print "\t".join((str(r.top_dt), r.area,
                str(len(r.graph.nodes())),
                str(len(r.graph.edges())), r.result_fn))


def list_detailed_results(conf):
    src_dir = conf.get("dag", "output_dir")

    splitter = ","
    print splitter.join(["dt", "area", "node", "edge",
            "edge_oh", "d_edge", "d_edge_oh", "fn"])
    for fp in fslib.rep_dir(src_dir):
        r = PCOutput(conf).load(fp)
        row = []
        row.append(str(r.top_dt))
        row.append(str(r.area))
        row.append(str(len(r.graph.nodes())))
        row.append(str(len(r.graph.edges())))
        row.append(str(len(r._edge_across_host())))
        dedges, udedges = r._separate_edges()
        row.append(str(len(dedges)))
        row.append(str(len(r._edge_across_host(dedges))))
        row.append(r.result_fn())
        print ",".join(row)


def list_netsize(conf):
    src_dir = conf.get("dag", "output_dir")
    for fp in fslib.rep_dir(src_dir):
        r = PCOutput(conf).load(fp)
        d_size = {}
        for net in graph_network(r.graph):
            d_size[len(net)] = d_size.get(len(net), 0) + 1
        buf = []
        for size, cnt in sorted(d_size.items(), reverse = True):
            if cnt == 1:
                buf.append(str(size))
            else:
                buf.append("{0}x{1}".format(size, cnt))

        print "{0} : {1}".format(r.result_fn(), ", ".join(buf))


def whole_netsize(conf):
    src_dir = conf.get("dag", "output_dir")
    d_size = {}
    for fp in fslib.rep_dir(src_dir):
        r = PCOutput(conf).load(fp)
        for net in graph_network(r.graph):
            d_size[len(net)] = d_size.get(len(net), 0) + 1
    for size, cnt in d_size.items():
        print size, cnt


def show_result(conf, result, graph, dflag, limit):
    result.print_env() 
    result.print_result(graph)
    if dflag:
        result.print_result_detail(graph, limit)
    else:
        result.print_result_lt(graph)


def common_edge_graph(conf, r1, r2):
    g = nx.DiGraph()
    g1_edges = [r1._edge_info(edge) for edge in r1.graph.edges()]
    g2_edges = [r2._edge_info(edge) for edge in r2.graph.edges()]
    g.add_edges_from([r1._edge_id(edge) for edge in g1_edges
            if edge in g2_edges])
    return g # graph for r1


def diff_edge_graph(conf, r1, r2):
    g = nx.DiGraph()
    g1_edges = [r1._edge_info(edge) for edge in r1.graph.edges()]
    g2_edges = [r2._edge_info(edge) for edge in r2.graph.edges()]
    g.add_edges_from([r1._edge_id(edge) for edge in g1_edges
            if not edge in g2_edges])
    return g # graph for r1


def diff_label_graph(conf, r):
    g = nx.DiGraph()
    diff_edges = r._edge_across_label(l_edge = None, rest = False)
    for edge in diff_edges:
        g.add_edge(edge)
    return g


def cluster_results(conf):
    method = conf.get("cluster_graph", "dist_method")
    ig_direction = conf.getboolean("cluster_graph", "ig_direction")
    th_crit = conf.get("cluster_graph", "th_criterion")
    th = conf.getfloat("cluster_graph", "th")
    
    src_dir = conf.get("dag", "output_dir")
    l_result = [PCOutput(conf).load(fp) for fp in fslib.rep_dir(src_dir)]

    l_dist = []
    for r1, r2 in itertools.combinations(l_result, 2):
        if method == "ed":
            dist = graph_edit_distance(r1, r2, ig_direction)
        elif method == "mcs":
            dist = mcs_size_ratio(r1, r2, ig_direction)
        l_dist.append(dist)
    
    import scipy.cluster.hierarchy as hcls
    z = hcls.linkage(l_dist, method = 'single')
    hcls.dendrogram(z)
    import matplotlib.pyplot as plt
    plt.savefig('temp.pdf')

    fc = hcls.fcluster(z, th, criterion = th_crit)
    cdict = {}
    for c, r in zip(fc, l_result):
        cdict.setdefault(c, []).append(r)
    for cid, l_result in sorted(cdict.items(), key = lambda x: x[0]):
        print "[cluster {0}]".format(cid)
        for r in l_result:
            print r.filename


if __name__ == "__main__":

    import optparse
    usage = """
usage: {0} [options] args...
args:
  show-all : show abstraction of series of results
  show RESULT : show information of result DAG recorded in RESULT
  show-defail RESULT : show information of result DAG
                       with representative source log data
  graph RESULT GRAPH : output graph pdf as GRAPH
  common RESULT1 RESULT2 : show detail of edges in RESULT1
                           which appear in RESULT2
  diff RESULT1 RESULT2 : show detail of edges in RESULT1
                         which do not appear in RESULT2
    """.format(sys.argv[0]).strip()

    op = optparse.OptionParser(usage)
    op.add_option("-c", "--config", action="store",
            dest="conf", type="string", default=config.DEFAULT_CONFIG_NAME,
            help="configuration file path")
    op.add_option("-d", "--detail", action="store_true",
            dest="dflag", default=False,
            help="Show representative source log data")
    op.add_option("-e", "--edge", action="store_true",
            dest="eflag", default=False,
            help="Draw only nodes with adjacent edge")
    op.add_option("-l", "--limit", action="store",
            dest="show_limit", type="int", default=10,
            help="Limitation rows to show source log data")
    (options, args) = op.parse_args()

    conf = config.open_config(options.conf)
    config.set_common_logging(conf, _logger)
    mode = args.pop(0)
    if mode == "show-all":
        list_results(conf)
    elif mode == "show-all-detail":
        list_detailed_results(conf)
    elif mode == "show-all-netsize":
        list_netsize(conf)
    elif mode == "show-whole-netsize":
        whole_netsize(conf)
    elif mode == "show":
        if len(args) < 1:
            sys.exit("give me filename of pc result object")
        result = PCOutput(conf).load(args[0])
        show_result(conf, result, None, options.dflag, options.show_limit)
    elif mode == "graph": 
        if len(args) < 2:
            sys.exit("give me filename of pc result object, " + \
                    "and output filename of graph pdf")
        output = PCOutput(conf).load(args[0])
        output.show_graph(args[1], options.eflag)
    elif mode == "common":
        if len(args) < 2:
            sys.exit("give me 2 filenames of pc result object")
        r1 = PCOutput(conf).load(args[0])
        r2 = PCOutput(conf).load(args[1])
        graph = common_edge_graph(conf, r1, r2)
        show_result(conf, r1, graph, options.dflag, options.show_limit)
    elif mode == "diff":
        if len(args) < 2:
            sys.exit("give me 2 filenames of pc result object")
        r1 = PCOutput(conf).load(args[0])
        r2 = PCOutput(conf).load(args[1])
        graph = diff_edge_graph(conf, r1, r2)
        show_result(conf, r1, graph, options.dflag, options.show_limit)
    elif mode == "diff-label":
        if len(args) < 1:
            sys.exit("give me filename of pc result object")
        result = PCOutput(conf).load(args[0])
        graph = diff_label_graph(conf, result)
        show_result(conf, result, graph, options.dflag, options.show_limit)
    elif mode == "edit-distance":
        if len(args) < 2:
            sys.exit("give me 2 filenames of pc result object")
        r1 = PCOutput(conf).load(args[0])
        r2 = PCOutput(conf).load(args[1])
        print graph_edit_distance(r1, r2)
    elif mode == "cluster":
        cluster_results(conf)

