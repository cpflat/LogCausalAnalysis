#!/usr/bin/env python
# coding: utf-8

import sys
import os
import datetime
import logging
import cPickle as pickle
import numpy as np
import networkx as nx

import fslib
import config
import lt_label

_logger = logging.getLogger(__name__)


class PCOutput():

    def __init__(self, conf):
        self.conf = conf
        self.ld = None
        self.ll = None
    
    def make(self, graph, evmap, top_dt, end_dt, dur, area):

        self.dirname = self.conf.get("dag", "output_dir")
        self.graph = graph
        self.evmap = evmap  # log2event.LogEventIDMap
        self.top_dt = top_dt
        self.end_dt = end_dt
        self.dur = dur
        self.area = area
        self.threshold = self.conf.getfloat("dag", "threshold")
        
        self.filename = self.get_fn()

    def load(self, fn = None):
        if not fn: fn = self.filename
        # do not use old configuration
        c = self.conf
        with open(fn, 'r') as f:
            d = pickle.load(f)
        self.__dict__.update(d)
        self.conf = c

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

    def _init_ll(self):
        if self.ll is None:
            import lt_label
            ltconf_path = self.conf.get("visual", "ltlabel")
            if ltconf_path == "":
                ltconf_path = lt_label.DEFAULT_LABEL_CONF
            self.ll = lt_label.LTLabel(ltconf_path)

    def _label_ltg(self, ltgid):
        self._init_ld()
        self._init_ll()
        return self.ll.get_ltg_label(ltgid, self.ld.ltg_members(ltgid))

    def get_fn(self):
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

    def _print_edge(self, edge, label):
        src_ltgid, src_host = self.evmap.info(edge[0])
        dst_ltgid, dst_host = self.evmap.info(edge[1])
        if label == "directed":
            arrow = "->"
        elif label == "undirected":
            arrow = "<->"
        else:
            raise ValueError
        print("{0} [{1}] ({2}) {6} {3}[{4}] ({5})".format(
                src_ltgid, self._label_ltg(src_ltgid), src_host, 
                dst_ltgid, self._label_ltg(dst_ltgid), dst_host,
                arrow))
    
    def _print_edge_lt(self, edge):
        for eid, header in zip(edge, ("src", "dst")):
            ltgid, host = self.evmap.info(eid)
            print("{0}> ltgid {1} [label {2}] (host {3})".format(
                    header, ltgid, self._label_ltg(ltgid), host))
            print("\n".join(
                [str(ltline) for ltline in self.ld.ltg_members(ltgid)]))
            print
    
    def _print_edge_detail(self, edge, limit = None):
        if self.area == "all":
            area = None
        elif self.area[:5] == "host_":
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
        for edges, label in zip(self._separate_edges(graph),
                ("directed", "undirected")):
            # directed, undirected
            for edge in edges:
                self._print_edge(edge, label)
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
                self._print_edge(edge, label)
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
                self._print_edge(edge, label)
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
            src_node, dst_node = edge
            src_ltgid, src_host = self._node_info(src_node)
            dst_ltgid, dst_host = self._node_info(dst_node)
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
        
        l_same = []
        l_diff = []
        for edge in l_edge:
            src_node, dst_node = edge
            src_ltgid, src_host = self._node_info(src_node)
            src_label = self._label_ltg(src_ltgid)
            dst_ltgid, dst_host = self._node_info(dst_node)
            dst_label = self._label_ltg(dst_ltgid)
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

        mapping = {}
        for node in graph.nodes():
            ltgid, host = self._node_info(node)
            label = self._label_ltg(ltgid)
            if label is None:
                mapping[node] = "{0}, {1}".format(ltgid, host)
            else:
                mapping[node] = "{0}({1}), {2}".format(ltgid, label, host)
        return nx.relabel_nodes(graph, mapping, copy=True)

    def show_graph(self, fn, graph = None, eflag = False):
        import matplotlib
        matplotlib.use('Agg')
        if graph is None:
            graph = self.graph
        if eflag:
            graph = graph_no_orphan(graph)
        rgraph = self.relabel_graph(graph)
        g = nx.to_agraph(rgraph)
        g.draw(fn, prog='circo')
        print ">", fn


class EdgeTFIDF():

    def __init__(self, l_result):
        self.d_doc = {} # key : cedge, val : the dataset where the cedge found
        self.l_result = [(rid, r) for rid, r in enumerate(l_result)]

        for rid, r in self.l_result:
            for edge in r.graph.edges():
                cedge = r._edge_info(edge)
                self._add_cedge(cedge, rid)

    def _add_cedge(self, cedge, rid):
        src_info, dst_info = cedge
        if self.d_doc.has_key((src_info, dst_info)):
            key = (src_info, dst_info)
            self.d_doc[key].add(rid)
        elif self.d_doc.has_key((dst_info, src_info)):
            key = (dst_info, src_info)
            self.d_doc[key].add(rid)
        else:
            key = (src_info, dst_info)
            self.d_doc[key] = set([rid])

    def _docs_with_cedge(self, cedge):
        src_info, dst_info = cedge
        if self.d_doc.has_key((src_info, dst_info)):
            key = (src_info, dst_info)
            return self.d_doc[key]
        elif self.d_doc.has_key((dst_info, src_info)):
            key = (dst_info, src_info)
            return self.d_doc[key]
        else:
            return set()

    def _rid(self, r):
        for rid, temp_r in self.l_result:
            if temp_r.filename == r.filename:
                return rid
        else:
            raise ValueError(
                    "{0} not given for initialization of EdgeTFIDF".format(
                        r.filename))

    def tf(self, cedge, r):
        rid = self._rid(r)
        if rid in self._docs_with_cedge(cedge):
            return 1.0
        else:
            return 0.0

    def idf(self, cedge, r):
        doc_size = len(self.l_result)
        doc_with_cedge = len(self._docs_with_cedge(cedge))
        if doc_with_cedge == 0:
            import pdb; pdb.set_trace()
        return np.log2(1.0 * doc_size / doc_with_cedge)

    def tfidf(self, cedge, r):
        return self.tf(cedge, r) * self.idf(cedge, r)

    def weight(self, w, doc):
        return self.tfidf(w, doc)


# functions for graph


def equal_edge(cedge1, cedge2, ig_host = False):
    # return True if the adjacent nodes of cedge1 is same as that of cedge2
    # If ig_host is True, ignore difference of hosts
    src_info1, dst_info1 = cedge1
    src_info2, dst_info2 = cedge2
    if ig_host:
        src_ltgid1, src_host1 = src_info1
        dst_ltgid1, dst_host1 = dst_info1
        src_ltgid2, src_host2 = src_info2
        dst_ltgid2, dst_host2 = dst_info2
        if (src_ltgid1, dst_ltgid1) == (src_ltgid2, dst_ltgid2):
            return True
        elif (src_ltgid1, dst_ltgid1) == (dst_ltgid2, src_ltgid2):
            return True
        else:
            return False
    else:
        if (src_info1, dst_info1) == (src_info2, dst_info2):
            return True
        elif (src_info1, dst_info1) == (dst_info2, src_info2):
            return True
        else:
            return False


def graph_no_orphan(src):
    g = nx.DiGraph()
    g.add_edges_from([edge for edge in src.edges()])
    return g


def mcs_size_ratio(r1, r2, ig_direction = False, weight = None):
    mcs = maximum_common_subgraph(r1, r2, ig_direction)

    if weight is None:
        size = len(mcs.edges())
    else:
        mcs_cedges = [r1._edge_info(edge) for edge in mcs.edges()]
        size = sum([1.0 * weight.idf(cedge, r1) for cedge in mcs_cedges])
            # if use tf, fail for edges only in r2

    return (len(r1.graph.edges()) + len(r2.graph.edges())) / (2.0 * size)


def maximum_common_subgraph(r1, r2, ig_direction = False):
    
    g1_cedges = [r1._edge_info(edge) for edge in r1.graph.edges()]
    g2_cedges = [r2._edge_info(edge) for edge in r2.graph.edges()]

    if ig_direction:
        g = nx.Graph()
        for cedge in g1_cedges:
            src_info, dst_info = cedge
            if (src_info, dst_info) in g2_cedges or \
                    (dst_info, src_info) in g2_cedges:
                g.add_edge(r1._node_id(src_info), r1._node_id(dst_info))
        return nx.DiGraph(g)
    else:
        g = nx.DiGraph()
        for cedge in g1_cedges:
            if cedge in g2_cedges:
                g.add_edge(r1._node_id(cedge[0]), r1._node_id(cedge[1]))
        return g


def graph_edit_distance(r1, r2, ig_direction = False, weight = None):
    # weight : weight object like TFIDF()

    def owned(cedge, l_cedges, ig_direction):
        src_info, dst_info = cedge
        if cedge in l_cedges:
            return True
        elif ig_direction and (dst_info, src_info) in l_cedges:
            return True
        else:
            return False

    g1_cedges = [r1._edge_info(edge) for edge in r1.graph.edges()]
    g2_cedges = [r2._edge_info(edge) for edge in r2.graph.edges()]

    l_edit = []
    # counting "add edges only in g2"
    l_edit = l_edit + [cedge for cedge in g2_cedges
            if not owned(cedge, g1_cedges, ig_direction)]
    # counting "remove edges only in g1"
    l_edit = l_edit + [cedge for cedge in g1_cedges
            if not owned(cedge, g2_cedges, ig_direction)]

    if weight is None:
        return float(len(l_edit))
    else:
        return sum([1.0 * weight.idf(cedge, r1) for cedge in l_edit])
            # if use tf, fail for edges only in r2


#def graph_edit_distance(r1, r2, ig_direction = False):
#    # ignore orphan nodes, only seeing edges
#
#    g1_edges = [r1._edge_info(edge) for edge in r1.graph.edges()]
#    g2_edges = [r2._edge_info(edge) for edge in r2.graph.edges()]
#
#    l_edit = []
#    # counting "add edges only in g2"
#    l_edit = l_edit + [edge for edge in g2_edges if not edge in g1_edges]
#    # counting "remove edges only in g1"
#    l_edit = l_edit + [edge for edge in g1_edges if not edge in g2_edges]
#
#    # get combination in l_edit
#    s = set()
#    for edit in l_edit:
#        for e in s:
#            if edit[0] == e[1] and edit[1] == e[0]:
#                break
#        else:
#            s.add(edit)
#
#    def get_direction(r, edge):
#        # edge : edge_info
#        if (not r._has_node(edge[0])) or (not r._has_node(edge[1])):
#            return "none"
#        src_node = r._node_id(edge[0])
#        dst_node = r._node_id(edge[1])
#        if (src_node, dst_node) in r.graph.edges():
#            if (dst_node, src_node) in r.graph.edges():
#                return "both"
#            else:
#                return "src_to_dst"
#        elif (dst_node, src_node) in r.graph.edges():
#            return "dst_to_src"
#        else:
#            return "none"
#    pass
#
#    if ig_direction:
#        # count only if either graph is "none"
#        ret = 0.0
#        for edge in s:
#            d1 = get_direction(r1, edge)
#            d2 = get_direction(r2, edge)
#            if "none" in (d1, d2):
#                ret += 1.0
#    else:
#        ret = 1.0 * len(s)
#
#    return ret


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


# function for results

def result_areas(conf):
    s_area = set()
    src_dir = conf.get("dag", "output_dir")
    for fp in fslib.rep_dir(src_dir):
        r = PCOutput(conf).load(fp)
        s_area.add(r.area)
    return list(s_area)


# functions for visualization

def list_results(conf):
    src_dir = conf.get("dag", "output_dir")
    l_result = []
    for fp in fslib.rep_dir(src_dir):
        l_result.append(PCOutput(conf).load(fp))
    l_result.sort(key = lambda r: r.area)

    print "datetime\t\tarea\tnodes\tedges\tfilepath"
    for r in l_result:
        print "\t".join((str(r.top_dt), r.area,
                str(len(r.graph.nodes())),
                str(len(r.graph.edges())), r.result_fn()))


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
        g.add_edge(edge[0], edge[1])
    return g


def similar_graph(conf, result, area, alg, cand = 20):
    # ed, mcs, edw, mcsw
    assert result.area == area
    src_dir = conf.get("dag", "output_dir")
    l_result = []
    for fp in fslib.rep_dir(src_dir):
        r = PCOutput(conf).load(fp)
        if r.area == area:
            l_result.append(r)

    weight = None
    if "w" in alg:
        weight = EdgeTFIDF(l_result)

    data = []
    for r in l_result:
        if r.filename == result.filename:
            continue
        if alg.rstrip("w") == "ed":
            dist = graph_edit_distance(result, r, True, weight)
        elif alg.rstrip("w") == "mcs":
            dist = mcs_size_ratio(result, r, True, weight)
        else:
            raise ValueError()
        data.append((dist, r))

    data = sorted(data, key = lambda x: x[0], reverse = False)
    for d in data[:cand]:
        print d[0], d[1].filename


if __name__ == "__main__":

    usage = """
usage: {0} [options] args...
args:
  show-all : show abstraction of the series of results
  show-all-detail : show detailed abstraction
  all-netsize : show values about network size that
                every node have a path to others in same network
  whole-netsize : show sum of network size in all results
  
  show RESULT : show information of result DAG recorded in RESULT
  common RESULT1 RESULT2 : show details of edges in RESULT1
                           that appear in RESULT2
  diff RESULT1 RESULT2 : show details of edges in RESULT1
                         that do not appear in RESULT2
  diff-label RESULT : show details of edges in RESULT
                      that is across different label of log template group
  edit-distance RESULT1 RESULT2 : show graph edit distance value
                                  between RESULT1 and RESULT2
    """.format(sys.argv[0]).strip()

    import optparse
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
    op.add_option("-g", "--graph", action="store",
            dest="graph_fn", type="string", default=None,
            help="output graph pdf")
    op.add_option("-l", "--limit", action="store",
            dest="show_limit", type="int", default=10,
            help="Limitation rows to show source log data")
    (options, args) = op.parse_args()

    conf = config.open_config(options.conf)
    config.set_common_logging(conf, _logger)
    if len(args) == 0:
        sys.exit(usage)
    mode = args.pop(0)
    if mode == "show-all":
        list_results(conf)
    elif mode == "show-all-detail":
        list_detailed_results(conf)
    elif mode == "all-netsize":
        list_netsize(conf)
    elif mode == "whole-netsize":
        whole_netsize(conf)
    elif mode == "show":
        if len(args) < 1:
            sys.exit("give me filename of pc result object")
        result = PCOutput(conf).load(args[0])
        show_result(conf, result, None, options.dflag, options.show_limit)
        if options.graph_fn is not None:
            result.show_graph(options.graph_fn, None, options.eflag)
    elif mode == "common":
        if len(args) < 2:
            sys.exit("give me 2 filenames of pc result object")
        r1 = PCOutput(conf).load(args[0])
        r2 = PCOutput(conf).load(args[1])
        graph = common_edge_graph(conf, r1, r2)
        show_result(conf, r1, graph, options.dflag, options.show_limit)
        if options.graph_fn is not None:
            r1.show_graph(options.graph_fn, graph, options.eflag)
    elif mode == "diff":
        if len(args) < 2:
            sys.exit("give me 2 filenames of pc result object")
        r1 = PCOutput(conf).load(args[0])
        r2 = PCOutput(conf).load(args[1])
        graph = diff_edge_graph(conf, r1, r2)
        show_result(conf, r1, graph, options.dflag, options.show_limit)
        if options.graph_fn is not None:
            r1.show_graph(options.graph_fn, graph, options.eflag)
    elif mode == "diff-label":
        if len(args) < 1:
            sys.exit("give me filename of pc result object")
        result = PCOutput(conf).load(args[0])
        graph = diff_label_graph(conf, result)
        show_result(conf, result, graph, options.dflag, options.show_limit)
        if options.graph_fn is not None:
            result.show_graph(options.graph_fn, graph, options.eflag)
    elif mode == "edit-distance":
        if len(args) < 2:
            sys.exit("give me 2 filenames of pc result object")
        r1 = PCOutput(conf).load(args[0])
        r2 = PCOutput(conf).load(args[1])
        print graph_edit_distance(r1, r2)
    elif mode in ("similar-graph-ed", "similar-graph-mcs",
            "similar-graph-edw", "similar-graph-mcsw"):
        if len(args) < 2:
            sys.exit("give me area and filename of pc result object")
        alg = mode.rpartition("-")[-1]
        area = args[0]
        r = PCOutput(conf).load(args[1])
        similar_graph(conf, r, area, alg)
    else:
        print "invalid argument"
        sys.exit(usage)

