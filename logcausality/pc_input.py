#!/usr/bin/env python
# coding: utf-8

import numpy as np
import networkx as nx
from logging import getLogger

_logger = getLogger(__name__)


def pc(d_dt, threshold, mode = "pylib"):
    if mode == "rlib":   
        graph = pc_rlib(d_dt, threshold)
    elif mode == "pylib":
        graph = pc_pylib(d_dt, threshold)
    #print graph.edges()
    #import cPickle as pickle
    #with open("graph_dump", 'w') as f:
    #    pickle.dump(graph, f)
    return graph


def pc_pylib(d_dt, threshold):
    import pcalg
    from gsq.ci_tests import ci_test_bin

    dm = np.array([data for nid, data in sorted(d_dt.iteritems())]).transpose()
    (g, sep_set) = pcalg.estimate_skeleton(indep_test_func=ci_test_bin,
                                     data_matrix=dm,
                                     alpha=threshold)
    g = pcalg.estimate_cpdag(skel_graph=g, sep_set=sep_set)
    return g


def pc_rlib(d_dt, threshold):
    import pandas
    import pyper

    input_data = d_dt
    #input_data = {}
    #for nid, ns in nsdict.iteritems():
    #    input_data[nid] = ns.get_values()

    r = pyper.R(use_pandas='True')
    r("library(pcalg)")
    r("library(graph)")

    df = pandas.DataFrame(input_data)
    r.assign("input.df", df)
    r("evts = as.matrix(input.df)")
    #print r("evts")
    #r("t(evts)")
    
    #r("save(evts, file='rtemp')")

    r.assign("event.num", len(input_data))
    r.assign("threshold", threshold)

    r("""
        pc.result <- pc(suffStat = list(dm = evts, adaptDF = FALSE),
            indepTest = binCItest, alpha = threshold,
            labels = as.character(seq(event.num)-1), verbose = FALSE)
    """)
    #print r("""
    #    pc.result <- pc(suffStat = list(dm = evts, adaptDF = FALSE),
    #        indepTest = binCItest, alpha = threshold,
    #        labels = as.character(seq(event.num)-1), verbose = TRUE)
    #""")

    r("node.num <- length(nodes(pc.result@graph))")

    g = nx.DiGraph()
    for i in range(r.get("node.num")):
        r.assign("i", i)
        edges = r.get("pc.result@graph@edgeL[[as.character(i)]]$edges")
        if edges is None:
            pass
        elif type(edges) == int:
            g.add_edge(i, edges - 1)
        elif type(edges) == np.ndarray:
            for edge in edges:
                g.add_edge(i, edge - 1)
        else:
            raise ValueError("edges is unknown type {0}".format(type(edges)))
    return g


def empty_dag():
    return nx.DiGraph()


