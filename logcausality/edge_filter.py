#!/usr/bin/env python
# coding: utf-8

import sys
import os
import logging
import cPickle as pickle

import common
import config
import pcresult

_logger = logging.getLogger(__name__.rpartition(".")[-1])


class EdgeFilter():

    def __init__(self, conf):
        self.method = conf.get("visual", "edge_filter_method")
        self.threshold = conf.getfloat("visual", "edge_filter_th")
        self.filename = conf.get("visual", "edge_filter_file")
        self.src_dir = conf.get("dag", "output_dir")
        self.latest = None
        if os.path.exists(self.filename):
            self.load()
        if self.latest is None or self._update_check():
            self._init_dict(conf)
            self.dump()

    def _update_check(self):
        lm = common.last_modified(common.rep_dir(self.src_dir))
        return lm > self.latest

    def _init_dict(self, conf):
        import pcresult
        l_result = pcresult.results(conf)
        if self.method == "count":
            self.clsobj = _ClassifierOfCount(l_result, self.threshold)
        elif self.method == "count_ighost":
            self.clsobj = _ClassifierOfCountIgHost(l_result, self.threshold)
        elif self.method == "cont":
            areas = pcresult.result_areas(conf)
            self.clsobj = _ClassifierOfContinuation(l_result,
                    self.threshold, areas)
        elif self.method == "cont_ighost":
            areas = pcresult.result_areas(conf)
            self.clsobj = _ClassifierOfContinuationIgHost(l_result,
                    self.threshold, areas)
        #elif self.method == "tfidf":
        #    pass
        #elif self.method == "tfidf_ighost":
        #    pass

    def isfiltered(self, cedge):
        return self.clsobj.isfiltered(cedge)

    def show_all(self, l_result):
        return self.clsobj.show_all(l_result)

    def load(self):
        """Restore event periodicity data from a serialized file."""
        with open(self.filename, 'r') as f:
            self.__dict__ = pickle.load(f)

    def dump(self):
        """Serialize event periodicity with cPickle."""
        obj = self.__dict__
        with open(self.filename, 'w') as f:
            pickle.dump(obj, f)


class _Classifier(object):

    def __init__(self, *args):
        self._d_expl = common.SequenceKeyDict()

    def _add_expl(self, cedge, r):
        key = self._key(cedge)
        if self._d_expl.has_key(key):
            return False
        else:
            self._d_expl[key] = r.info2str(cedge)
            return True

    def _expl(self, cedge):
        return self._d_expl.get(cedge, "Not Found")


class _ClassifierOfCount(_Classifier):

    def __init__(self, l_result, threshold):
        _Classifier.__init__(self, l_result, threshold)
        self._d_cnt = common.SequenceKeyDict()
        for r in l_result:
            for cedge in r.iter_edge_info():
                key = self._key(cedge)
                self._add_expl(cedge, r)
                self._d_cnt[key] = self._d_cnt.get(key, 0) + 1
        l_stat = sorted(self._d_cnt.items(),
                key = lambda x: x[1], reverse = True)
        length = len(self._d_cnt)
        filtered_num = int(length * threshold)
        self._th_val = l_stat[filtered_num]

    def _key(self, cedge):
        return cedge

    def isfiltered(self, cedge, *args):
        key = self._key(cedge)
        return self._d_cnt[key] >= self._th_val

    def show_all(self, *args):
        for key, cnt in sorted(self._d_cnt.items(),
                key = lambda x: x[1], reverse = True):
            if cnt >= self._th_val:
                sys.stdout.write("! ")
            print "{0} times : {1}".format(cnt, self._expl(key))


class _ClassifierOfCountIgHost(_ClassifierOfCount):

    def _key(self, cedge):
        return cedge[0]


class _ClassifierOfContinuation(_Classifier):

    def __init__(self, l_result, threshold, areas):
        _Classifier.__init__(self, l_result, threshold)
        self._d_cont = common.SequenceKeyDict()
        self._ridmap = pcresult.PCOutputIDMap(l_result)
        self._th_val = threshold
        for area in areas:
            d_temp_cont = {}
            l_result_area = [r for r in l_result if r.area == area]
            for r in l_result_area:
                rid = self._ridmap.rid(r)
                l_cedge = [e for e in r.iter_edge_info()]
                s_lost_cedge = set(d_temp_cont.keys()) - set(l_cedge)
                for cedge in l_cedge:
                    self._add_expl(cedge, r)
                    key = self._key(cedge)
                    key2 = self._key2(rid, cedge)
                    cont = d_temp_cont.get(key, 0) + 1
                    d_temp_cont[key] = cont
                    self._d_cont[key2] = cont
                for cedge in s_lost_cedge:
                    assert not cedge in l_cedge
                    key = self._key(cedge)
                    d_temp_cont.pop(key)

    def _key(self, cedge):
        return cedge

    def _key2(self, rid, cedge):
        return (rid, self._key(cedge))

    def isfiltered(self, cedge, r):
        rid = self._ridmap.rid(r)
        key = self._key2(rid, cedge)
        return self._d_cont[key] >= self._th_val

    def show_all(self, l_result):
        for r in l_result:
            print("# {0}".format(r.filename))
            rid = self._ridmap.rid(r)
            l_temp = []
            for cedge in r.iter_edge_info():
                key = self._key2(rid, cedge)
                cont = self._d_cont[key]
                l_temp.append((cont, self._expl(cedge)))
            for cont, expl in sorted(l_temp,
                    key = lambda x: x[0], reverse = True):
                if cont >= self._th_val:
                    sys.stdout.write("! ")
                print("{0} : {1}".format(cont, expl))


class _ClassifierOfContinuationIgHost(_ClassifierOfContinuation):

    def _key(self, cedge):
        return cedge

    
#class _ClassifierOfTFIDF(object):
#
#    def __init__(self, l_result, threshold):
#        self._tfidfobj = pcresult.EdgeTFIDF(l_result)
#        self._d_score = common.SequenceKeyDict()
#        for r in l_result:
#            for cedge in r.iter_edge_info():
#                key = self._key(cedge)
#                self._d_score[key] = self._tfidfobj.tfidf(cedge, r)
#        self._
#        #TODO
#
#    def _key(self, cedge):
#        return cedge
#
#    def isfiltered(self, cedge, r):
#        self.tfidf


def test_edge_filter(conf):
    l_result = pcresult.results(conf)
    ef = EdgeFilter(conf)
    ef.show_all(l_result)


def test_edge_filter_cont(conf):
    conf.set("visual", "edge_filter_method", "cont")
    conf.set("visual", "edge_filter_th", "2.0")
    l_result = pcresult.results(conf)
    ef = EdgeFilter(conf)
    ef.show_all(l_result)


def graph_edge_filter(conf):
    ef = EdgeFilter(conf)



if __name__ == "__main__":
    usage = "usage: {0} [options]".format(sys.argv[0])
    import optparse
    op = optparse.OptionParser(usage)
    op.add_option("-c", "--config", action="store",
            dest="conf", type="string", default=config.DEFAULT_CONFIG_NAME,
            help="configuration file path")
    op.add_option("-d", "--delete", action="store_true",
            dest="delete", default=False,
            help="clean edge_filter dump file and reconstruct")
    
    (options, args) = op.parse_args()

    conf = config.open_config(options.conf)
    if options.delete:
        filename = conf.get("visual", "edge_filter_file")
        common.rm(filename)
    #test_edge_filter(conf)
    test_edge_filter_cont(conf)





