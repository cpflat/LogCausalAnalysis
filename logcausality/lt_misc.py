#!/usr/bin/env python
# coding: utf-8


class LTSearchTree():
    # Search tree for un-incremental lt generation algorithms

    def __init__(self, sym):
        self.sym = sym
        self.root = self._new_node()

    def __str__(self):
        l_buf = []

        def print_children(point, depth):
            word = point.word
            if word is None: word = "**"
            buf = "-" * depth + " {0}".format(point.word)
            if point.end is not None:
                buf += "  <-- ltid {0}".format(point.end)
            l_buf.append(buf)
            for word in point.windex.keys():
                print_children(point.windex[word], depth + 1)
            if point.wild is not None:
                print_children(point.wild, depth + 1)

        point = self.root
        l_buf.append("<head of log template search tree>")
        for word in point.windex.keys():
            print_children(point.windex[word], 1)
        if point.wild is not None:
            print_children(point.wild, 1)
        return "\n".join(l_buf)

    @staticmethod
    def _new_node(parent = None, word = None):
        return LTSearchTreeNode(parent, word)

    def add(self, ltid, ltwords):
        point = self.root
        for w in ltwords:
            if w == self.sym:
                if point.wild is None:
                    point.wild = self._new_node(point, w)
                point = point.wild
            else:
                if not point.windex.has_key(w):
                    point.windex[w] = self._new_node(point, w)
                point = point.windex[w]
        else:
            point.set_ltid(ltid)

    def _trace(self, ltwords):
        point = self.root
        temp_ltwords = ltwords[:]
        check_points = []
            # points with 2 candidates to go, for example "word" and "**"
            # [(node, len(left_words)), ...]
            # use as stack
        while True:
            w = temp_ltwords.pop(0)
            if w == self.sym:
                if point.wild is None:
                    return None
                else:
                    point = point.wild
            elif point.windex.has_key(w):
                if point.wild is not None:
                    check_points.append((point, len(temp_ltwords)))
                point = point.windex[w]
            elif point.wild is not None:
                point = point.wild
            else:
                if len(check_points) == 0:
                    return None
                else:
                    p, left_wlen = check_points.pop(-1)
                    temp_ltwords = ltwords[-left_wlen+1:]
                        # +1 : for one **(wild) node
                    point = p.wild

            if len(temp_ltwords) == 0:
                if point.end is None:
                    if len(check_points) == 0:
                        return None
                    else:
                        p, left_wlen = check_points.pop(-1)
                        temp_ltwords = ltwords[-left_wlen+1:]
                        point = p.wild
                else:
                    return point

    def remove(self, ltid, ltwords):
        node = self._trace(ltwords)
        if node is None:
            _logger.warning(
                    "LTSearchTree : Failed to remove ltid {0}".format(ltid))
        point.remove_ltid(ltid)
        while point.unnecessary():
            w = point.word
            point = point.parent
            if w is None:
                point.wild = None
            else:
                point.wdict.pop(w)
        else:
            if self.root is None:
                self.root = self._new_node()

    def search(self, ltwords):
        node = self._trace(ltwords)
        if node is None:
            return None
        else:
            return node.get_ltid()


class LTSearchTreeNode():

    def __init__(self, parent, word):
        self.windex = {}
        self.wild = None
        self.end = None
        self.parent = parent # for reverse search to remove
        self.word = word

    def child(self, word = None):
        if word is None:
            # wildcard
            return self.wild
        elif self.windex.has_key(word):
            return self.windex[word]
        else:
            return None

    def current_point(self):
        buf = []
        point = self
        while point.parent is not None:
            buf = [point.word] + buf
            point = point.parent
        print " ".join(buf)

    def set_ltid(self, ltid):
        self.end = ltid

    def remove_ltid(self, ltid):
        assert self.end == ltid
        self.end = None

    def get_ltid(self):
        return self.end

    def unnecessary(self):
        return (len(self.windex) == 0) and \
                (self.wild is None) and \
                (self.end is None)


