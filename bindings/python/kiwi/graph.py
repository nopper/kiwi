from msgpack import dumps
from db import DB, DBIterator
from edge import KiwiEdge
from vertex import KiwiVertex

class KiwiGraph(object):
    def __init__(self, basedir):
        self.db = DB(basedir)
        self.basedir = basedir
        self.vertices = 0
        self.edges = 0

    def add_vertex(self, id):
        self.vertices += 1
        self.db.add('V/%s' % str(id), '')
        return KiwiVertex(self, id)

    def get_vertex(self, id):
        return KiwiVertex(self, id)

    def add_edge(self, id, src, dst, label):
        self.edges += 1
        self.db.add('E/%s' % str(id), dumps([src.get_id(), dst.get_id(), label]))

        # Here we assume that the two vertex are present.

        edge = KiwiEdge(self, id)

        # We could add this as a method to a vertex but
        # the vertex should not be able to modify this information
        # directly but only to consume it

        def append_missing(vertex, var, label, id):
            dct = vertex.get(var, {})
            lst = dct.get(label, [])
            lst.append(int(id))
            dct[label] = lst
            vertex.set(var, dct)

        append_missing(src, 'outE', label, id)
        append_missing(src, 'outV', label, dst.id)
        append_missing(dst, 'inE', label, id)
        append_missing(dst, 'inV', label, src.id)

        return edge

    def get_edge(self, id):
        return KiwiEdge(self, id)

    def shutdown(self):
        self.db.close()

    def __str__(self):
        return "KiwiGraph[%s] - [%d vertices, %d edges]" % \
               (self.basedir, self.vertices, self.edges)

    def __repr__(self):
        return str(self)

if __name__ == "__main__":
    g = KiwiGraph('/tmp')
    g.add_edge(3, g.add_vertex(1), g.add_vertex(2), 'knows')
    g.shutdown()

    g = KiwiGraph('/tmp')
    print g.get_edge(3)