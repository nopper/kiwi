from msgpack import dumps
from bisect import insort
from db import DB, DBIterator
from edge import KiwiEdge
from vertex import KiwiVertex

#import leveldb
#
#class DB(object):
#    def __init__(self, basedir):
#        self.l = leveldb.LevelDB(basedir)
#
#    def add(self, k, v):
#        self.l.Put(k, v)
#
#    def remove(self, k):
#        self.l.Del(k, v)
#
#    def get(Self, k):
#        return self.l.Get(k)

class KiwiGraph(object):
    def __init__(self, basedir):
        self.db = DB(basedir)
        self.basedir = basedir
        self.vertices = 0
        self.edges = 0

    def addVertex(self, id):
        self.vertices += 1
        vertex = KiwiVertex(self, id)
        vertex.save()
        return vertex

    def getVertex(self, id):
        return KiwiVertex(self, id)

    def addEdge(self, id, src, dst, label):
        self.edges += 1

        # Here we assume that the two vertex are present.
        edge = KiwiEdge(self, id)

        edge.reset(dst, src, label)
        edge.save()

        # We could add this as a method to a vertex but
        # the vertex should not be able to modify this information
        # directly but only to consume it

        def append_missing(vertex, var, id):
            lst = vertex.getProperty(var, [])
            insort(lst, int(id))
            vertex.setProperty(var, lst)

        append_missing(src, 'outE', id)
        append_missing(dst, 'inE', id)

        return edge

    def getEdge(self, id):
        return KiwiEdge(self, id)

    def getEdges(self, key=None, value=None):
        pass

    def getVertices(self, key=None, value=None):
        pass

    def shutdown(self):
        self.db.close()

    def __str__(self):
        return "KiwiGraph[%s] - [%d vertices, %d edges]" % \
               (self.basedir, self.vertices, self.edges)

    def __repr__(self):
        return str(self)


    # Gremlin interface
    def v(self, id):
        return self.getVertex(id)

    def e(self, id):
        return self.getEdge(id)

if __name__ == "__main__":
    g = KiwiGraph('/tmp')

    #assert map(lambda v: v.getId(), g.v(1).outV()) == [2, 4, 3]
    #assert g.e(12).outV().getId() == 6
    #assert g.e(12).inV().getId() == 3
    #assert map(lambda v: v.getId(), g.e(12).bothV()) == [6, 3]
    #assert map(lambda v: v.getId(), g.v(3).inV(["created"])) == [1, 4, 6]
    #assert map(lambda e: e.outV().getId(), g.v(3).inE(["created"])) == [1, 4, 6]

    import readline
    from code import InteractiveConsole
    InteractiveConsole(locals()).interact("v.getEdges('OUT', ['knows'])")
