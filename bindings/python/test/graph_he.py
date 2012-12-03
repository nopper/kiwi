import sys
from kiwi.graph import KiwiGraph

vertices = {}
vertex_id = int(sys.argv[2])

def vertices_out(g, v, wikipedia):
    ret = [i.getId() for i in g.v(v).outV()]
    return ret

def vertices_in(g, v, wikipedia):
    ret = [i.getId() for i in g.v(v).inV()]
    for item in ret:
        if wikipedia:
            assert (item > 10000000)
        else:
            assert (item < 10000000)
    return ret

g = KiwiGraph(sys.argv[1])

vertices[vertex_id] = vertices_out(g, vertex_id, True)

for v in vertices_out(g, vertex_id, True):
    print "Hashtags", v
    vertices[v] = vertices_in(g, v, False)

    for k in vertices[v]:
        vertices[k] = vertices_out(g, k, True)

print vertices
