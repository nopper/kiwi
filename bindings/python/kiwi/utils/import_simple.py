import sys
import gzip
from time import time
from decorators import lfu_cache
from kiwi.graph import KiwiGraph
from kiwi.vertex import KiwiVertex
from kiwi.edge import KiwiEdge

class SimpleImporter(object):
    def __init__(self, filename, outputpath):
        self.graph = KiwiGraph(outputpath)
        self.filename = filename

    @lfu_cache(200000)
    def get_vertex(self, id):
        v = self.graph.getVertex(id)
        v.save()
        return v

    def run(self):
        g = self.graph

        with open(self.filename, 'r') as input:
            vertices = 0
            edges = 0
            start = time()

            for line in input:
                src, dst = line.split()
                src = self.get_vertex(int(src))
                dst = self.get_vertex(int(dst))
                vertices += 2

                g.addEdge(edges, src, dst, "link")
                edges += 1

                if edges % 1000 == 0:
                    curr = time()
                    eps = edges / (curr - start)

                    sys.stderr.write("Vertices: %d Edges: %d EPS: %.2f\r" % (vertices, edges, eps))
                    sys.stderr.flush()

        # This should be the reason of the crash
        self.get_vertex.clear()
        g.shutdown()

if __name__ == "__main__":
    #imp = SimpleImporter("/home/nopper/Desktop/Sources/indexer/WikipediaEdges.gz", "/tmp/wiki")
    imp = SimpleImporter(sys.argv[1], sys.argv[2])
    imp.run()
