import sys
import gzip
from decorators import lru_cache
from kiwi.graph import KiwiGraph

class SimpleImporter(object):
    def __init__(self, filename, outputpath):
        self.graph = KiwiGraph(outputpath)
        self.filename = filename

    @lru_cache(1000)
    def get_vertex(self, id):
        v = self.graph.getVertex(id)
        v.save()
        return v

    def run(self):
        g = self.graph

        with gzip.open(self.filename, 'r') as input:
            vertices = 0
            edges = 0

            for line in input:
                src, dst = line.split()
                src = self.get_vertex(int(src))
                dst = self.get_vertex(int(dst))
                vertices += 2

                g.addEdge(edges, src, dst, "link")

                edges += 1

                if edges % 1000 == 0:
                    sys.stderr.write("Vertices: %d Edges: %d\r" % (vertices, edges))
                    sys.stderr.flush()

        g.shutdown()

if __name__ == "__main__":
    imp = SimpleImporter("/home/nopper/Desktop/Sources/indexer/WikipediaEdges.gz", "/tmp/wiki")
    imp.run()
