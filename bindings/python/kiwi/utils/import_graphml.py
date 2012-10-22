import sys
import xml.sax
from decorators import lru_cache
from kiwi.graph import KiwiGraph

VERTEX_ID = "node"
EDGE_ID = "edge"
DATA_ID = "data"

class GraphMLHandler(xml.sax.ContentHandler):
    def __init__(self, basepath):
        xml.sax.ContentHandler.__init__(self)
        
        self.in_vertex = False
        self.get_chars = False
        self.hashtag = ""

        self.in_edge = False
        self.get_data = False
        self.data = ""
        self.edge_id = ''
        self.edge_src = ''
        self.edge_dst = ''
        self.edge_label = ''

        self.graph = KiwiGraph(basepath)
        self.locator = None

    @lru_cache(100000)
    def get_vertex(self, id):
        return self.graph.get_vertex(id)

    def setDocumentLocator(self, locator):
        self.locator = locator
    
    def startElement(self, name, attrs):
        if self.in_vertex and name == DATA_ID:
            self.get_chars = True
            self.hashtag = ""
        elif self.in_edge and name == DATA_ID:
            self.get_data = True
            self.data = ""
        elif name == VERTEX_ID:
            self.in_vertex = True
            self.node_id = attrs.get('id', 0)
        elif name == EDGE_ID:
            self.in_edge = True
            self.edge_id = attrs.get('id', 0)
            self.edge_src = attrs.get('source', 0)
            self.edge_dst = attrs.get('target', 0)
            self.edge_label = attrs.get('label', '')
    
    def endElement(self, name):
        if self.get_chars and name == DATA_ID:
            self.get_chars = False
        if self.get_data and name == DATA_ID:
            self.get_data = False
        elif self.in_edge and name == EDGE_ID:
            self.in_edge = False

            src = self.get_vertex(self.edge_src)
            dst = self.get_vertex(self.edge_dst)
            edge = self.graph.add_edge(self.edge_id, src, dst, self.edge_label)
            edge.set('weight', self.data)

        elif self.in_vertex and name == VERTEX_ID:
            self.in_vertex = False

            vertex = self.graph.add_vertex(self.node_id)
            vertex.set('hashtag', self.hashtag)

        sys.stderr.write("Line: %d Vertices: %d Edges: %d\r" % \
            (self.locator.getLineNumber(), self.graph.vertices, self.graph.edges))
        sys.stderr.flush()

    def characters(self, content):
        if self.get_chars:
            self.hashtag += content
        if self.get_data:
            self.data += content

def main(filename, graphpath):
    handler = GraphMLHandler(graphpath)

    with open(filename, 'r') as inp:
        xml.sax.parse(inp, handler)

    handler.graph.shutdown()


if __name__ == "__main__":
    main("/home/nopper/graph.xml", "/tmp")
