import sys
import xml.sax
from time import time
from decorators import lfu_cache
from kiwi.graph import KiwiGraph

VERTEX_ID = "node"
EDGE_ID = "edge"
DATA_ID = "data"
KEY_ID = "key"

class GraphMLHandler(xml.sax.ContentHandler):
    def __init__(self, basepath):
        xml.sax.ContentHandler.__init__(self)

        self.in_vertex = False
        self.get_chars = False
        self.data = ""
        self.attributes = []
        self.key_id = ""

        self.in_edge = False
        self.get_data = False
        self.edge_id = ''
        self.edge_src = ''
        self.edge_dst = ''
        self.edge_label = ''

        self.vertex_conv = {}
        self.edge_conv = {}

        self.graph = KiwiGraph(basepath)
        self.locator = None

        self.conversions = {
            "string": unicode,
            "int": int,
            "float": float,
        }

        self.start = None

    @lfu_cache(200000)
    def get_vertex(self, id):
        return self.graph.getVertex(id)

    def setDocumentLocator(self, locator):
        self.locator = locator

    def startElement(self, name, attrs):
        if (self.in_edge or self.in_vertex) and name == DATA_ID:
            self.get_data = True
            self.data = ""

            self.key_id = attrs.get("key")

        elif name == VERTEX_ID:
            self.in_vertex = True
            self.node_id = attrs.get('id', 0)
            self.attributes = []

        elif name == EDGE_ID:
            self.in_edge = True
            self.edge_id = attrs.get('id', 0)
            self.edge_src = attrs.get('source', 0)
            self.edge_dst = attrs.get('target', 0)
            self.edge_label = attrs.get('label', '')
            self.attributes = []

        elif name == KEY_ID:
            #<key id="lang" for="node" attr.name="lang" attr.type="string"/>
            key_id = attrs.get('id', None)
            key_for = attrs.get('for', None)

            key_name = attrs.get('attr.name', None)
            key_type = attrs.get('attr.type', "string")

            if not all((key_id, key_for, key_name, key_type)) or key_id != key_name:
                raise Exception("Error while parsing key attribute")

            if key_type not in self.conversions:
                raise Exception("Unable to parse attribute type")

            if key_for == "node":
                self.vertex_conv[key_id] = (self.conversions[key_type], key_name)
            elif key_for == "edge":
                self.edge_conv[key_id] = (self.conversions[key_type], key_name)

    def endElement(self, name):
        if self.get_data and name == DATA_ID:
            self.get_data = False

            if self.in_edge:
                conv = self.edge_conv
            else:
                conv = self.vertex_conv

            cast, name = conv[self.key_id]
            self.attributes.append((name, cast(self.data)))

            self.data = ""
            self.key_id = ""

        elif self.in_edge and name == EDGE_ID:
            self.in_edge = False

            if self.start is None:
                self.start = time()

            src = self.get_vertex(self.edge_src)
            dst = self.get_vertex(self.edge_dst)
            edge = self.graph.addEdge(self.edge_id, src, dst, self.edge_label)

            for name, value in self.attributes:
                edge.setProperty(name, value)

            self.attributes = []

        elif self.in_vertex and name == VERTEX_ID:
            self.in_vertex = False

            vertex = self.graph.addVertex(self.node_id)

            for name, value in self.attributes:
                vertex.setProperty(name, value)

            self.attributes = []

        if self.locator.getLineNumber() % 1000 == 0:
            if self.start:
                eps = self.graph.edges / (time() - self.start)
            else:
                eps = 0

            sys.stderr.write("Line: %d Vertices: %d Edges: %d EPS: %.2f\r" % \
                (self.locator.getLineNumber(), self.graph.vertices, self.graph.edges, eps))
            sys.stderr.flush()

    def characters(self, content):
        if self.get_data:
            self.data += content

def main(filename, graphpath):
    handler = GraphMLHandler(graphpath)

    with open(filename, 'r') as inp:
        xml.sax.parse(inp, handler)

    handler.graph.shutdown()


if __name__ == "__main__":
    #main("/home/nopper/graph.xml", "/tmp")
    #main("/home/nopper/Desktop/Sources/indexer/indexer/bindings/java/gremlin/data/graph-example-1.xml", "/tmp")
    main(sys.argv[1], sys.argv[2])
