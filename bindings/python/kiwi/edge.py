from msgpack import dumps, loads
import kiwi.vertex
import kiwi.element

class KiwiEdge(kiwi.element.KiwiElement):
    _directory = 'E/'

    def __init__(self, graph, id=None):
        super(KiwiEdge, self).__init__(graph, id)

        self._label = None
        self._inV = None
        self._outV = None

    def reset(self, inV, outV, label):
        self._label = label
        self._inV = inV
        self._outV = outV

    def save(self):
        self.db.add(self.path_for(None), dumps([self._inV.getId(), self._outV.getId(), self._label]))

    def load(self):
        in_id, out_id, self._label = loads(self.db.get(self.path_for(None)))
        self._inV = kiwi.vertex.KiwiVertex(self.graph, in_id)
        self._outV = kiwi.vertex.KiwiVertex(self.graph, out_id)

    def getVertex(self, direction):
        if self._outV is None or self._inV is None:
            self.load()

        if direction == 'OUT':
            return self._outV
        if direction == 'IN':
            return self._inV
        if direction == 'BOTH':
            return (self._outV, self._inV)

    def getOther(self, current):
        if self._outV is None or self._inV is None:
            self.load()

        if current.getId() != self._outV.getId():
            return self._outV
        elif current.getId() != self._inV.getId():
            return self._inV

        return self._inV

        raise Exception("Error in edge %d: inV and outV are the same %d %d" % \
            (self.getId(), self._outV.getId(), self._inV.getId()))

    def getLabel(self):
        if not self._label:
            self.load()

        return self._label

    def outV(self):
        return self.getVertex('OUT')

    def inV(self):
        return self.getVertex('IN')

    def bothV(self):
        return self.getVertex('BOTH')
