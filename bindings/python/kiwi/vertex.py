import itertools
import kiwi.edge
import kiwi.element

class KiwiVertex(kiwi.element.KiwiElement):
    _directory = 'V/'

    def __init__(self, graph, id=None):
        super(KiwiVertex, self).__init__(graph, id)

    def save(self):
        self.db.add(self.path_for(None), '')

    def getEdges(self, direction, labels=[]):
        return self._getEntriesFor(direction, labels)

    def getVertices(self, direction, labels=[]):
        if direction == 'BOTH':
            it_in = itertools.imap(lambda e: e.getOther(self), self._getEntriesFor('IN', labels))
            it_out = itertools.imap(lambda e: e.getOther(self), self._getEntriesFor('OUT', labels))
            return itertools.chain(it_in, it_out)

        return itertools.imap(lambda e: e.getOther(self), self._getEntriesFor(direction, labels))

    def _getEntriesFor(self, direction, labels):
        if direction == 'IN':
            it = iter(self.getProperty('inE', []))
        if direction == 'OUT':
            it = iter(self.getProperty('outE', []))
        if direction == 'BOTH':
            it_in = iter(self.getProperty('inE', []))
            it_out = iter(self.getProperty('outE', []))

            it = itertools.chain(it_in, it_out)

        try:
            if not labels:
                return itertools.imap(lambda id: kiwi.edge.KiwiEdge(self.graph, id), it)

            return itertools.ifilter(
                lambda e: e.getLabel() in labels,
                itertools.imap(lambda id: kiwi.edge.KiwiEdge(self.graph, id), it)
            )
        except:
            raise Exception("Unknown direction")

    @staticmethod
    def load_outE(lst):
        return KiwiVertex._load_list(lst)

    @staticmethod
    def load_inE(lst):
        return KiwiVertex._load_list(lst)

    @staticmethod
    def save_outE(lst):
        return KiwiVertex._save_list(lst)

    @staticmethod
    def save_inE(lst):
        return KiwiVertex._save_list(lst)

    @staticmethod
    def _save_list(lst):
        if not lst:
            return lst

        nlst = []
        curr = 0

        for elem in lst:
            nlst.append(elem - curr)
            curr = elem

        return nlst

    @staticmethod
    def _load_list(lst):
        if not lst:
            return lst

        add = 0
        for pos, elem in enumerate(lst):
            lst[pos] += add
            add = lst[pos]

        return lst

    def bothV(self, labels=[]):
        return self.getVertices('BOTH', labels)

    def outV(self, labels=[]):
        return self.getVertices('OUT', labels)

    def inV(self, labels=[]):
        return self.getVertices('IN', labels)

    def bothE(self, labels=[]):
        return self.getEdges('BOTH', labels)

    def outE(self, labels=[]):
        return self.getEdges('OUT', labels)

    def inE(self, labels=[]):
        return self.getEdges('IN', labels)
