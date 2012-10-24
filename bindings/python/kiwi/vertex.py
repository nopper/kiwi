from element import KiwiElement

class KiwiVertex(KiwiElement):
    def __init__(self, graph, id=None):
        super(KiwiVertex, self).__init__(graph, id)

    @staticmethod
    def load_outE(dct):
        KiwiVertex._load_dict(dct)

    @staticmethod
    def load_outV(dct):
        KiwiVertex._load_dict(dct)

    @staticmethod
    def load_inE(dct):
        KiwiVertex._load_dict(dct)

    @staticmethod
    def load_inV(dct):
        KiwiVertex._load_dict(dct)

    @staticmethod
    def save_outE(dct):
        KiwiVertex._save_dict(dct)

    @staticmethod
    def save_outV(dct):
        KiwiVertex._save_dict(dct)

    @staticmethod
    def save_inE(dct):
        KiwiVertex._save_dict(dct)

    @staticmethod
    def save_inV(dct):
        KiwiVertex._save_dict(dct)

    @staticmethod
    def _save_dict(dct):
        for key in dct:
            lst = dct[key]

            if not lst:
                continue

            lst.sort()
            current = lst[0]

            for pos, elem in enumerate(lst[1:]):
                lst[pos + 1] = elem - current
                current = elem

            dct[key] = lst

    @staticmethod
    def _load_dict(dct):
        for key in dct:
            lst = dct[key]

            if not lst:
                continue

            add = 0
            for pos, elem in enumerate(lst):
                curr = lst[pos]
                lst[pos] += add
                add = curr

            dct[key] = lst
