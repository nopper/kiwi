from kiwi.db import DBIterator
from msgpack import dumps, loads

class KiwiElement(object):
    def __init__(self, graph, id=None):
        self.db = graph.db
        self.graph = graph
        self.id = long(id)
        self.str_id = str(id)
        self.props = {}
        self.needs_save = False

    def getId(self):
        return self.id

    def path_for(self, key):
        if key is None:
            return self._directory.lower() + self.str_id
        else:
            return self._directory + self.str_id + '/' + key

    def key_from(self, path):
        return path.split('/')[-1]

    def setProperty(self, key, value):
        self.props[key] = value
        self.needs_save = True

    def __del__(self):
        if self.needs_save:
            self.save_properties()

    def save_properties(self):
        self.needs_save = False
        for key, value in self.props.iteritems():
            value = getattr(self, 'save_' + key, lambda x: x)(value)
            self.db.add(self.path_for(key), dumps(value))

    def getProperty(self, key, default=None):
        try:
            return self.props[key]
        except:
            ret = self.db.get(self.path_for(key))

            if ret is not None:
                try:
                    value = getattr(self, 'load_' + key, lambda x: x)(loads(ret, use_list=True))

                    if value is None:
                        return default

                    self.props[key] = value
                    return value
                except:
                    pass

            return default

    def getPropertyKeys(self):
        prefix = self.path_for('')
        iter = DBIterator(self.db)
        iter.seek(prefix)

        for key, value in iter:
            if not key.startswith(prefix):
                break

            key = self.key_from(key)
            self.props[key] = getattr(self, 'load_' + key, lambda x: x)(loads(value, use_list=True))

        del iter
        return self.props.keys()

    def removeProperty(self, key):
        if key in self.props:
            del self.props[key]

        self.db.remove(self.path_for(key))

    def __str__(self):
        return "%s:%s" % (self.__class__.__name__, self.id)

    def __repr__(self):
        return str(self)
