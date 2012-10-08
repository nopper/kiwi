import _indexer

class DB(object):
    """
    Simple wrapper for the indexer database
    """
    def __init__(self, basedir):
        self.db = _indexer.db_open(basedir)

    def add(self, key, value):
        return _indexer.db_add(self.db, key, value)

    def get(self, key):
        return _indexer.db_get(self.db, key)

    def remove(self, key):
        return _indexer.db_remove(self.db, key)

    def close(self):
        if self.db is not None:
            _indexer.db_close(self.db)
            self.db = None

    def __del__(self):
        self.close()
