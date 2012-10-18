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

    def __iter__(self):
        return DBIterator(self)

class DBIterator(object):
    def __init__(self, db):
        self.db = db
        self._iter = _indexer.db_iterator_new(db.db)

    def seek(self, key):
        _indexer.db_iterator_seek(self._iter, key)

    def __iter__(self):
        return self

    def next(self):
        if not _indexer.db_iterator_valid(self._iter):
            raise StopIteration

        k, v = _indexer.db_iterator_key(self._iter), \
               _indexer.db_iterator_value(self._iter)

        _indexer.db_iterator_next(self._iter)

        return k, v

if __name__ == '__main__':
    d = DB('/tmp')
    for i in range(100):
        d.add(str(i), str(i))
    for k in d:
        print k
    d.close()
