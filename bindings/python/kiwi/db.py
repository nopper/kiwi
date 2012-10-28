import _kiwidb

class DB(object):
    """
    Simple wrapper for the indexer database
    """
    def __init__(self, basedir):
        self.db = _kiwidb.db_open(basedir)

    def add(self, key, value):
        return _kiwidb.db_add(self.db, key, value)

    def get(self, key):
        return _kiwidb.db_get(self.db, key)

    def remove(self, key):
        return _kiwidb.db_remove(self.db, key)

    def close(self):
        if self.db is not None:
            _kiwidb.db_close(self.db)
            self.db = None

    def __del__(self):
        self.close()

    def __iter__(self):
        return DBIterator(self)

class DBIterator(object):
    def __init__(self, db):
        self.db = db
        self._iter = _kiwidb.db_iterator_new(db.db)

    def seek(self, key):
        _kiwidb.db_iterator_seek(self._iter, key)

    def __iter__(self):
        return self

    def __del__(self):
        _kiwidb.db_iterator_free(self._iter)

    def next(self):
        if not _kiwidb.db_iterator_valid(self._iter):
            raise StopIteration

        k, v = _kiwidb.db_iterator_key(self._iter), \
               _kiwidb.db_iterator_value(self._iter)

        _kiwidb.db_iterator_next(self._iter)

        return k, v

if __name__ == '__main__':
    d = DB('/tmp')
    for i in range(100):
        d.add(str(i), str(i))
    for k in d:
        print k
    d.close()
