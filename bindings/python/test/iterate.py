import kiwi.db
from msgpack import loads

d = kiwi.db.DB('/tmp')
it = kiwi.db.DBIterator(d)
it.seek('e/')

for k, v in it:
    print "%s => %s" % (k, loads(v)), repr(v)

d.close()
