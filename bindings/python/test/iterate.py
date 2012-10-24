import kiwi.db

d = kiwi.db.DB('/tmp')
print d.get('V6')
print d.get('E5')
it = kiwi.db.DBIterator(d)
it.seek('\x00')

for k, v in it:
    print k, v

d.close()
