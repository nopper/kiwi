import kiwi.db

d = kiwi.db.DB('/tmp')
d.add('ciao', 'mamma')
d.close()
d = kiwi.db.DB('/tmp')
d.get('ciao')
d.get('ciao')
d.close()
