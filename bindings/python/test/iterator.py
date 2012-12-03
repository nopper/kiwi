import indexer
d = indexer.DB('/tmp')
d.add('V0', 'vertice 0')
d.add('V1', 'vertice 1')
d.add('E2', 'edge numero 2')

it = indexer.DBIterator(d)
it.seek('E')

for i in it:
    print i
