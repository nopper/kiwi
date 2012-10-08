import sys
import indexer

d = indexer.DB('/tmp/si')
for i in range(1000000):
    if i % 1000 == 0:
        sys.stderr.write("Item %d\n" % i)
        sys.stderr.flush()
    d.add(str(i), str(i))
d.close()
