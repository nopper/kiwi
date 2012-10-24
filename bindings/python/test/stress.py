import kiwi.db
import random
count = 0
d = kiwi.db.DB('/tmp')
for i in xrange(1000000):
    d.add(str(i), "A" * 80)
a = range(1000000)
random.shuffle(a)
for i in a:
    assert d.get(str(i)) == "A" * 80
    count += 1
print "Count", count
d.close()
