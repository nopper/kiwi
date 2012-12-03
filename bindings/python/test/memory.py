from time import sleep
from kiwi.db import DB

MAX = 1000000
d = DB('/tmp')

for i in xrange(MAX):
    d.add(str(i), "A" * 100)

print "Inserted..."
sleep(10)
print "Start getting"
for i in xrange(MAX):
    d.get(str(i))

print "Get completed"
sleep(10)

d.close()


