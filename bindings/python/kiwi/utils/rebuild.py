import sys
from kiwi.db import DB, DBIterator

src = DB(sys.argv[1])
dst = DB(sys.argv[2])

iterator = DBIterator(src)
iterator.seek('\x00')

for k, v in iterator:
    dst.add(k, v)

dst.close()
src.close()
