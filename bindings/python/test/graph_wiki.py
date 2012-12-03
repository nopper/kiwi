import sys
from kiwi.graph import KiwiGraph

g = KiwiGraph(sys.argv[1])

out = [1818
, 2017
, 3194
, 34003
, 38204
, 38561
, 38703
, 38833
, 39976
, 40272
, 93697
, 99514
, 103275
, 219754
, 244546
, 261959
, 370727
, 445127
, 516406
, 612928
, 616777
, 649739
, 765011
, 1653500
, 1965586
, 2134816
, 2353744
, 2989486
, 3036279
, 3125786]


vertices = []
d = {}

for outV in g.v(int(sys.argv[2])).outV():
    vertices.append(outV.getId())
    d[outV.getId()] = [x.getId() for x in outV.outV()]


assert vertices == out

print vertices, len(vertices)
print out, len(out)
print d
