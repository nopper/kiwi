#!/bin/sh

ITERATIONS=1000000
DATA=10000

rm -rf testdb
echo "KC PUT"
./kc-benchmark -put 1 -get 0 -del 0 -range $ITERATIONS -duration 1 -data $DATA $1
echo "KC GET"
./kc-benchmark -put 0 -get 1 -del 0 -range $ITERATIONS -duration 1 -data $DATA $1

rm -rf testdb
echo "LEVELDB PUT"
./leveldb-benchmark -put 1 -get 0 -del 0 -range $ITERATIONS -duration 1 -data $DATA $1
echo "LEVELDB GET"
./leveldb-benchmark -put 0 -get 1 -del 0 -range $ITERATIONS -duration 1 -data $DATA $1

rm -rf testdb
echo "KIWI PUT"
./kiwi-benchmark -put 1 -get 0 -del 0 -range $ITERATIONS -duration 1 -data $DATA $1
echo "KIWI GET"
./kiwi-benchmark -put 0 -get 1 -del 0 -range $ITERATIONS -duration 1 -data $DATA $1
