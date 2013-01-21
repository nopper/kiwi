#!/bin/sh

ITERATIONS=1000000
DATA=100
CACHE=4194304
CACHE=8388608
CACHE=104857600

echo "======================================================"
echo "Number of keys: $ITERATIONS"
echo "Length of data: $DATA"
echo "Cache size:     $CACHE"
echo "======================================================"


rm -rf testdb
echo "KC PUT"
./kc-benchmark -put 1 -get 0 -del 0 -range $ITERATIONS -duration 1 -data $DATA -cache $CACHE $1
echo "KC GET"
./kc-benchmark -put 0 -get 1 -del 0 -range $ITERATIONS -duration 1 -data $DATA -cache $CACHE $1

rm -rf testdb
echo "LEVELDB PUT"
./leveldb-benchmark -put 1 -get 0 -del 0 -range $ITERATIONS -duration 1 -data $DATA -cache $CACHE $1
echo "LEVELDB GET"
./leveldb-benchmark -put 0 -get 1 -del 0 -range $ITERATIONS -duration 1 -data $DATA -cache $CACHE $1

rm -rf testdb
echo "KIWI PUT"
./kiwi-benchmark -put 1 -get 0 -del 0 -range $ITERATIONS -duration 1 -data $DATA -cache $CACHE $1
echo "KIWI GET"
./kiwi-benchmark -put 0 -get 1 -del 0 -range $ITERATIONS -duration 1 -data $DATA -cache $CACHE $1
