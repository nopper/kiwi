#!/bin/sh

ITERATIONS=1000000
DATA=100
CACHE=4194304
CACHE=8388608
CACHE=104857600

function benchmark {

  ITERATIONS=$1
  DATA=$2
  CACHE=$3
  ARGS=$4
  TITLE=$5

  echo "======================================================"
  echo "$TITLE"
  echo "Number of keys: $ITERATIONS"
  echo "Length of data: $DATA"
  echo "Cache size:     $CACHE"
  echo "======================================================"


  rm -rf testdb
  echo "KC PUT"
  ./kc-benchmark -put 1 -get 0 -del 0 -range $ITERATIONS -duration 1 -data $DATA -cache $CACHE $ARGS
  echo "KC GET"
  ./kc-benchmark -put 0 -get 1 -del 0 -range $ITERATIONS -duration 1 -data $DATA -cache $CACHE $ARGS

  rm -rf testdb
  echo "LEVELDB PUT"
  ./leveldb-benchmark -put 1 -get 0 -del 0 -range $ITERATIONS -duration 1 -data $DATA -cache $CACHE $ARGS
  echo "LEVELDB GET"
  ./leveldb-benchmark -put 0 -get 1 -del 0 -range $ITERATIONS -duration 1 -data $DATA -cache $CACHE $ARGS

  rm -rf testdb
  echo "KIWI PUT"
  ./kiwi-benchmark -put 1 -get 0 -del 0 -range $ITERATIONS -duration 1 -data $DATA -cache $CACHE $ARGS
  echo "KIWI GET"
  ./kiwi-benchmark -put 0 -get 1 -del 0 -range $ITERATIONS -duration 1 -data $DATA -cache $CACHE $ARGS
}

benchmark 1000000 100 4194304 "" "SMALL VALUES 4MB"
benchmark 1000000 100 8388608 "" "SMALL VALUES 8MB"
benchmark 1000000 100 104857600 "" "SMALL VALUES 100MB"

benchmark 1000000 100 4194304 "-sequential" "SMALL VALUES SEQUENTIAL 4MB"
benchmark 1000000 100 8388608 "-sequential" "SMALL VALUES SEQUENTIAL 8MB"
benchmark 1000000 100 104857600 "-sequential" "SMALL VALUES SEQUENTIAL 100MB"

benchmark 1000 10000 4194304 "" "BIG VALUES 4MB"
benchmark 1000 10000 8388608 "" "BIG VALUES 8MB"
benchmark 1000 10000 104857600 "" "BIG VALUES 100MB"

benchmark 1000 10000 4194304 "-sequential" "BIG VALUES SEQUENTIAL 4MB"
benchmark 1000 10000 8388608 "-sequential" "BIG VALUES SEQUENTIAL 8MB"
benchmark 1000 10000 104857600 "-sequential" "BIG VALUES SEQUENTIAL 100MB"
