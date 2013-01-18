#!/bin/sh
if [ ! -e kyotocabinet-1.2.30.tar.gz ]; then
    wget http://fallabs.com/kyotocabinet/pkg/kyotocabinet-1.2.30.tar.gz
fi

if [ ! -d deps/kyotocabinet ]; then
    tar xfz kyotocabinet-1.2.30.tar.gz
    mv kyotocabinet-1.2.30 deps/kyotocabinet
fi

cd deps/kyotocabinet
./configure
make

cd ../../

if [ ! -e leveldb-1.9.0.tar.gz ]; then
    wget http://leveldb.googlecode.com/files/leveldb-1.9.0.tar.gz
fi

if [! -d deps/leveldb ]; then
    tar xfz leveldb-1.9.0.tar.gz
    mv leveldb-1.9.0 deps/leveldb
fi

cd deps/leveldb
make
