CC = gcc
CFLAGS = -std=c99 $(DEBUG) -I.. -g -ggdb -O0
LDFLAGS = -L/usr/lib -lcheck

INDEXER = indexer

skiplist:
	$(CC) $(CFLAGS) ../skiplist.c ../indexer.c ../arena.c skiplist_test.c -o skiplist_test

memtable:
	$(CC) $(CFLAGS) ../memtable.c ../skiplist.c ../indexer.c ../arena.c ../utils.c ../buffer.c memtable_test.c $(LDFLAGS) -o memtable_test