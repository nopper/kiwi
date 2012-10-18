#ifndef __DB_H__
#define __DB_H__

#include "indexer.h"
#include "sst.h"
#include "variant.h"
#include "memtable.h"
#include "merger.h"

typedef struct _db {
    char basedir[MAX_FILENAME];
    SST* sst;
    MemTable* memtable;
} DB;

DB* db_open(const char *basedir);
void db_close(DB* self);
int db_add(DB* self, Variant* key, Variant* value);
int db_get(DB* self, Variant* key, Variant* value);
int db_remove(DB* self, Variant* key);

typedef struct _db_iterator {
    DB* db;
    unsigned valid:1;

    unsigned use_memtable:1;
    unsigned use_files:1;

    Heap* minheap;
    Vector* iterators;

    SkipNode* node;
    SkipNode* prev;

    Variant* sl_key;
    Variant* sl_value;

    ChainedIterator* current;
} DBIterator;

DBIterator* db_iterator_new(DB* self);
void db_iterator_free(DBIterator* self);

void db_iterator_seek(DBIterator* self, Variant* key);
void db_iterator_next(DBIterator* self);
int db_iterator_valid(DBIterator* self);

Variant* db_iterator_key(DBIterator* self);
Variant* db_iterator_value(DBIterator* self);

#endif
