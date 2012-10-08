#ifndef __DB_H__
#define __DB_H__

#include "indexer.h"
#include "sst.h"
#include "variant.h"
#include "memtable.h"

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

#endif
