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
DB* db_open_ex(const char *basedir, uint64_t cache_size);

void db_close(DB* self);
int db_add(DB* self, Variant* key, Variant* value);
int db_get(DB* self, Variant* key, Variant* value);
int db_remove(DB* self, Variant* key);

typedef struct _db_iterator {
    DB* db;
    unsigned valid:1;

    unsigned use_memtable:1;
    unsigned use_files:1;
    unsigned has_imm:1;

    Heap* minheap;
    Vector* iterators;

    SkipNode* node;
    SkipNode* imm_node;
    SkipNode* prev;
    SkipNode* imm_prev;

    SkipList* list;
    SkipList* imm_list;

    unsigned list_end:1;
    unsigned imm_list_end:1;
    unsigned advance;

#define ADV_MEM 1
#define ADV_IMM 2

    Variant* sl_key;
    Variant* sl_value;

    Variant* isl_key;
    Variant* isl_value;

    Variant* key;
    Variant* value;

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
