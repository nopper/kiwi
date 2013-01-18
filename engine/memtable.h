#ifndef __MEMTABLE_H__
#define __MEMTABLE_H__

#include "skiplist.h"
#include "variant.h"
#include "log.h"

typedef struct _memtable {
    SkipList* list;

    int lsn;
    Log* log;

    uint32_t needs_compaction;
    uint32_t del_count;
    uint32_t add_count;
} MemTable;

MemTable* memtable_new(Log* log);
void memtable_reset(MemTable* self);
void memtable_free(MemTable* self);

int memtable_add(MemTable* self, const Variant *key, const Variant *value);
int memtable_remove(MemTable* self, const Variant* key);
int memtable_get(SkipList* list, const Variant *key, Variant* value);


// Utility function
int memtable_needs_compaction(MemTable* self);
void memtable_extract_node(SkipNode* node, Variant* key, Variant* value, OPT* opt);

#endif
