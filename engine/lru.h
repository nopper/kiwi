#ifndef __LRU_H__
#define __LRU_H__

#include <pthread.h>
#include <sys/types.h>
#include <inttypes.h>
#include "config.h"
#include "uthash.h"

#define KEYLEN (sizeof(LookupKey))

typedef struct _lookup_key {
    uint64_t filenum; // Key
    uint64_t offset;  // Key
} LookupKey;

typedef struct _cache_entry {
    LookupKey key;

    void *start; // Value
    void *stop;  // Value

    UT_hash_handle hh;
} CacheEntry;

typedef struct _lru {
    CacheEntry* cache;
    uint32_t max_size;
    uint32_t curr_size;

    uint32_t max_entries;
    uint32_t num_entries;
} LRU;

LRU* lru_new(uint64_t size);
void lru_free(LRU* self);

void lru_set(LRU* self, CacheEntry* entry);
CacheEntry* lru_get(LRU* self, const LookupKey* key);
void lru_release(LRU* self, const LookupKey* key);

#endif
