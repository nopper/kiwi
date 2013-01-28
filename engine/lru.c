#include <stdlib.h>
#include <stddef.h>
#include <string.h>
#include "lru.h"
#include "indexer.h"

LRU* lru_new(uint64_t size)
{
    LRU* self = calloc(1, sizeof(LRU));

    self->max_size = size;
    self->curr_size = 0;
    self->num_entries = 0;
    self->max_entries = 100000;
    self->cache = NULL;

    return self;
}

void lru_free(LRU* self)
{
    CacheEntry *entry, *iterator;

    HASH_ITER(hh, self->cache, entry, iterator)
    {
        HASH_DEL(self->cache, entry);
        free(entry->start);
        free(entry);
    }

    free(self);
}

static inline void _lru_cleanup(LRU* self)
{
    CacheEntry *entry, *iterator;

    //INFO("Curr: %d Max: %d entries Curr: %d Max: %d size", self->num_self->cache, self->max_self->cache, self->curr_size, self->max_size);

    HASH_ITER(hh, self->cache, entry, iterator)
    {
        HASH_DEL(self->cache, entry);

        self->curr_size -= (char*)entry->stop - (char*)entry->start;
        self->num_entries--;

        free(entry->start);
        free(entry);


        //if (self->curr_size < (self->max_size * 0.95) && self->num_entries < self->max_entries)
            break;
    }

    //INFO("Curr: %d Max: %d entries Curr: %d Max: %d size", self->num_self->cache, self->max_self->cache, self->curr_size, self->max_size);
}

void lru_set(LRU* self, CacheEntry *entry)
{
    //INFO("Saving file: %"PRIu64" off: %"PRIu64" len: %u", entry->key.filenum, entry->key.offset, entry->stop - entry->start);

    HASH_ADD(hh, self->cache, key, KEYLEN, entry);

    self->curr_size += (char*)entry->stop - (char*)entry->start;
    self->num_entries ++;

    if ((HASH_COUNT(self->cache) >= self->max_entries) ||
        (self->curr_size >= self->max_size))

        _lru_cleanup(self);
}

CacheEntry* lru_get(LRU* self, const LookupKey* key)
{
    CacheEntry* entry = NULL;

    //INFO("Requesting file: %"PRIu64" off: %"PRIu64, key->filenum, key->offset);

    HASH_FIND(hh, self->cache, key, sizeof(LookupKey), entry);

    if (entry)
    {
        //DEBUG("Got file: %d off: %d len: %d", entry->filenum, entry->offset, entry->stop - entry->start);
        // Remove it (so the subsequent add will throw it on the front of the list)
        HASH_DELETE(hh, self->cache, entry);
        HASH_ADD(hh, self->cache, key, KEYLEN, entry);

        //INFO("Got file: %"PRIu64" off: %"PRIu64" len: %u", entry->key.filenum, entry->key.offset, entry->stop - entry->start);
    }

    return entry;
}

void lru_release(LRU* self, const LookupKey* key)
{
    CacheEntry* entry = NULL;

    HASH_FIND(hh, self->cache, key, KEYLEN, entry);

    if (entry)
    {
        HASH_DELETE(hh, self->cache, entry);

        self->curr_size -= (char*)entry->stop - (char*)entry->start;
        self->num_entries--;

        free(entry->start);
        free(entry);
    }
}
