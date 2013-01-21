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
}

static inline void _lru_cleanup(LRU* self)
{
    CacheEntry *entry, *iterator;

    INFO("Curr: %d Max: %d entries Curr: %d Max: %d size", self->num_entries, self->max_entries, self->curr_size, self->max_size);

    HASH_ITER(hh, self->cache, entry, iterator)
    {
        HASH_DEL(self->cache, entry);

        self->curr_size -= entry->stop - entry->start;
        self->num_entries--;

        free(entry->start);
        free(entry);


        if (self->curr_size < (self->max_size * 0.80) && self->num_entries < self->max_entries)
            break;
    }

    INFO("Curr: %d Max: %d entries Curr: %d Max: %d size", self->num_entries, self->max_entries, self->curr_size, self->max_size);
}

void lru_set(LRU* self, CacheEntry *entry)
{
    //DEBUG("Saving file: %d off: %d len: %d", entry->filenum, entry->offset, entry->stop - entry->start);

    HASH_ADD(hh, self->cache, filenum, KEYLEN, entry);

    self->curr_size += entry->stop - entry->start;
    self->num_entries ++;

    if ((HASH_COUNT(self->cache) >= self->max_entries) ||
        (self->curr_size >= self->max_size))

        _lru_cleanup(self);
}

CacheEntry* lru_get(LRU* self, const LookupKey* key)
{
    CacheEntry* entry = NULL;

    //DEBUG("Requesting file: %d off: %d", key->filenum, key->offset);

    HASH_FIND(hh, self->cache, key, KEYLEN, entry);

    if (entry)
    {
        //DEBUG("Got file: %d off: %d len: %d", entry->filenum, entry->offset, entry->stop - entry->start);
        // Remove it (so the subsequent add will throw it on the front of the list)
        HASH_DELETE(hh, self->cache, entry);
        HASH_ADD(hh, self->cache, filenum, KEYLEN, entry);

        //DEBUG("Got file: %d off: %d len: %d", entry->filenum, entry->offset, entry->stop - entry->start);
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

        self->curr_size -= entry->stop - entry->start;
        self->num_entries--;

        free(entry->start);
        free(entry);
    }
}
