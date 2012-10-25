#ifndef __SST_LOADER_H__
#define __SST_LOADER_H__

// This file is responsible for loading and querying a single sst file

#include <sys/types.h>
#include "lib/kvec.h"
#include "file.h"
#include "variant.h"
#include "lru.h"

typedef struct _index_entry {
    size_t klen; // Length of the index key
    char *key;   // Actual pointer to the index key

    uint64_t offset; // Position of the block in the sst file
    uint64_t size;   // Size of the block
} IndexEntry;

typedef struct _sst_loader {
    LRU* cache;
    uint32_t level;
    uint32_t filenum;

    uint64_t bloom_off, bloom_size;
    uint64_t data_size, filter_size, index_size, key_size, num_blocks, num_entries, value_size;

    File* file;
    kvec_t(IndexEntry*) index;
} SSTLoader;

SSTLoader* sst_loader_new(LRU *cache, File* file, uint32_t level, uint32_t filenum);
void sst_loader_free(SSTLoader* self);
int sst_loader_get(SSTLoader* self, Variant* key, Variant* value, OPT *opt);

typedef struct _sst_loader_iterator {
    int prev_block;
    int block; // This is an integer indexing the index of SSTLoader
    unsigned valid:1;

    char *current;
    char *start, *stop;
    SSTLoader* loader;

    OPT opt;
    Variant* key;
    Variant* value;
} SSTLoaderIterator;

SSTLoaderIterator* sst_loader_iterator(SSTLoader* self);
SSTLoaderIterator* sst_loader_iterator_seek(SSTLoader* self, Variant* key);
void sst_loader_iterator_free(SSTLoaderIterator* iter);
void sst_loader_iterator_next(SSTLoaderIterator* iter);
int sst_loader_iterator_valid(SSTLoaderIterator* iter);
int sst_loader_iterator_compare(SSTLoaderIterator* a, SSTLoaderIterator* b);

#endif
