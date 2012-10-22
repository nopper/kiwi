#ifndef __SST_BUILDER_H__
#define __SST_BUILDER_H__

#include "indexer.h"
#include "file.h"
#include "sst_block_builder.h"
#include "lib/kvec.h"
#ifdef WITH_BLOOM_FILTER
#include "bloom_builder.h"
#endif

typedef struct _sst_builder {
    File* file;
    size_t offset;

    unsigned pending_index:1;
    unsigned needs_reset:1;
    unsigned block_written:1;

    uint64_t metadata_num_entries; // number of kv
    uint64_t metadata_num_blocks;  // number of data blocks
    uint64_t metadata_index_size;  // size in bytes of the Index block
    uint64_t metadata_data_size;   // size in bytes of all data blocks
    uint64_t metadata_key_size;    // size in bytes of all keys (uncompressed)
    uint64_t metadata_value_size;  // size in bytes of all values (uncompressed)
#ifdef WITH_BLOOM_FILTER
    uint64_t metadata_filter_size; // size in bytes of the filters
    BloomBuilder* bloom;
#endif

    Variant* last_key;
    Variant* last_block_offset;

    SSTBlockBuilder* data_block;
    SSTBlockBuilder* index_block;
} SSTBuilder;

SSTBuilder* sst_builder_new(File* output);
void sst_builder_free(SSTBuilder* self);
void sst_builder_add(SSTBuilder* self, Variant* key, Variant* value, OPT opt);

#endif
