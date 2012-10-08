#ifndef __SST_BUILDER_H__
#define __SST_BUILDER_H__

#include "indexer.h"
#include "file.h"
#include "sst_block_builder.h"
#include "lib/kvec.h"

typedef struct _sst_builder {
    File* file;
    size_t offset;

    unsigned pending_index:1;
    unsigned needs_reset:1;
    unsigned block_written:1;

    Variant* last_key;
    Variant* last_block_offset;

    SSTBlockBuilder* data_block;
    SSTBlockBuilder* index_block;
} SSTBuilder;

SSTBuilder* sst_builder_new(File* output);
void sst_builder_free(SSTBuilder* self);
void sst_builder_add(SSTBuilder* self, Variant* key, Variant* value);

#endif
