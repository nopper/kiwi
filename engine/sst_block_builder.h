#ifndef __SST_BLOCK_BUILDER_H__
#define __SST_BLOCK_BUILDER_H__

#include "lib/kvec.h"
#include "buffer.h"
#include "variant.h"

/*

[data block]
[data block]
[Footer] ->
  8 bytes: position of index
  8 bytes: size of index
  8 bytes: position of metaindex
  8 bytes: size of metaindex
  8 bytes: magic number

Index:
  [key length][key value][8 bytes position][8 bytes size]

We don't need bloom filters for the moment.
*/

#define FLAG_NOCOMPRESS 0
#define FLAG_COMPRESS   1
#define FLAG_INDEX      2
#define FLAG_FINISHED   4

// This object is responsible for building, reading an elementary block
// which can be optionally compressed if specified

/* This creates a data block, ready to be compressed where each key is
   stored using the following scheme:
 */

typedef struct _sst_block_builder {
    unsigned int flags;
    unsigned int restart_interval;
    unsigned int counter;
    unsigned int entries;

    Variant* last_key;
    kvec_t(size_t) restarts;

    Buffer* buffer;
} SSTBlockBuilder;

SSTBlockBuilder* sst_block_builder_new(uint32_t flags, uint32_t restart_interval);
void sst_block_builder_free(SSTBlockBuilder* self);

void sst_block_builder_add(SSTBlockBuilder* self, Variant* key, Variant* value, OPT opt);
size_t sst_block_builder_current_size(SSTBlockBuilder* self);
void sst_block_builder_flush(SSTBlockBuilder* self);
void sst_block_builder_reset(SSTBlockBuilder* self);

#endif
