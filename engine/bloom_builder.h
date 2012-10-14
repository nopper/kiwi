#ifndef __BLOOM_BUILDER_H__
#define __BLOOM_BUILDER_H__

#include <sys/types.h>
#include "buffer.h"
#include "lib/kvec.h"
#include "sst_block_builder.h"

typedef struct _bloom_builder {
    Buffer* buff;
    kvec_t(uint32_t) offsets;
    size_t bits_per_key;
    uint32_t current;
} BloomBuilder;

BloomBuilder* bloom_builder_new(size_t bits_per_key);
void bloom_builder_free(BloomBuilder* self);

void bloom_builder_generate(BloomBuilder* self, uint64_t start_off, SSTBlockBuilder* data);
void bloom_builder_finish(BloomBuilder* self);

#endif
