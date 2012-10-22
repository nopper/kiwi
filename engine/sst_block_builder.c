#include <assert.h>
#include "sst_block_builder.h"
#include "indexer.h"
#include "lib/kvec.h"

SSTBlockBuilder* sst_block_builder_new(uint32_t flags, uint32_t restart_interval)
{
    SSTBlockBuilder* self = malloc(sizeof(SSTBlockBuilder));

    if (!self)
        PANIC("NULL allocation");

    self->flags = flags;
    self->restart_interval = restart_interval;
    self->counter = 0;
    self->entries = 0;

    // Allocate enough space to avoid reallocations
    self->last_key = buffer_new(1024);
    self->buffer = buffer_new(4096);

    kv_init(self->restarts);

    return self;
}

void sst_block_builder_free(SSTBlockBuilder* self)
{
    buffer_free(self->last_key);
    buffer_free(self->buffer);
    kv_destroy(self->restarts);
    free(self);
}

void sst_block_builder_add(SSTBlockBuilder* self, Variant* key, Variant* value, OPT opt)
{
    size_t shared = 0;

    if (self->counter < self->restart_interval)
    {
        const size_t min_length = MIN(self->last_key->length, key->length);
        while ((shared < min_length) && (self->last_key->mem[shared] == key->mem[shared]))
            shared++;
    }
    else
    {
        // Restart compression
        self->counter = 0;
    }

    // Also take in consideration keys that have nothing in common with the previous
    if (shared == 0 && (self->flags & FLAG_INDEX) == 0)
        // Index block does not need to know where is the restart point
        kv_push(size_t, self->restarts, self->buffer->length);

    const size_t non_shared = key->length - shared;

//    DEBUG("Key: %.*s Value: %.*s shared: %d non-shared: %d",
//          key->length, key->mem, value->length, value->mem, shared, non_shared);

    if (opt == DEL)
        assert(value->length == 0);

    // Add preamble <shared><non-shared><value-length>
    buffer_putvarint32(self->buffer, shared);
    buffer_putvarint32(self->buffer, non_shared);
    buffer_putvarint32(self->buffer, value->length + ((opt == ADD) ? 1 : 0));

    // Add actual value <non-shared-key><value>
    buffer_putnstr(self->buffer, key->mem + shared, non_shared);
    buffer_putnstr(self->buffer, value->mem, value->length);

    // Just copy the new unshared part to update the last_key
    self->last_key->length = shared;
    buffer_putnstr(self->last_key, key->mem + shared, non_shared);

    self->counter++;
    self->entries++;
}

size_t sst_block_builder_current_size(SSTBlockBuilder* self)
{
    return self->buffer->length +                       // data
            kv_size(self->restarts) * sizeof(uint32_t) + // restart array
            sizeof(uint32_t);                            // restart array length
}

void sst_block_builder_flush(SSTBlockBuilder* self)
{
    if ((self->flags & FLAG_INDEX) == 0)
    {
        for (size_t i = 0; i < kv_size(self->restarts); i++)
        {
            // Here we are actually downcasting but it should be safe
            //DEBUG("Restart position: %u", (uint32_t)kv_A(self->restarts, i));
            buffer_putint32(self->buffer, (uint32_t)kv_A(self->restarts, i));
        }

        buffer_putint32(self->buffer, (uint32_t)kv_size(self->restarts));
    }

    self->flags |= FLAG_FINISHED;
}

void sst_block_builder_reset(SSTBlockBuilder* self)
{
    buffer_clear(self->buffer);
    buffer_clear(self->last_key);
    kv_reset(self->restarts);

    self->counter = 0;
    self->entries = 0;
}
