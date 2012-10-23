#include <assert.h>
#include <inttypes.h>
#include "bloom_builder.h"
#include "hash.h"
#include "utils.h"
#include "indexer.h"

static void _update(BloomBuilder* self, SSTBlockBuilder* block)
{
    // Let's create our dynamic length bloom filter. According to wikipedia
    // 9.6 bits per key are a reasonable amount of bits
    size_t bits = block->entries * self->bits_per_key;

    if (bits < 64) bits = 64;

    size_t bytes = (bits + 7) / 8;
    bits = bytes * 8;

    Buffer* key = buffer_new(1);
    buffer_extend_by(self->buff, bytes);
//    DEBUG("%d bytes for %d entries allocated for the bloom filter", bytes, block->entries);

    char* array = self->buff->mem + self->buff->length;
    self->buff->length += bytes;

    char* start = block->buffer->mem;

    for (size_t i = 0; i < block->entries; i++)
    {
        uint32_t plen, klen, vlen;
        start = (char *)get_varint32(start, start + 5, &plen);
        start = (char *)get_varint32(start, start + 5, &klen);
        start = (char *)get_varint32(start, start + 5, &vlen);

        key->length = plen;
        buffer_putnstr(key, start, klen);
        start += klen;

        if (vlen > 1)
            start += vlen - 1;

        uint32_t h = hash(key->mem, key->length, 0xbc9f1d34);
        const uint32_t delta = (h >> 17) | (h << 15);  // Rotate right 17 bits

//        DEBUG("Hash of %.*s = %d", key->length, key->mem, h);

        for (size_t j = 0; j < NUM_PROBES; j++)
        {
            const uint32_t bitpos = h % bits;
            array[bitpos / 8] |= (1 << (bitpos % 8));
            h += delta;
        }
    }

    buffer_free(key);
}

BloomBuilder* bloom_builder_new(size_t bits_per_key)
{
    BloomBuilder* self = malloc(sizeof(BloomBuilder));

    kv_init(self->offsets);
    self->buff = buffer_new(4095);

    self->bits_per_key = bits_per_key;
    self->current = 0;

    return self;
}

void bloom_builder_free(BloomBuilder* self)
{
    buffer_free(self->buff);
    kv_destroy(self->offsets);
    free(self);
}

void bloom_builder_generate(BloomBuilder* self, uint64_t start_off, SSTBlockBuilder* data)
{
//    uint32_t index = kv_size(self->offsets);
//    DEBUG("Creating filter block n. %d [%" PRIu64 "-%" PRIu64, index, start_off, start_off + data->buffer->length);

    uint32_t start = self->buff->length;
    kv_push(uint32_t, self->offsets, start);
    _update(self, data);
//    DEBUG("Filter block %d is %d bytes", index, self->buff->length - start);
}

void bloom_builder_finish(BloomBuilder* self)
{
    for (int i = 0; i < kv_size(self->offsets); i++)
        buffer_putint32(self->buff, kv_A(self->offsets, i));
    buffer_putint32(self->buff, kv_size(self->offsets));
}
