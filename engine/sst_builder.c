#include "sst_builder.h"
#include "crc32.h"
#ifdef WITH_SNAPPY
#include <snappy-c.h>
#endif

static void shortest_separator(Variant *last_key, Variant *new_key)
{
    size_t diff_index = 0;
    size_t min_length = MIN(last_key->length, new_key->length);

//    DEBUG("Shortest separator between: %.*s %.*s",
//          last_key->length, last_key->mem,
//          new_key->length, new_key->mem);

    while ((diff_index < min_length) &&
           (last_key->mem[diff_index] == new_key->mem[diff_index]))
        diff_index++;

    if (diff_index >= min_length) {

    } else {
        while (diff_index < min_length)
        {
            if (last_key->mem[diff_index] < (char)0xff &&
                last_key->mem[diff_index] + 1 < new_key->mem[diff_index])
            {
                last_key->mem[diff_index]++;
                last_key->length = diff_index + 1;
                break;
            }
            diff_index++;
        }
    }

//    DEBUG("Shortest separator result: %.*s", last_key->length, last_key->mem);
}

static void _write_block(SSTBuilder* self, SSTBlockBuilder* block, int skip_comp)
{
    // Write the contents of the block to the file
    sst_block_builder_flush(block);

    uint32_t crc32;
    int type = TYPE_NO_COMPRESSION;
    size_t output_length = 0;
    char* output = NULL;
    Buffer *output_buffer, st_buffer;

#ifdef WITH_SNAPPY
    if (!skip_comp)
    {
        output_length = snappy_max_compressed_length(block->buffer->length);
        output = (char *)malloc(output_length + sizeof(uint32_t) * 2);

        if (snappy_compress(block->buffer->mem, block->buffer->length, output, &output_length) != SNAPPY_OK
            || ((float)output_length / (float)block->buffer->length) > 0.8)
        {
            free(output);
            output = NULL;
            output_length = 0;
        }
        else
        {
            type = TYPE_SNAPPY_COMPRESSION;

            st_buffer.mem = output;
            st_buffer.length = output_length;
            st_buffer.allocated = output_length + sizeof(uint32_t) * 2;
            output_buffer = &st_buffer;
        }
    }
#endif

    if (type == TYPE_NO_COMPRESSION)
    {
        output = block->buffer->mem;
        output_length = block->buffer->length;
        output_buffer = block->buffer;
    }

    crc32 = crc32_extend(0, output_buffer->mem, output_buffer->length);

    buffer_putint32(output_buffer, type);
    buffer_putint32(output_buffer, crc32);

    file_append(self->file, output_buffer);

    // The value of the index is the offset in the file pointing to the
    // actual data block.
    buffer_clear(self->last_block_offset);
    buffer_putvarint64(self->last_block_offset, self->offset);

//    DEBUG("Block @ offset: 0x%X crc32: %u", self->offset, crc32);

#ifdef WITH_BLOOM_FILTER
    if (block == self->data_block)
        bloom_builder_generate(self->bloom, self->offset, self->data_block);
#endif

    self->offset += output_buffer->length;
    buffer_putvarint64(self->last_block_offset, output_buffer->length);

    if (type != TYPE_NO_COMPRESSION)
        free(output);
}

static void _sst_builder_flush(SSTBuilder* self)
{
    if (!self->block_written)
    {
        _write_block(self, self->data_block, 0);
        self->metadata_num_blocks++;
    }

    self->block_written = 1;
    self->pending_index = 1;
    self->needs_reset = 1;
}

static void _write_footer(SSTBuilder* self)
{
    self->metadata_data_size = self->offset;

#ifdef WITH_BLOOM_FILTER
    // First write the bloom filter than all the statistics
    bloom_builder_finish(self->bloom);

    size_t bloom_off = self->offset;
    size_t bloom_size = self->bloom->buff->length;
    file_append(self->file, self->bloom->buff);
    self->offset += bloom_size;

    self->metadata_filter_size = self->bloom->buff->length;
#endif

    // Here just save as indexing term the last key and then we save the index block
    sst_block_builder_add(self->index_block, self->data_block->last_key, self->last_block_offset, ADD);

    // Write the actual metadata
    buffer_clear(self->last_key);
    buffer_putint64(self->last_key, self->metadata_data_size);
    buffer_putint64(self->last_key, self->metadata_index_size);
    buffer_putint64(self->last_key, self->metadata_key_size);
    buffer_putint64(self->last_key, self->metadata_num_blocks);
    buffer_putint64(self->last_key, self->metadata_num_entries);
    buffer_putint64(self->last_key, self->metadata_value_size);

#ifdef WITH_BLOOM_FILTER
    buffer_putint64(self->last_key, self->metadata_filter_size);
    buffer_putint64(self->last_key, bloom_off);
    buffer_putint64(self->last_key, bloom_size);
#endif

    size_t meta_off = self->offset;
    size_t meta_size = self->last_key->length;

    file_append(self->file, self->last_key);
    self->offset += meta_size;

    size_t index_off = self->offset;
    _write_block(self, self->index_block, 1);

    size_t index_size = self->index_block->buffer->length;
    self->metadata_index_size = index_size;

    DEBUG("Index block @ offset: 0x%X size: %u", index_off, index_size);
    DEBUG("Meta block @ offset: 0x%X size: %u", meta_off, meta_size);

#ifdef WITH_BLOOM_FILTER
    DEBUG("Bloom block @ offset: 0x%X size: %u", bloom_off, bloom_size);
#endif

    buffer_clear(self->last_key);
    buffer_putint64(self->last_key, index_off);
    buffer_putint64(self->last_key, index_size);
    buffer_putint64(self->last_key, meta_off);
    buffer_putint64(self->last_key, meta_size);

    file_append(self->file, self->last_key);
}

static void _sst_builder_finish(SSTBuilder* self)
{
    _sst_builder_flush(self);
    _write_footer(self);

    buffer_putnstr(self->last_key, MAGIC_STR, 8);
    file_append(self->file, self->last_key);

    file_close(self->file);
}

SSTBuilder* sst_builder_new(File* file)
{
    SSTBuilder* self = malloc(sizeof(SSTBuilder));

    if (!self)
        PANIC("NULL allocation");

    self->file = file;
    self->pending_index = 0;
    self->needs_reset = 0;
    self->offset = 0;

    self->metadata_data_size = 0;
    self->metadata_index_size = 0;
    self->metadata_key_size = 0;
    self->metadata_num_blocks = 0;
    self->metadata_num_entries = 0;
    self->metadata_value_size = 0;

#ifdef WITH_BLOOM_FILTER
    self->metadata_filter_size= 0;
    self->bloom = bloom_builder_new(BITS_PER_KEY);
#endif

    self->last_key = buffer_new(1024);
    // Just 5 bytes are sufficient to store a file offset as a varint32
    self->last_block_offset = buffer_new(5);
    self->index_block = sst_block_builder_new(FLAG_NOCOMPRESS | FLAG_INDEX, 1);

    self->data_block = sst_block_builder_new(FLAG_COMPRESS, RESTART_INTERVAL);

    return self;
}

void sst_builder_free(SSTBuilder* self)
{
    _sst_builder_finish(self);

    sst_block_builder_free(self->data_block);
    sst_block_builder_free(self->index_block);

    buffer_free(self->last_key);
    buffer_free(self->last_block_offset);

#ifdef WITH_BLOOM_FILTER
    bloom_builder_free(self->bloom);
#endif

    free(self);
}

void sst_builder_add(SSTBuilder* self, Variant* key, Variant* value, OPT opt)
{
//    DEBUG("ADD: %.*s %.*s %d %d", key->length, key->mem, value->length, value->mem, opt, self->pending_index);

    self->block_written = 0;

    if (self->needs_reset)
    {
        sst_block_builder_reset(self->data_block);
        self->needs_reset = 0;
    }

    if (self->pending_index)
    {
        // Extract the shortest separator that indexes the block >= all keys
        shortest_separator(self->last_key, key);
        sst_block_builder_add(self->index_block, self->last_key, self->last_block_offset, ADD);
        self->pending_index = 0;
    }

    sst_block_builder_add(self->data_block, key, value, opt);

    self->metadata_num_entries++;
    self->metadata_key_size += key->length;
    self->metadata_value_size += value->length;

    if (sst_block_builder_current_size(self->data_block) >= BLOCK_SIZE)
    {
        // Get the last key which is part of the just written data block
        buffer_clear(self->last_key);
        buffer_putnstr(self->last_key, key->mem, key->length);

        // The offset of the block will be updated inside this call
        _sst_builder_flush(self);
    }
}
