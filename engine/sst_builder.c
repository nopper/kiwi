#include "sst_builder.h"
#include "crc32.h"

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
            if (last_key->mem[diff_index] < 0xff &&
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

static void short_successor(Variant *last_key)
{
    for (size_t i = 0; i < last_key->length; i++)
    {
        if (last_key->mem[i] != 0xff)
            last_key->mem[i]++;
        last_key->length = i + 1;
    }
}

static void _write_block(SSTBuilder* self, SSTBlockBuilder* block)
{
    // Write the contents of the block to the file
    sst_block_builder_flush(block);
    uint32_t crc32 = crc32_extend(0, block->buffer->mem, block->buffer->length);

    // TODO: compression here
    buffer_putint32(block->buffer, TYPE_NO_COMPRESSION);
    buffer_putint32(block->buffer, crc32);

    file_append(self->file, block->buffer);

    // The value of the index is the offset in the file pointing to the
    // actual data block.
    buffer_clear(self->last_block_offset);
    buffer_putvarint64(self->last_block_offset, self->offset);

    DEBUG("Block @ offset: 0x%X crc32: %u", self->offset, crc32);
    self->offset += block->buffer->length;

    buffer_putvarint64(self->last_block_offset, block->buffer->length);
}

static void _sst_builder_flush(SSTBuilder* self)
{
    if (!self->block_written)
        _write_block(self, self->data_block);
    self->block_written = 1;
    self->pending_index = 1;
    self->needs_reset = 1;
}

static void _sst_builder_finish(SSTBuilder* self)
{
    _sst_builder_flush(self);

    // Here just save as indexing term the last key and then we save the index block
    sst_block_builder_add(self->index_block, self->data_block->last_key, self->last_block_offset);

    size_t offset = self->offset;
    _write_block(self, self->index_block);

    // Here we need to write the block footer. We use last key as a temporary buffer
    buffer_clear(self->last_key);

//    DEBUG("Index block @ offset: 0x%X size: %u",
//          offset, self->index_block->buffer->length);

    // Index position
    buffer_putint64(self->last_key, offset);
    buffer_putint64(self->last_key, self->index_block->buffer->length);

    // Metablock position
    buffer_putint64(self->last_key, 0);
    buffer_putint64(self->last_key, 0);

    // Magic string
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

    free(self);
}

void sst_builder_add(SSTBuilder* self, Variant* key, Variant* value)
{
//    DEBUG("ADD: %.*s %.*s %d", key->length, key->mem, value->length, value->mem, self->pending_index);

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
        sst_block_builder_add(self->index_block, self->last_key, self->last_block_offset);
        self->pending_index = 0;
    }

    sst_block_builder_add(self->data_block, key, value);

    if (sst_block_builder_current_size(self->data_block) >= BLOCK_SIZE)
    {
        // Get the last key which is part of the just written data block
        buffer_clear(self->last_key);
        buffer_putnstr(self->last_key, key->mem, key->length);

        // The offset of the block will be updated inside this call
        _sst_builder_flush(self);
    }
}
