#include <string.h>
#include <inttypes.h>
#include "sst_loader.h"
#include "utils.h"
#include "crc32.h"

static int _read_block(SSTLoader* self, uint64_t offset, uint64_t size, char **begin, char **end)
{
    char* start = self->file->base + offset;
    char* stop = start + size - sizeof(uint32_t) * 2;

    uint32_t block_type = get_int32(stop);
    uint32_t block_crc32 = get_int32(stop + sizeof(uint32_t));
    uint32_t actual_crc32 = crc32_extend(0, start, stop - start);

    if (actual_crc32 != block_crc32)
    {
        ERROR("Data block seems to be corrupted. Data CRC: %X Block CRC: %X",
              actual_crc32, block_crc32);
        return 0;
    }

    if (block_type == TYPE_SNAPPY_COMPRESSION)
    {

    }

    *begin = start;
    *end = stop;

    return 1;
}

static int _load_index(SSTLoader* self, uint64_t offset, uint64_t size)
{
    const char* start = self->file->base + offset;
    const char* stop = start + size - sizeof(uint32_t) * 2;

    uint32_t block_type = get_int32(stop);
    uint32_t block_crc32 = get_int32(stop + sizeof(uint32_t));
    uint32_t actual_crc32 = crc32_extend(0, start, stop - start);

    if (actual_crc32 != block_crc32)
    {
        ERROR("Data block seems to be corrupted. Data CRC: %X Block CRC: %X",
              actual_crc32, block_crc32);
        return 0;
    }

    if (block_type == TYPE_SNAPPY_COMPRESSION)
    {

    }

    uint32_t vlen;
    IndexEntry* entry;

    // Now let's read the contents from the index
    while (start < stop)
    {
        entry = malloc(sizeof(IndexEntry));

        start += 1; // Skip the first character. We do not have any kind of shared
        start = get_varint32(start, start + 5, &entry->klen);
        start = get_varint32(start, start + 5, &vlen);

        entry->key = malloc(sizeof(char) * entry->klen);
        memcpy(entry->key, start, entry->klen);

        start += entry->klen;
        start = get_varint64(start, start + 9, &entry->offset);
        start = get_varint64(start, start + 9, &entry->size);

//        DEBUG("Key <= %.*s are at offset %" PRIu64 " size: %" PRIu64,
//              entry->klen, entry->key, entry->offset, entry->size);
//        DEBUG("Start: %p Stop: %p", start, stop);

        kv_push(IndexEntry*, self->index, entry);
    }

    return 1;
}

static int _read_footer(SSTLoader* self)
{
    if (memcmp(self->file->limit - 8, MAGIC_STR, 8) != 0)
        ERROR("The file %s does not seem to be a sst file", self->file->filename);

    uint64_t index_off = 0, index_sz = 0;
    uint64_t meta_off = 0, meta_sz = 0;

    const char* start = self->file->limit - FOOTER_SIZE;

    index_off = get_int64(start);
    index_sz = get_int64(start + sizeof(uint64_t));
    meta_off = get_int64(start + sizeof(uint64_t) * 2);
    meta_sz = get_int64(start + sizeof(uint64_t) * 3);

    DEBUG("Index @ offset: %" PRIu64 " size: %" PRIu64, index_off, index_sz);
    DEBUG("Meta Block @ offset: %" PRIu64 " size: %" PRIu64, meta_off, meta_sz);

    // TODO: probably here we should load the first string from the file
    // in order to know the (start, stop) interval the file ranges

    if (!_load_index(self, index_off, index_sz))
        return 0;

    return 1;
}

SSTLoader* sst_loader_new(File* file, uint32_t level, uint32_t filenum)
{
    SSTLoader* self = malloc(sizeof(SSTLoader));

    if (!self)
        PANIC("NULL allocation");

    self->file = file;
    self->level = level;
    self->filenum = filenum;

    kv_init(self->index);

    if (!mmapped_file_new(self->file))
    {
        ERROR("Unable to mmap the file %s", self->file->filename);
        goto err;
    }

    if (!_read_footer(self))
        goto err;

    return self;

err:
    {
        sst_loader_free(self);
        return NULL;
    }
}

void sst_loader_free(SSTLoader* self)
{
    file_free(self->file);
    free(self);
}

IndexEntry* _get_index_entry(SSTLoader* self, Variant* key)
{
    int ret;
    IndexEntry* entry;
    uint32_t left = 0, right = kv_size(self->index) - 1;

    while (left < right)
    {
        uint32_t mid = (left + right) / 2;
        entry = kv_A(self->index, mid);

        ret = string_cmp(entry->key, key->mem, entry->klen, key->length);
        INFO("[1 of 3] L: %d R: %d M: %d Comparing: %.*s %.*s = %d", left, right, mid, entry->klen, entry->key, key->length, key->mem, ret);

        if (ret < 0) // block < key
            left = mid + 1;
        else // key < block
            right = mid;
    }

    entry = kv_A(self->index, left);
    INFO("[1 of 3] Key %.*s is contained in block <= %.*s", key->length, key->mem, entry->klen, entry->key);

    return entry;
}

int sst_loader_get(SSTLoader* self, Variant* key, Variant* value)
{
    IndexEntry *entry = _get_index_entry(self, key);

    int ret;
    char *start = NULL, *stop = NULL, *iter;
    _read_block(self, entry->offset, entry->size, &start, &stop);

    uint32_t num_restarts = get_int32(stop - sizeof(uint32_t));
    stop -= sizeof(uint32_t) * (num_restarts + 1);
    INFO("There are %d restart points in this data block", num_restarts);

    // [start - stop] points to the actual data, not the restarts array
    uint32_t left = 0;
    uint32_t right = num_restarts - 1;
    uint32_t plen, klen, vlen;

    while (left < right)
    {
        uint32_t mid = (left + right + 1) / 2;

        // Skip the first character which must be a 0 since this is a
        // restart position
        iter = start + get_int32(stop + (sizeof(uint32_t) * mid)) + 1;
        iter = (char *)get_varint32(iter, iter + 5, &klen);
        iter = (char *)get_varint32(iter, iter + 5, &vlen);

        ret = string_cmp(iter, key->mem, klen, key->length);
        INFO("[2 of 3] Comparing: L: %d R: %d M: %d %.*s %.*s = %d", left, right, mid, klen, iter, key->length, key->mem, ret);

        if (ret <= 0) // restart < key => might be ok
            left = mid;
        else // key < restart
            right = mid - 1;
    }

    INFO("Left is %d Right is %d", left, right);
    buffer_clear(value);

    if (ret == 0)
    {
        buffer_putnstr(value, iter + klen, vlen);
        return 1;
    }

    // From here we temporarly use use the value as storage buffer
    iter = start + get_int32(stop + (sizeof(uint32_t) * left));

    do {
        // Skip the first character which is 0 since this is a restart position
        iter = (char *)get_varint32(iter, iter + 5, &plen);
        iter = (char *)get_varint32(iter, iter + 5, &klen);
        iter = (char *)get_varint32(iter, iter + 5, &vlen);

        DEBUG("plen: %d vlen: %d", plen, vlen);

        value->length = plen;
        buffer_putnstr(value, iter, klen);

        ret = string_cmp(value->mem, key->mem, value->length, key->length);

        iter += klen + vlen;

        INFO("Comparing %.*s with %.*s = %d", key->length, key->mem, value->length, value->mem, ret);
    } while (ret < 0 && iter < stop);

    buffer_clear(value);

    if (ret == 0)
    {
        buffer_putnstr(value, iter - vlen, vlen);
        return 1;
    }

    return 0;
}

static void _sst_loader_iterator_next_block(SSTLoaderIterator* iter)
{
    iter->block++;

    if (iter->block >= kv_size(iter->loader->index))
    {
        iter->valid = 0;
        return;
    }

    IndexEntry* entry = kv_A(iter->loader->index, iter->block);
    _read_block(iter->loader, entry->offset, entry->size, &iter->start, &iter->stop);

    iter->current = iter->start;

    // We need to skip restart pointer since we are just iterating over the structure
    // at least for now
    uint32_t num_restarts = get_int32(iter->stop - sizeof(uint32_t));
    iter->stop -= sizeof(uint32_t) * (num_restarts + 1);

    sst_loader_iterator_next(iter);
}

SSTLoaderIterator* sst_loader_iterator(SSTLoader* self)
{
    SSTLoaderIterator* iter = malloc(sizeof(SSTLoaderIterator));

    iter->block = -1;
    iter->loader = self;

    iter->key = buffer_new(32);
    iter->value = buffer_new(32);
    iter->valid = 0;

    _sst_loader_iterator_next_block(iter);
    return iter;
}

void sst_loader_iterator_free(SSTLoaderIterator *iter)
{
    buffer_free(iter->key);
    buffer_free(iter->value);
    free(iter);
}

void sst_loader_iterator_next(SSTLoaderIterator* iter)
{
    if (iter->start >= iter->stop)
    {
        _sst_loader_iterator_next_block(iter);
        return;
    }

    uint32_t plen, klen, vlen;
    iter->start = (char *)get_varint32(iter->start, iter->start + 5, &plen);
    iter->start = (char *)get_varint32(iter->start, iter->start + 5, &klen);
    iter->start = (char *)get_varint32(iter->start, iter->start + 5, &vlen);

    iter->key->length = plen;
    buffer_putnstr(iter->key, iter->start, klen);
    iter->start += klen;

    buffer_clear(iter->value);
    buffer_putnstr(iter->value, iter->start, vlen);
    iter->start += vlen;
    iter->valid = 1;
}

int sst_loader_iterator_valid(SSTLoaderIterator* iter)
{
    return iter->valid;
}

int sst_loader_iterator_compare(SSTLoaderIterator* a, SSTLoaderIterator* b)
{
    if (a->valid && b->valid)
    {
        return string_cmp(a->key->mem, b->key->mem, a->key->length, b->key->length);
    }

    if (!a->valid && !b->valid)
        return 0;

    return (!a->valid) ? -1 : 1;
}
