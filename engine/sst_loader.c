#include <string.h>
#include <assert.h>
#include <inttypes.h>
#include "sst_loader.h"
#include "utils.h"
#include "crc32.h"

#ifdef WITH_BLOOM_FILTER
#include "hash.h"
#endif

#ifdef WITH_SNAPPY
#include <snappy-c.h>
#endif

static int _read_block(SSTLoader* self, uint64_t offset, uint64_t size, char **alloced, char **begin, char **end, int must_cache)
{
    // If must_cache is set to 0 we are iterating over the keyspace. Skip caching but remember
    // to also free allocated strings
    LookupKey lru_key;
    CacheEntry *lru_value = NULL;

    lru_key.filenum = self->filenum;
    lru_key.offset = offset;

    if (must_cache)
    {
        lru_value = lru_get(self->cache, &lru_key);

        if (lru_value)
        {
            *begin = lru_value->start;
            *end = lru_value->stop;

            return 1;
        }
    }

    // get from lru(self->filenum, offset, &start, &stop)
    char* start = self->file->base + offset;
    char* stop = start + size - sizeof(uint32_t) * 2;

    uint32_t block_type = get_int32(stop);

#ifdef PARANOID_CHECK
    uint32_t block_crc32 = get_int32(stop + sizeof(uint32_t));
    uint32_t actual_crc32 = crc32_extend(0, start, stop - start);

    if (actual_crc32 != block_crc32)
    {
        ERROR("Data block seems to be corrupted. Data CRC: %X Block CRC: %X",
              actual_crc32, block_crc32);
        return 0;
    }
#endif

#ifdef WITH_SNAPPY
    if (block_type == TYPE_SNAPPY_COMPRESSION)
    {
        size_t output_length;

        if (snappy_uncompressed_length(start, stop - start, &output_length) != SNAPPY_OK)
            return 0;

        char* output = (char*)malloc(output_length);

        if (snappy_uncompress(start, stop - start, output, &output_length) != SNAPPY_OK)
        {
            free(output);
            return 0;
        }

        if (alloced)
            *alloced = output;
        *begin = output;
        *end = output + output_length;

        if (must_cache)
        {
            lru_value = malloc(sizeof(CacheEntry));

            lru_value->key.filenum = self->filenum;
            lru_value->key.offset = offset;
            lru_value->start = output;
            lru_value->stop = output + output_length;

            lru_set(self->cache, lru_value);
        }

        return 1;
    }
#endif
    *begin = start;
    *end = stop;

#if 0
    // Apparently there's some sort of race condition here
    start = malloc(*end - *begin);
    memcpy(start, *begin, *end - *begin);
    stop = start + (*end - *begin);

    *begin = start;
    *end = stop;

    lru_value->filenum = self->filenum;
    lru_value->offset = offset;
    lru_value->start = start;
    lru_value->stop = stop;

    lru_set(self->cache, lru_value);
#endif

    return 1;
}

static int _load_index(SSTLoader* self, uint64_t offset, uint64_t size)
{
    assert(size > 0);

    const char* start = self->file->base + offset;
    const char* stop = start + size - sizeof(uint32_t) * 2;

    assert(stop > start);

    uint32_t block_type = get_int32(stop);
    uint32_t block_crc32 = get_int32(stop + sizeof(uint32_t));
    uint32_t actual_crc32 = crc32_extend(0, start, stop - start);

    assert(block_type == 0);

    if (actual_crc32 != block_crc32)
    {
        ERROR("Index block seems to be corrupted. Data CRC: %X Block CRC: %X",
              actual_crc32, block_crc32);
        return 0;
    }

    uint32_t vlen;
    IndexEntry* entry;

    // Now let's read the contents from the index
    while (start < stop)
    {
        entry = calloc(1, sizeof(IndexEntry));

        start += 1; // Skip the first character. We do not have any kind of shared
        start = get_varint32(start, start + 5, (uint32_t*)&entry->klen);
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

    start = self->file->base + meta_off;

    self->data_size = get_int64(start); start+=8;
    self->index_size = get_int64(start); start+=8;
    self->key_size = get_int64(start); start+=8;
    self->num_blocks = get_int64(start); start+=8;
    self->num_entries = get_int64(start); start+=8;
    self->value_size = get_int64(start); start+=8;

#ifdef WITH_BLOOM_FILTER
    self->filter_size = get_int64(start); start+=8;
    self->bloom_off = get_int64(start); start+=8;
    self->bloom_size = get_int64(start); start+=8;
#endif

    INFO("Data size:        %" PRIu64, self->data_size);

    INFO("Index size:       %" PRIu64, self->index_size);
    INFO("Key size:         %" PRIu64, self->key_size);
    INFO("Num blocks size:  %" PRIu64, self->num_blocks);
    INFO("Num entries size: %" PRIu64, self->num_entries);
    INFO("Value size:       %" PRIu64, self->value_size);

#ifdef WITH_BLOOM_FILTER
    INFO("Filter size:      %" PRIu64, self->filter_size);
    INFO("Bloom offset %" PRIu64 " size: %" PRIu64, self->bloom_off, self->bloom_size);
#endif

    // TODO: probably here we should load the first string from the file
    // in order to know the (start, stop) interval the file ranges

    if (!_load_index(self, index_off, index_sz))
        return 0;

    return 1;
}

SSTLoader* sst_loader_new(LRU* cache, File* file, uint32_t level, uint32_t filenum)
{
    SSTLoader* self = calloc(1, sizeof(SSTLoader));

    if (!self)
        PANIC("NULL allocation");

    self->file = file;
    self->level = level;
    self->filenum = filenum;
    self->cache = cache;

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
    // Iterate and free
    for (int i = 0; i < kv_size(self->index); i++)
    {
        IndexEntry* entry = kv_A(self->index, i);
        free(entry->key);
        free(entry);
    }

    kv_destroy(self->index);
    file_free(self->file);
    free(self);
}

IndexEntry* _get_index_entry(SSTLoader* self, Variant* key, int *block)
{
    int ret;
    IndexEntry* entry;
    uint32_t left = 0, right = kv_size(self->index) - 1;

    while (left < right)
    {
        uint32_t mid = (left + right) / 2;
        entry = kv_A(self->index, mid);

        ret = string_cmp(entry->key, key->mem, entry->klen, key->length);
//        DEBUG("[1 of 3] L: %d R: %d M: %d Comparing: %.*s %.*s = %d", left, right, mid, entry->klen, entry->key, key->length, key->mem, ret);

        if (ret < 0) // block < key
            left = mid + 1;
        else // key < block
            right = mid;
    }

    if (block)
        *block = left;

    entry = kv_A(self->index, left);
//    DEBUG("[1 of 3] Key %.*s is contained in block <= %.*s", key->length, key->mem, entry->klen, entry->key);

#ifdef WITH_BLOOM_FILTER
    // Avoid bloom filters checking if we are actually iterating over the file
    if (block)
        return entry;

    // Check the bloom filter and see if the key is not inside this block

    char *position = self->file->base +
            self->bloom_off + self->bloom_size -
            sizeof(uint32_t) - // Remove count
            (sizeof(uint32_t) * self->num_blocks) +
            (sizeof(uint32_t) * left);

    uint32_t offset = get_int32(position);
    uint32_t next_offset = self->bloom_size - sizeof(uint32_t) * (self->num_blocks + 1);

    if (left < self->num_blocks - 1)
        next_offset = get_int32(position + sizeof(uint32_t));

//    DEBUG("Bloom filter for block %d left starts at off + %d (%d bytes)", left, offset, next_offset - offset);

    char* array = self->file->base + self->bloom_off + offset;
    size_t bits = (next_offset - offset) * 8;// -1) ?

    uint32_t h = hash(key->mem, key->length, 0xbc9f1d34);
    const uint32_t delta = (h >> 17) | (h << 15);  // Rotate right 17 bits

//    DEBUG("Hash of %.*s = %d", key->length, key->mem, h);

    for (size_t j = 0; j < NUM_PROBES; j++)
    {
        const uint32_t bitpos = h % bits;
        if ((array[bitpos / 8] & (1 << (bitpos % 8))) == 0)
        {
//            INFO("Bloom filters match!");
            return NULL;
        }
        h += delta;
    }
#endif

    return entry;
}

int sst_loader_get(SSTLoader* self, Variant* key, Variant* value, OPT* opt)
{
    IndexEntry* entry = _get_index_entry(self, key, NULL);

    if (!entry)
        return 0;

    int ret = -2;
    char *start = NULL, *stop = NULL, *iter;
    _read_block(self, entry->offset, entry->size, NULL, &start, &stop, 1);

    uint32_t num_restarts = get_int32(stop - sizeof(uint32_t));
    stop -= sizeof(uint32_t) * (num_restarts + 1);
//    INFO("There are %d restart points in this data block", num_restarts);

    // [start - stop] points to the actual data, not the restarts array
    uint32_t left = 0;
    uint32_t right = num_restarts - 1;
    uint32_t plen = 0, klen = 0, vlen = 0;

    while (left < right)
    {
        uint32_t mid = (left + right + 1) / 2;

        // Skip the first character which must be a 0 since this is a
        // restart position
        iter = start + get_int32(stop + (sizeof(uint32_t) * mid)) + 1;
        iter = (char *)get_varint32(iter, iter + 5, &klen);
        iter = (char *)get_varint32(iter, iter + 5, &vlen);

        ret = string_cmp(iter, key->mem, klen, key->length);
//        DEBUG("[2 of 3] Comparing: L: %d R: %d M: %d %.*s %.*s = %d", left, right, mid, klen, iter, key->length, key->mem, ret);

        if (ret <= 0) // restart < key => might be ok
            left = mid;
        else // key < restart
            right = mid - 1;
    }

//    DEBUG("Left is %d Right is %d VLEN is %d RET is %d", left, right, vlen, ret);
    buffer_clear(value);

    if (ret == 0)
    {
        if (vlen > 1)
            buffer_putnstr(value, iter + klen, vlen - 1);

        *opt = (vlen == 0) ? DEL : ADD;

        //_release_block(self, entry->offset, entry->size);
        return 1;
    }

    // From here we temporarly use use the value as storage buffer
    iter = start + get_int32(stop + (sizeof(uint32_t) * left));

    do {
        // Skip the first character which is 0 since this is a restart position
        iter = (char *)get_varint32(iter, iter + 5, &plen);
        iter = (char *)get_varint32(iter, iter + 5, &klen);
        iter = (char *)get_varint32(iter, iter + 5, &vlen);

//        DEBUG("plen: %d vlen: %d", plen, vlen);

        value->length = plen;
        buffer_putnstr(value, iter, klen);

        ret = string_cmp(value->mem, key->mem, value->length, key->length);

        iter += klen + MAX(0, vlen - 1);

//        DEBUG("Comparing %.*s with %.*s = %d", key->length, key->mem, value->length, value->mem, ret);
    } while (ret < 0 && iter < stop);

    buffer_clear(value);

    if (ret == 0)
    {
        if (vlen > 1)
            buffer_putnstr(value, iter - (vlen - 1), vlen - 1);

        *opt = (vlen == 0) ? DEL : ADD;

        //_release_block(self, entry->offset, entry->size);
        return 1;
    }

    //_release_block(self, entry->offset, entry->size);
    return 0;
}

static uint32_t _sst_loader_read_block(SSTLoaderIterator* iter, IndexEntry* entry)
{
    // NOTE: The current keep track of the beginning of the block and it is used
    // to release the string allocated in _read_block when must_cache is set to 0
    iter->current = NULL;
    _read_block(iter->loader, entry->offset, entry->size, &iter->current, &iter->start, &iter->stop, 0);

    // We need to skip restart pointer since we are just iterating over the structure
    // at least for now
    uint32_t num_restarts = get_int32(iter->stop - sizeof(uint32_t));
    iter->stop -= sizeof(uint32_t) * (num_restarts + 1);

    return num_restarts;
}

static void _sst_loader_iterator_next_block(SSTLoaderIterator* iter)
{
    iter->block++;
    iter->prev_block++;

    if (iter->prev_block >= 0)
    {
        //IndexEntry* entry = kv_A(iter->loader->index, iter->prev_block);
        free(iter->current);
        //_release_block(iter->loader, entry->offset, entry->size, 0);
    }

    if (iter->block >= kv_size(iter->loader->index))
    {
        iter->block = iter->prev_block = -1;
        iter->valid = 0;
        return;
    }

    _sst_loader_read_block(iter, kv_A(iter->loader->index, iter->block));
    sst_loader_iterator_next(iter);
}

static void _sst_loader_iterator_find(SSTLoaderIterator* iter, Variant* key)
{
    IndexEntry* entry = _get_index_entry(iter->loader, key, &iter->block);
    assert(entry != NULL);

    int ret = -2;
    uint32_t num_restarts = _sst_loader_read_block(iter, entry);

    uint32_t left = 0;
    uint32_t right = num_restarts - 1;
    uint32_t plen = 0, klen = 0, vlen = 0;

    char *ptr = iter->start;

    while (left < right)
    {
        uint32_t mid = (left + right + 1) / 2;

        ptr = iter->start + get_int32(iter->stop + (sizeof(uint32_t) * mid)) + 1;
        ptr = (char *)get_varint32(ptr, ptr + 5, &klen);
        ptr = (char *)get_varint32(ptr, ptr + 5, &vlen);

        ret = string_cmp(ptr, key->mem, klen, key->length);

        if (ret <= 0) // restart < key => might be ok
            left = mid;
        else // key < restart
            right = mid - 1;
    }

    assert(ptr != NULL);

    buffer_clear(iter->key);
    buffer_clear(iter->value);

    if (ret == 0)
    {
        iter->valid = 1;
        buffer_putnstr(iter->key, key->mem, key->length);

        if (vlen > 1)
            buffer_putnstr(iter->value, ptr + klen, vlen - 1);

        iter->opt = (vlen == 0) ? DEL : ADD;
        iter->start = ptr + klen;

        if (vlen > 1)
            iter->start += vlen - 1;

        return;
    }

    // From here we temporarly use use the value as storage buffer
    ptr = iter->start + get_int32(iter->stop + (sizeof(uint32_t) * left));

    do {
        ptr = (char *)get_varint32(ptr, ptr + 5, &plen);
        ptr = (char *)get_varint32(ptr, ptr + 5, &klen);
        ptr = (char *)get_varint32(ptr, ptr + 5, &vlen);

        assert(plen <= iter->key->length);

        iter->key->length = plen;
        buffer_putnstr(iter->key, ptr, klen);

        ret = string_cmp(iter->key->mem, key->mem, iter->key->length, key->length);

        ptr += klen;

        if (vlen > 1)
            ptr += vlen - 1;

    } while (ret < 0 && ptr < iter->stop);

    if (ptr >= iter->stop)
    {
        iter->valid = 0;
        iter->start = iter->stop;
    }

    if (vlen > 1)
        buffer_putnstr(iter->value, ptr - (vlen - 1), vlen - 1);

    iter->opt = (vlen == 0) ? DEL : ADD;
    iter->start = ptr;
    iter->valid = 1;
}

SSTLoaderIterator* sst_loader_iterator_seek(SSTLoader* self, Variant* key)
{
    SSTLoaderIterator* iter = malloc(sizeof(SSTLoaderIterator));

    iter->prev_block = -2;
    iter->block = -1;
    iter->loader = self;

    iter->key = buffer_new(32);
    iter->value = buffer_new(32);
    iter->valid = 0;
    iter->opt = ADD;

    if (!key)
        _sst_loader_iterator_next_block(iter);
    else
        _sst_loader_iterator_find(iter, key);

    return iter;
}

SSTLoaderIterator* sst_loader_iterator(SSTLoader* self)
{
    return sst_loader_iterator_seek(self, NULL);
}

void sst_loader_iterator_free(SSTLoaderIterator *iter)
{
    if (iter->prev_block >= 0)
        free(iter->current);

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

    uint32_t plen = 0, klen = 0, vlen = 0;
    iter->start = (char *)get_varint32(iter->start, iter->start + 5, &plen);
    iter->start = (char *)get_varint32(iter->start, iter->start + 5, &klen);
    iter->start = (char *)get_varint32(iter->start, iter->start + 5, &vlen);

    assert(plen <= iter->key->length);

    iter->key->length = plen;
    buffer_putnstr(iter->key, iter->start, klen);
    iter->start += klen;

    buffer_clear(iter->value);

    if (vlen > 1)
    {
        buffer_putnstr(iter->value, iter->start, vlen - 1);
        iter->start += vlen - 1;
    }

    iter->opt = (vlen == 0) ? DEL : ADD;
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
