#include <string.h>
#include "memtable.h"
#include "utils.h"
#include "indexer.h"

MemTable* memtable_new(void)
{
    MemTable* self = malloc(sizeof(MemTable));

    if (!self)
        PANIC("NULL allocation");

    self->list = skiplist_new(SKIPLIST_SIZE);
    self->add_count = 0;
    self->del_count = 0;

    return self;
}

void memtable_free(MemTable* self)
{
    skiplist_free(self->list);
    free(self);
}

static int _memtable_edit(MemTable* self, const Variant* key, const Variant* value, OPT opt)
{
    // Here we need to insert the new node that has as a skipnode's key
    // an encoded string that encompasses both the key and value supplied
    // by the user.

    size_t klen = varint_length(key->length);      // key length
    size_t vlen = varint_length(value->length);    // value length
    size_t encoded_len = klen + vlen + key->length + value->length + 1; // + 1 for tag

    char *mem = malloc(sizeof(char) * encoded_len);
    char *node_key = mem;

    encode_varint32(node_key, key->length);
    node_key += klen;

    memcpy(node_key, key->mem, key->length);
    node_key += key->length;

    *node_key = (opt == DEL) ? MARK_DELETED : MARK_ADDED;
    node_key++;

    encode_varint32(node_key, value->length);
    node_key += vlen;

    memcpy(node_key, value->mem, value->length);

    if (skiplist_insert(self->list, key->mem, key->length, opt, mem) == STATUS_OK_DEALLOC)
        free(mem);

    if (opt == ADD)
        self->add_count++;
    else
        self->del_count++;

//    DEBUG("memtable_edit: %.*s %.*s opt: %d", key->length, key->mem, value->length, value->mem, opt);


    return 1;
}

int memtable_add(MemTable* self, const Variant* key, const Variant* value)
{
    return _memtable_edit(self, key, value, ADD);
}

int memtable_remove(MemTable* self, const Variant* key)
{
    Variant value;
    value.length = 0;
    return _memtable_edit(self, key, &value, DEL);
}

int memtable_get(MemTable* self, const Variant *key, Variant* value)
{
    SkipNode* node = skiplist_lookup(self->list, key->mem, key->length);

    if (!node)
        return 0;

    const char* encoded = node->data;
    encoded += varint_length(key->length) + key->length;

    if (*encoded == MARK_DELETED)
        return 1;

    encoded++;

    uint32_t encoded_len = 0;
    encoded = get_varint32(encoded, encoded + 5, &encoded_len);
    buffer_putnstr(value, encoded, encoded_len);

    return 1;
}

int memtable_needs_compaction(MemTable *self)
{
    return (self->list->count >= SKIPLIST_SIZE ||
            self->list->allocated >= MAX_SKIPLIST_ALLOCATION);
}

void memtable_extract_node(SkipNode* node, Variant* key, Variant* value, OPT* opt)
{
    uint32_t length = 0;
    const char* encoded = node->data;
    encoded = get_varint32(encoded, encoded + 5, &length);

    buffer_clear(key);
    buffer_putnstr(key, encoded, length);

    encoded += length;
    *opt = (*encoded == MARK_ADDED) ? ADD : DEL;

    encoded++;
    length = 0;
    encoded = get_varint32(encoded, encoded + 5, &length);

    if (value)
    {
        buffer_clear(value);

        if (*opt == ADD)
            buffer_putnstr(value, encoded, length);

//        DEBUG("memtable_extract_node: %.*s %.*s opt: %d", key->length, key->mem, value->length, value->mem, *opt);
    }
}
