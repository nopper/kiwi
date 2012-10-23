#ifndef __LRU_H__
#define __LRU_H__

#include <sys/types.h>
#include <inttypes.h>

typedef struct _lru_key {
    int filenum;
    uint64_t offset;
} LRUKey;

typedef struct _lru_value {
    void *start;
    void *stop;
} LRUValue;

typedef struct _lru_node {
    size_t size;
    LRUKey key;
    LRUValue value;
    struct _lru_node* next;
    struct _lru_node* prev;
} LRUNode;

typedef struct _lru_list {
    uint64_t count;
    uint64_t used;
    LRUNode* head;
    LRUNode* tail;
} LRUList;

struct _ht;

typedef struct _lru {
    uint64_t allow;
    LRUList* list;
    struct _ht* ht;
} LRU;

typedef struct _ht_node {
    LRUNode* data;
    struct _ht_node* next;
} HTNode;

typedef struct _ht {
    size_t size;
    uint64_t capacity;
    HTNode** buckets;
} HT;

LRU* lru_new(uint64_t size);
void lru_free(LRU* self);

void lru_set(LRU* self, const LRUKey* key, const LRUValue *value);
int lru_get(LRU* self, const LRUKey* key, LRUValue* value);

#endif
