#include <stdlib.h>
#include <string.h>
#include "lru.h"
#include "indexer.h"

static HT* hashtable_new(uint64_t capacity)
{
    HT* self = calloc(1, sizeof(HT));

    self->capacity = capacity;
    self->buckets = malloc(capacity * sizeof(HTNode*));

    memset(self->buckets, 0, sizeof(HTNode*) * capacity);

    return self;
}

static void hashtable_free(HT* ht)
{
    uint64_t i;

    for (i = 0; i < ht->capacity; i++)
    {
        if (ht->buckets[i])
        {
            HTNode* cur = ht->buckets[i];
            HTNode* nxt;

            while (cur)
            {
                nxt = cur->next;
                free(cur);
                cur = nxt;
            }
        }
    }

    free(ht->buckets);
    free(ht);
}

static inline uint64_t _find_slot(HT* ht, const LRUKey* key)
{
    return ((key->filenum ^ key->offset) + 3) % ht->capacity;
}

static LRUNode* hashtable_get(HT* ht, const LRUKey* key)
{
    if (!key)
        return NULL;

    uint64_t slot = _find_slot(ht, key);
    HTNode* node;

    node = ht->buckets[slot];

    while (node)
    {
        if (node->data->key.offset == key->offset &&
            node->data->key.filenum == key->filenum)
            return node->data;

        node = node->next;
    }

    return NULL;
}

static void hashtable_set(HT* ht, const LRUKey* key, LRUNode* data)
{
    if (!key || !data)
        return;

    uint64_t slot = _find_slot(ht, key);
    HTNode* node = calloc(1, sizeof(HTNode));

    node->data = data;
    node->next = ht->buckets[slot];

    ht->buckets[slot] = node;
    ht->size++;
}

void hashtable_remove(HT* ht, const LRUKey* key)
{
    if (!key)
        return;

    uint64_t slot = _find_slot(ht, key);

    HTNode* cur;
    HTNode* prev = NULL;
    cur = ht->buckets[slot];

    while (cur)
    {
        if (key->offset == cur->data->key.offset &&
            key->filenum == cur->data->key.filenum)
        {
            if (prev != NULL)
                prev->next = cur->next;
            else
                ht->buckets[slot] = cur->next;

            free(cur);

            ht->size--;
            return;
        }

        prev = cur;
        cur = cur->next;
    }
}


static LRUList* list_new(void)
{
    return calloc(1, sizeof(LRUList));
}

static void list_remove(LRUList* list, LRUNode* node)
{
    if (!node)
        return;

    LRUNode* pre = node->prev;
    LRUNode* nxt = node->next;

    if (pre)
    {
        if (nxt)
        {
            pre->next = nxt;
            nxt->prev = pre;
        }
        else
        {
            list->tail = pre;
            pre->next = NULL;
        }
    }
    else
    {
        if (nxt)
        {
            nxt->prev = NULL;
            list->head = nxt;
        }
        else
        {
            list->head = NULL;
            list->tail = NULL;
        }
    }

    list->used -= node->size;
    list->count--;

    free(node->value.start);
    free(node);
}

static void list_free(LRUList* list)
{
    LRUNode* cur = list->head;
    LRUNode* nxt = cur;

    while (cur)
    {
        nxt = cur->next;
        list_remove(list, cur);
        cur = nxt;
    }

    free(list);
}

static void list_push(LRUList* list, LRUNode* node)
{
    if (!node)
        return;

    if (list->head != NULL)
    {
        node->next = list->head;
        list->head->prev = node;
        list->head = node;
    }
    else
    {
        list->head = node;
        list->tail = node;
    }

    list->used += node->size;
    list->count++;
}

LRU* lru_new(uint64_t size)
{
    LRU* self = calloc(1, sizeof(LRU));

#ifdef BACKGROUND_MERGE
    pthread_mutex_init(&self->lock, NULL);
#endif

    self->list = list_new();
    self->ht = hashtable_new(size);
    self->allow = size;

    return self;
}

void lru_free(LRU* self)
{
    hashtable_free(self->ht);
    list_free(self->list);
    free(self);
}

static void _lru_check(LRU* self)
{
    while (self->list->used >= self->allow)
    {
        LRUNode* tail = self->list->tail;
        HT* ht = self->ht;

        while (tail && tail->refcount > 0)
        {
            LRUNode* ref = tail;
            tail = tail->prev;

            self->list->tail = tail;
            tail->next = NULL;

            ref->next = self->list->head;
            self->list->head->prev = ref;
            ref->prev = NULL;
            self->list->head = ref;
        }

        /* free list tail */
        if (tail && tail->refcount <= 0)
        {
            hashtable_remove(ht, &tail->key);
            list_remove(self->list, tail);
        }
    }
}

void lru_set(LRU* self, const LRUKey* key, const LRUValue* value)
{
#ifdef BACKGROUND_MERGE
    pthread_mutex_lock(&self->lock);
#endif

    if (self->allow == 0)
        goto out;

    _lru_check(self);

    LRUNode* node = hashtable_get(self->ht, key);

    if (node)
    {
        hashtable_remove(self->ht, key);
        list_remove(self->list, node);
    }

    node = calloc(1, sizeof(LRUNode));

    node->key.filenum = key->filenum;
    node->key.offset = key->offset;

    node->value.start = value->start;
    node->value.stop = value->stop;

    node->refcount = 1;
    node->size = sizeof(LRUNode) + (value->stop - value->start);

    hashtable_set(self->ht, &node->key, node);
    list_push(self->list, node);

    out:
#ifdef BACKGROUND_MERGE
    pthread_mutex_unlock(&self->lock);
#endif
}

int lru_get(LRU* self, const LRUKey* key, LRUValue* value)
{
#ifdef BACKGROUND_MERGE
    pthread_mutex_lock(&self->lock);
#endif

    int ret = 0;
    LRUNode* node = hashtable_get(self->ht, key);

    if (node)
    {
        value->start = node->value.start;
        value->stop = node->value.stop;
        node->refcount++;
        ret = 1;
    }

#ifdef BACKGROUND_MERGE
    pthread_mutex_unlock(&self->lock);
#endif

    return ret;
}

void lru_release(LRU* self, const LRUKey* key)
{
#ifdef BACKGROUND_MERGE
    pthread_mutex_lock(&self->lock);
#endif

    LRUNode* node = hashtable_get(self->ht, key);

    if (node)
        node->refcount--;

#ifdef BACKGROUND_MERGE
    pthread_mutex_unlock(&self->lock);
#endif
}
