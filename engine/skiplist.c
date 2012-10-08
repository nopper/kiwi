#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <signal.h>
#include <assert.h>
#include "skiplist.h"
#include "config.h"
#include "utils.h"
#include "indexer.h"

#define cmp_lt(node, key, klen) (comparator((const char *)node, key, klen) < 0)
#define cmp_eq(node, key, klen) (comparator((const char *)node, key, klen) == 0)

SkipList* skiplist_new(size_t max_count)
{
    int i;
    SkipList* self = calloc(1, sizeof(SkipList));

    if (!self)
        PANIC("NULL allocation");

    self->hdr = malloc(SKIPNODE_SIZE + SKIPLIST_MAXLEVEL * sizeof(SkipNode*));
    self->hdr->klen = 0;
    self->hdr->level = SKIPLIST_MAXLEVEL;
    self->level = 1;

    if (!self->hdr)
        PANIC("NULL allocation");

    for (i = 0; i < SKIPLIST_MAXLEVEL; i++)
        NODE_FWD_SET(self->hdr, i, self->hdr);

    self->max_count = max_count;
    self->arena = arena_new();

    return self;
}

void skiplist_free(SkipList* self)
{
    arena_free(self->arena);
    free(self->hdr);
    free(self);
}

inline int skiplist_notfull(SkipList* self)
{
    if (self->count < self->max_count)
        return 1;

    return 0;
}

SkipNode* skiplist_new_node(SkipList* self, size_t klen, size_t *reserved, int* level)
{
    SkipNode* x;
    int new_level = 0;
    size_t allocated;

    for (new_level = 0; rand() < RAND_MAX/2 && new_level < SKIPLIST_MAXLEVEL - 1; new_level++);

    allocated = SKIPNODE_SIZE +    // basic header
            sizeof(char) * klen +  // actual key val
            (new_level + 1) * sizeof(SkipNode*);

    if ((x = arena_alloc(self->arena, allocated)) == NULL)
        PANIC("NULL allocation");

    *reserved = allocated;
    *level = new_level + 1;

    x->klen = klen;
    x->level = new_level + 1;

    self->last = x;
    self->last_size = allocated;

    return x;
}

void skiplist_free_node(SkipList* self, size_t reserved)
{
    arena_dealloc(self->arena, reserved);

    self->last = NULL;
    self->last_size = 0;
}

static int comparator(const char *encoded, const char *key, size_t klen)
{
    uint32_t encoded_len = 0;
    encoded = get_varint32(encoded, encoded + 5, &encoded_len);
    return string_cmp(encoded, key, encoded_len, klen);
}

int skiplist_insert_last(SkipList* self, const char *key, size_t klen, int new_level, OPT opt)
{
    SkipNode* update[SKIPLIST_MAXLEVEL];
    SkipNode* newnode = self->last;

    int set = 0;

    memset(update, 0, sizeof(update));

    if (!skiplist_notfull(self))
        return STATUS_ERR;

    SkipNode* current = self->hdr;
    SkipNode* sentinel = NULL;
    int current_level = self->hdr->level;

    for (int i = self->level - 1; i >= 0; i--)
    {
        SkipNode* target = NODE_FWD(current, i);

        while (target != self->hdr && cmp_lt(NODE_KEY(target), key, klen))
        {
            current = target;
            target = NODE_FWD(current, i);
        }

        if (target != self->hdr && sentinel != target)
        {
            sentinel = target;
            current_level = i + 1;
        }

        update[i] = current;
    }

    SkipNode* prev = current;
    current = NODE_FWD(current, 0);

    // If this is valid we can use this directly instead of having a different field
    assert(new_level == newnode->level);
    assert(current_level == current->level);

    if (current != self->hdr && cmp_eq(NODE_KEY(current), key, klen))
    {
        if (opt == DEL)
        {
            INFO("Node previously inserted. Only marking it as deleted");
            *(NODE_KEY(current) + klen) = MARK_DELETED;
            return STATUS_OK_DEALLOC;
        }
        else if (newnode->klen <= current->klen)
        {
            if (newnode->klen < current->klen)
            {
                INFO("Node previously inserted but the new object fits (smaller).");
                int cklen = current->klen, clevel = current->level;
                int diff = current->klen - newnode->klen;

                INFO("Diff bytes: %d", diff);

                self->wasted_bytes += diff;

                memcpy(NODE_KEY(current) + newnode->klen, NODE_KEY(current) + cklen, sizeof(SkipNode*) * clevel);
                memcpy(NODE_KEY(current), NODE_KEY(newnode), newnode->klen);
                memset(NODE_KEY(current) + newnode->klen + sizeof(SkipNode*) * clevel, 'D', diff);
                current->klen = newnode->klen;
            }
            else
            {
                INFO("Node previously inserted but the new object fits perfectly.");
                memcpy(NODE_KEY(current), NODE_KEY(newnode), newnode->klen);
            }

            return STATUS_OK_DEALLOC;
        }
        else
        {
            int diff = (newnode->level - current->level) * sizeof(SkipNode*);
            self->arena->pool->memory -= diff;
            self->arena->pool->remaining += diff;
            self->count_unused++;
            self->wasted_bytes += current->klen + SKIPNODE_SIZE + sizeof(SkipNode*) * current->level;

            newnode->level = current->level;

            for (int j = 0; j < current->level; j++)
                NODE_FWD_SET(newnode, j, NODE_FWD(current, j));

            for (int j = 0; j < prev->level; j++)
            {
                if (NODE_FWD(prev, j) == current)
                    NODE_FWD_SET(prev, j, newnode);
            }

            set = 1;
        }
    }

    if (newnode->level > self->level)
    {
        for (int i = self->level; i < newnode->level; i++)
            update[i] = self->hdr;

        self->level = newnode->level;
    }

    self->count++;

    if (set)
        return STATUS_OK;

    for (int i = 0; i < newnode->level; i++)
    {
        NODE_FWD_SET(newnode, i, NODE_FWD(update[i], i));
        NODE_FWD_SET(update[i], i, newnode);
    }

    return STATUS_OK;
}

SkipNode* skiplist_last(SkipList* self)
{
    int i;
    SkipNode* x = self->hdr;

    for (i = self->level - 1; i >= 0; i--) {
        SkipNode* target = NODE_FWD(x, i);
        while (target != self->hdr)
        {
            x = target;
            target = NODE_FWD(x, i);
        }
    }

    return x;
}

SkipNode* skiplist_first(SkipList* self)
{
    return NODE_FWD(self->hdr, 0);
}

SkipNode* skiplist_lookup(SkipList* self, char* key, size_t klen)
{
    int i;
    SkipNode* x = self->hdr;

    for (i = self->level - 1; i >= 0; i--) {
        while (NODE_FWD(x, i) != self->hdr &&
               cmp_lt(NODE_KEY(NODE_FWD(x, i)), key, klen))
            x = NODE_FWD(x, i);
    }
    x = NODE_FWD(x, 0);
    if (x != self->hdr && cmp_eq(NODE_KEY(x), key, klen))
        return (x);

    return NULL;
}
