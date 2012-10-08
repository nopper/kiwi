#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <signal.h>
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

    self->hdr = malloc(SKIPNODE_SIZE + MAXLEVEL * sizeof(SkipNode*));
    self->hdr->klen = 0;

    if (!self->hdr)
        PANIC("NULL allocation");

    for (i = 0; i <= MAXLEVEL; i++)
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

    for (new_level = 0; rand() < RAND_MAX/2 && new_level < MAXLEVEL; new_level++);

    allocated = SKIPNODE_SIZE +        // basic header
            sizeof(char) * klen +  // actual key val
            (new_level + 1) * sizeof(SkipNode*);

    if ((x = arena_alloc(self->arena, allocated)) == NULL)
        PANIC("NULL allocation");

    *reserved = allocated;
    *level = new_level;

    x->klen = klen;

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
    int i = 0, node_level = 0;
    SkipNode* old_node = NULL;
    SkipNode* node = self->last;
    SkipNode* update[MAXLEVEL+1];
    SkipNode* x;

    memset(update, 0, sizeof(update));

    if (!skiplist_notfull(self))
        return STATUS_ERR;

    x = self->hdr;
    for (i = self->level; i >= 0; i--) {
        while (NODE_FWD(x, i) != self->hdr &&
               cmp_lt(NODE_KEY(NODE_FWD(x, i)), key, klen))
            x = NODE_FWD(x, i);

        if (!node_level)
            node_level = i;

        update[i] = x;
    }

    x = NODE_FWD(x, 0);
    if (x != self->hdr && cmp_eq(NODE_KEY(x), key, klen)) {
        INFO("Comparing: %d", cmp_eq(NODE_KEY(x), key, klen));

        if (opt == DEL)
        {
            INFO("Node previously inserted. Only marking it as deleted");
            *(NODE_KEY(x) + klen) = MARK_DELETED;
            return STATUS_OK_DEALLOC;
        }
        // TODO if the space is smaller we can just shift the pointers down avoiding waste of space
        else if (node->klen == x->klen)
        {
            INFO("Node previously inserted but the new object fits perfectly.");
            memcpy(NODE_KEY(x), NODE_KEY(node), node->klen);
            return STATUS_OK_DEALLOC;
        }
        else
        {
            INFO("Node previously inserted. Unlinking the old object");
            INFO("Size of SkipNode: %d len: %d level: %d", SKIPNODE_SIZE, x->klen, node_level);
            self->count_unused++;
            self->wasted_bytes += x->klen + SKIPNODE_SIZE + sizeof(SkipNode*) * (node_level + 1);

            if (node_level > new_level)
            {
                DEBUG("Newly created node must be reallocated. %d pointer missing", node_level - new_level);
                self->last_size += (node_level - new_level + 1) * sizeof(SkipNode*);
                self->last = node = arena_realloc(self->arena, node, self->last_size);
                new_level = node_level;
            }

            old_node = x;
        }
    }

    if (new_level > self->level) {
        for (i = self->level + 1; i <= new_level; i++)
            update[i] = self->hdr;

        self->level = new_level;
    }

    x = node;

    if (!old_node)
    {
        for (i = 0; i <= new_level; i++) {
            NODE_FWD_SET(x, i, NODE_FWD(update[i], i));
            NODE_FWD_SET(update[i], i, x);
        }

        self->count++;
    }
    else
    {
        for (i = 0; i <= new_level; i++) {
            NODE_FWD_SET(node, i, NODE_FWD(old_node, i));
            NODE_FWD_SET(update[i], i, node);
        }

        // TODO: remove me
        memset(old_node, 'B', SKIPNODE_SIZE + old_node->klen + (new_level) * sizeof(SkipNode*));
    }

    return STATUS_OK;
}

SkipNode* skiplist_last(SkipList* self)
{
    int i;
    SkipNode* x = self->hdr;

    for (i = self->level; i >= 0; i--) {
        while (NODE_FWD(x, i) != self->hdr)
            x = NODE_FWD(x, i);
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

    for (i = self->level; i >= 0; i--) {
        while (NODE_FWD(x, i) != self->hdr &&
               cmp_lt(NODE_KEY(NODE_FWD(x, i)), key, klen))
            x = NODE_FWD(x, i);
    }
    x = NODE_FWD(x, 0);
    if (x != self->hdr && cmp_eq(NODE_KEY(x), key, klen))
        return (x);

    return NULL;
}
