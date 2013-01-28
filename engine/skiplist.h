#ifndef __SKIPLIST_H__
#define __SKIPLIST_H__

#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include "arena.h"
#include "config.h"
#include "variant.h"

#define SKIPLIST_MAXLEVEL (15)
#define SKIPNODE_SIZE (sizeof(SkipNode))
// This macro is used to support non-fixed size key length

#define MARK_DELETED 0x1
#define MARK_ADDED   0x0

typedef struct _skipnode {
    char* data;
    struct _skipnode* forward[1];
} SkipNode;

typedef struct _skiplist {
    size_t count;          // count of used key/value
    size_t max_count;      // max number of key/value allowed
    unsigned int level;          // current level
    unsigned int count_unused;   // count unused key/value that are holes
    unsigned int cum_key_length; // used to evaluate avg key len
    size_t wasted_bytes;     // how many bytes are unallocated and fragments
    size_t allocated;

#ifdef BACKGROUND_MERGE
    pthread_mutex_t lock;
    int refcount;
#endif

    // the data structure
    SkipNode* hdr;
    Arena* arena;
} SkipList;

#define STATUS_OK         0
#define STATUS_OK_DEALLOC 1

SkipList* skiplist_new(size_t size);
int skiplist_insert(SkipList* self, const char *key, size_t klen, OPT opt, char *data);
SkipNode* skiplist_lookup(SkipList* self, char* key, size_t klen);
SkipNode* skiplist_lookup_prev(SkipList* self, char* key, size_t klen);


void skiplist_free(SkipList* self);

SkipNode* skiplist_first(SkipList* self);
SkipNode* skiplist_last(SkipList* self);

void skiplist_acquire(SkipList* self);
void skiplist_release(SkipList* self);


#endif
