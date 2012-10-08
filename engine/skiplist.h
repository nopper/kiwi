#ifndef __SKIPLIST_H__
#define __SKIPLIST_H__

#include <stdint.h>
#include <stdlib.h>
#include "arena.h"
#include "config.h"

#define MAXLEVEL (15)
#define SKIPNODE_SIZE (sizeof(size_t))
// This macro is used to support non-fixed size key length

#define NODE_KEY(node)      ( (((unsigned char *)node) + sizeof(size_t)) )

#define MARK_DELETED 0x1
#define MARK_ADDED   0x0

#define NODE_FWD(node, idx) ( *((SkipNode **)((unsigned char *)&NODE_KEY(node)[node->klen + idx * sizeof(SkipNode*)])) )
#define NODE_FWD_SET(node, idx, ptr) ( *((unsigned char**)&NODE_KEY(node)[node->klen + idx * sizeof(SkipNode*)]) = (unsigned char *)ptr )

typedef enum {ADD,DEL} OPT;

/* The `mem` member actually contains:
 *  char key[];
 *  SkipNode* forward[];
 * All these magic attributes can be easilly accessed through the macros:
 *  NODE_KEY(node)
 *  NODE_FWD(node, idx)
 */
typedef struct _skipnode {
    size_t klen;
    //char mem[1];
} SkipNode;

typedef struct _skiplist {
    size_t count;          // count of used key/value
    size_t max_count;      // max number of key/value allowed
    unsigned int level;          // current level
    unsigned int count_unused;   // count unused key/value that are holes
    unsigned int cum_key_length; // used to evaluate avg key len
    size_t wasted_bytes;     // how many bytes are unallocated and fragments
    // the data structure

    SkipNode* last;
    size_t last_size;

    SkipNode* hdr;
    Arena* arena;
} SkipList;

#define NODE_FIRST(lst) NODE_FWD(lst->hdr, 0)
#define NODE_NEXT(node) node = NODE_FWD(node, 0)
#define NODE_END(lst) lst->hdr

#define STATUS_ERR        0 // Error: skiplist full
#define STATUS_OK         1 // Ok: inserted but do not free
#define STATUS_OK_DEALLOC 2 // Ok: you can free the arena

SkipList* skiplist_new(size_t size);

/* Insert-update or delete a previosly inserted element in the skiplist.
 *
 * Insert and delete operation are trivial:
 *  - Insert just allocate a new node and link the pointers accordingly
 *  - Delete just set the opt value of the node to DEL. This is needed to keep
 *    track of deleted keys in the memtable and to avoid unnecessary disk accesses.
 *
 * In case of update we have two different cases:
 *  - If the node->klen == old_node->klen an implace substitution is made
 *  - If the lengths differ the old element is unlinked but not deallocated from the
 *    arena. The new element is just appended at the end of the list and the pointers
 *    are updated.
 */
int skiplist_insert_last(SkipList* self, const char *key, size_t klen, int new_level, OPT opt);
SkipNode* skiplist_lookup(SkipList* self, char* key, size_t klen);

SkipNode* skiplist_new_node(SkipList* self, size_t klen, size_t *reserved, int* level);
void skiplist_free_node(SkipList* self, size_t reserved);

//SkipNode* skiplist_lookup(SkipList* self, char* data);
int skiplist_notfull(SkipList* self);
void skiplist_free(SkipList* self);

SkipNode* skiplist_first(SkipList* self);
SkipNode* skiplist_last(SkipList* self);


#endif
