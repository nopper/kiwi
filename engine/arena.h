#ifndef __ARENA_H__
#define __ARENA_H__

#include <stdlib.h>
#include <stdint.h>
//#include <jemalloc/jemalloc.h>

typedef struct _pool {
    struct _pool* next;
    char* memory;
    size_t remaining;
} Pool;

typedef struct _arena {
    uint32_t pools;
    uint64_t allocated;
    Pool* pool;
} Arena;

Arena* arena_new(void);
void arena_free(Arena* self);
void* arena_alloc(Arena* self, size_t size);
void* arena_realloc(Arena* self, void* oldptr, size_t size);
void arena_dealloc(Arena* self, size_t size);

#endif
