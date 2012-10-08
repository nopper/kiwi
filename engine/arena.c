#include <string.h>
#include "arena.h"
#include "indexer.h"

static Pool* pool_new(void)
{
    unsigned int p_size = POOL_SIZE - sizeof(Pool);
    Pool* self = calloc(1, sizeof(Pool) + p_size);

    if (!self)
        PANIC("NULL allocation");

    self->memory = (char*)(self + 1);
    self->remaining = POOL_SIZE - sizeof(Arena);

    return self;
}

static void pool_free(Pool* self)
{
    Pool* next;
    while (self->next != NULL) {
        next = self->next;
        free(self);
        self = next;
    }
    free(self);
}

Arena* arena_new(void)
{
    Arena* self = calloc(1, sizeof(Arena));

    if (!self)
        PANIC("NULL allocation");

    self->pool = pool_new();
    return self;
}

void arena_free(Arena* self)
{
    pool_free(self->pool);
    free(self);
}

void* arena_alloc(Arena* self, size_t size)
{
    void *ptr;
    Pool* pool;

    if (self->pool->remaining < size) {
        pool = pool_new();
        pool->next = self->pool;
        self->pool = pool;
        self->pools++;
    }

    ptr = self->pool->memory;
    self->pool->memory += size;
    self->pool->remaining -= size;
    self->allocated += size;

    return ptr;
}

void* arena_realloc(Arena* self, void* oldptr, size_t size)
{
    Pool* pool;
    size_t original_size = (char *)self->pool->memory - (char *)oldptr;
    size_t diff = size - original_size;

    if (self->pool->remaining < diff)
    {
        pool = pool_new();
        pool->next = self->pool;
        self->pool = pool;
        self->pools++;

        memcpy(self->pool->memory, oldptr, original_size);

        self->pool->memory += size;
        self->pool->remaining -= size;
        self->allocated += size;

        return self->pool->memory;
    }

    self->pool->memory += diff;
    self->pool->remaining -= diff;
    self->allocated += diff;

    return oldptr;
}

void arena_dealloc(Arena* self, size_t size)
{
    self->pool->remaining += size;
    self->pool->memory -= size;
    self->allocated -= size;
}
