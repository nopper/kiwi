#include "vector.h"

Vector* vector_new(void)
{
    return calloc(1, sizeof(Vector));
}

size_t vector_count(Vector* self)
{
    return self->count;
}

void** vector_data(Vector *self)
{
    return self->data;
}

void vector_clear(Vector *self)
{
    self->count = 0;
}

void vector_add(Vector* self, void *data)
{
    if (self->size == 0)
    {
        self->size = 10;
        self->data = malloc(sizeof(void*) * self->size);
    }

    if (self->size == self->count)
    {
        self->size *= 2;
        self->data = realloc(self->data, sizeof(void*) * self->size);
    }

    self->data[self->count] = data;
    self->count++;
}

void vector_set(Vector* self, uint32_t pos, void* data)
{
    if (pos >= self->count)
        return;

    self->data[pos] = data;
}

void* vector_get(Vector* self, uint32_t pos)
{
    if (pos >= self->count)
        return NULL;

    return self->data[pos];
}

void vector_free(Vector* self)
{
    if (self->data)
        free(self->data);
    free(self);
}

void *vector_release(Vector *self)
{
    void* ptr = self->data;
    self->size = 10;
    self->data = malloc(sizeof(void*) * self->size);
    self->count = 0;
    return ptr;
}
