#ifndef __VECTOR_H__
#define __VECTOR_H__

#include <stdlib.h>
#include <inttypes.h>

typedef struct vector_ {
    void** data;
    size_t size;
    size_t count;
} Vector;

Vector* vector_new(void);
void vector_free(Vector* self);
void* vector_release(Vector* self);
void vector_clear(Vector* self);
size_t vector_count(Vector* self);
void** vector_data(Vector* self);
void vector_add(Vector* self, void* data);
void *vector_get(Vector* self, uint32_t pos);
void vector_set(Vector* self, uint32_t pos, void* data);

#endif
