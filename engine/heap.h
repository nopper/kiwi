#ifndef __HEAP_H__
#define __HEAP_H__

#include <stdint.h>
typedef int (*comparator)(const void *, const void *);

typedef struct _heap {
    void** data;
    uint32_t allocated;
    uint32_t length;
    comparator comp;
} Heap;

Heap* heap_new(uint32_t size, comparator cb);
void heap_free(Heap* self);

int heap_pop(Heap* self, void **data);
int heap_insert(Heap* self, void *data);

#endif
