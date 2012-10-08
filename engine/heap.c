#include <stdlib.h>
#include "heap.h"

Heap* heap_new(uint32_t size, comparator cb)
{
    Heap* self = malloc(sizeof(Heap));
    self->data = malloc(sizeof(void**) * size);
    self->allocated = size;
    self->length = 0;
    self->comp = cb;
    return self;
}

void heap_free(Heap *self)
{
    free(self->data);
    free(self);
}

inline static uint32_t _parent(uint32_t index) { return (index - 1) >> 1; }

inline static void _swap(Heap* self, uint32_t a, uint32_t b)
{
    void* temp = self->data[a];
    self->data[a] = self->data[b];
    self->data[b] = temp;
}

static void _bubble_up(Heap* self, uint32_t index)
{
    while (index > 0 &&
           self->comp(self->data[_parent(index)], self->data[index]) > 0)
    {
        _swap(self, _parent(index), index);
        index = _parent(index);
    }
}

static void _bubble_down(Heap* self, uint32_t index)
{
    while (1)
    {
        uint32_t minpos = index;
        uint32_t left = (index << 1) + 1;
        uint32_t right = (index << 1) + 2;

        if (left < self->length && self->comp(self->data[left], self->data[minpos]) < 0)
            minpos = left;
        if (right < self->length && self->comp(self->data[right], self->data[minpos]) < 0)
            minpos = right;

        if (minpos == index)
            break;

        _swap(self, index, minpos);
        index = minpos;
    }
}

int heap_insert(Heap *self, void *data)
{
    if (self->length == self->allocated)
        return 0;

    self->data[self->length] = data;

    uint32_t index = self->length++;
    _bubble_up(self, index);
    return 1;
}

int heap_pop(Heap *self, void **data)
{
    if (self->length == 0)
        return 0;

    *data = self->data[0];
    _swap(self, 0, self->length - 1);

    --(self->length);
    _bubble_down(self, 0);

    return 1;
}
