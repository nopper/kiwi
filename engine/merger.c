#include <assert.h>
#include <limits.h>
#include "compaction.h"
#include "merger.h"
#include "vector.h"
#include "utils.h"

FileRange* file_range_new(uint32_t level)
{
    if (!(level < MAX_LEVELS))
        return NULL;

    FileRange* self = calloc(1, sizeof(FileRange));
    self->files = vector_new();
    self->level = level;
    self->overlaps_from = UINT_MAX;
    return self;
}

void file_range_free(FileRange* self)
{
    vector_free(self->files);
    free(self);
}

void file_range_debug(FileRange* self, const char *pre)
{
    INFO("%s FileRange: %.*s %.*s", pre, self->smallest_key->length, self->smallest_key->mem, self->largest_key->length, self->largest_key->mem);
    for (uint32_t i = 0; i < vector_count(self->files); i++)
    {
        SSTMetadata* curr = ((SSTMetadata*)vector_get(self->files, i));
        INFO("\tFile %s [%.*s, %.*s]", curr->loader->file->filename,
             curr->smallest_key->length, curr->smallest_key->mem,
             curr->largest_key->length, curr->largest_key->mem);
    }
}

uint64_t file_range_size(FileRange* self)
{
    uint64_t size = 0;
    for (uint32_t i = 0; i < vector_count(self->files); i++)
        size += ((SSTMetadata*)vector_get(self->files, i))->filesize;
    return size;
}

int chained_iterator_comp(ChainedIterator* a, ChainedIterator* b)
{
    assert(a->current->valid && b->current->valid);

    int ret = string_cmp(a->current->key->mem, b->current->key->mem,
                         a->current->key->length, b->current->key->length);

    // If equals sort by level
    if (ret == 0)
    {
        ret = a->current->loader->level - b->current->loader->level;

        if (ret == 0)
            ret = b->current->loader->filenum - a->current->loader->filenum;

        if (ret < 0)
            b->skip = 1;
        else
            a->skip = 1;
    }

    return ret;
}

void chained_iterator_init(ChainedIterator* iterator, FileRange* inputs)
{
    iterator->files = (SSTMetadata**)vector_data(inputs->files);
    iterator->num_files = vector_count(inputs->files);
    iterator->pos = 0;
    iterator->skip = 0;
    iterator->overlaps_from = inputs->overlaps_from;
    iterator->current = sst_loader_iterator((*(iterator->files + iterator->pos++))->loader);
}

ChainedIterator* chained_iterator_new(uint32_t num_files, SSTMetadata** files)
{
    ChainedIterator* iterator = calloc(1, sizeof(ChainedIterator));
    iterator->files = files;
    iterator->num_files = num_files;
    iterator->current = sst_loader_iterator((*(iterator->files + iterator->pos++))->loader);
    return iterator;
}

ChainedIterator* chained_iterator_new_seek(uint32_t num_files, SSTMetadata** files, Variant* key)
{
    ChainedIterator* iterator = calloc(1, sizeof(ChainedIterator));
    iterator->files = files;
    iterator->num_files = num_files;
    iterator->current = sst_loader_iterator_seek((*(iterator->files + iterator->pos++))->loader, key);

    DEBUG("Creating a chained iterator of %d files (seek: %.*s)", num_files, key->length, key->mem);
    for (int i = 0; i < num_files; i++)
        DEBUG("  => %d", files[i]->filenum);
    return iterator;
}

void chained_iterator_free(ChainedIterator* iterator)
{
    free(iterator->files);
    free(iterator);
}

void merge_iterator_free(MergeIterator *self)
{
    ChainedIterator* curr = self->iterators;

    while (curr < self->iterators + self->minheap->allocated)
    {
        sst_loader_iterator_free(curr->current);
        curr++;
    }

    free(self->iterators);
    heap_free(self->minheap);
    free(self);
}

MergeIterator* merge_iterator_new(struct _compaction* comp)
{
    MergeIterator* self = malloc(sizeof(MergeIterator));

    self->overlap_check = 0;

    FileRange* inputs1 = comp->current_range;
    FileRange* inputs2 = comp->parent_range;

    self->compaction = comp;
    self->current = NULL;
    self->valid = 0;

    uint32_t len2 = vector_count(inputs2->files);

    // Just count 1 for inputs2 since its level must be > 0. Therefore all
    // the ranges are disjoints and one chained iterator can be used
    size_t num_inputs = 0;

    if (inputs1->level == 0)
        num_inputs += vector_count(inputs1->files);
    else
        num_inputs += 1;

    if (len2 > 0)
        num_inputs += 1;

    ChainedIterator* curr;
    curr = self->iterators = calloc(num_inputs, sizeof(ChainedIterator));

    if (len2 > 0)
        chained_iterator_init(curr++, inputs2);

    if (inputs1->level == 0)
    {
        uint32_t i = 0;
        while (curr < self->iterators + num_inputs)
        {
            curr->files = (SSTMetadata**)vector_data(inputs1->files) + i;
            curr->num_files = 1;
            curr->pos = 0;
            curr->skip = 0;

            if (i >= inputs1->overlaps_from)
                curr->overlaps_from = 0;
            else
                curr->overlaps_from = UINT_MAX;

            curr->current = sst_loader_iterator((*(curr->files + curr->pos++))->loader);
            curr++;
            i++;
        }
    }
    else
        chained_iterator_init(curr++, inputs1);

    self->minheap = heap_new(num_inputs, (comparator)chained_iterator_comp);

    for (uint32_t i = 0; i < num_inputs; i++)
    {
        INFO("Inputs %d for merge %s", i, (self->iterators + i)->current->loader->file->filename);
        heap_insert(self->minheap, self->iterators + i);
    }

    merge_iterator_next(self);
    return self;
}

void merge_iterator_next(MergeIterator* self)
{
    ChainedIterator* iter;

start:

    // Let's advance our stalled current chained iterator
    if (self->current != NULL)
    {
        iter = self->current;
        sst_loader_iterator_next(iter->current);

        if (iter->current->valid)
        {
            iter->skip = 0;
            heap_insert(self->minheap, iter);
        }
        else
        {
            // Let's see if we can go on with the chained iterator
            if (iter->pos < iter->num_files)
            {
                // TODO: Maybe a reinitialization would be better
                sst_loader_iterator_free(iter->current);
                iter->current = sst_loader_iterator((*(iter->files + iter->pos++))->loader);

                if (iter->pos >= iter->overlaps_from)
                    self->overlap_check = 1;

                assert(iter->current->valid);
                heap_insert(self->minheap, iter);
            }
        }
    }

    if (heap_pop(self->minheap, (void**)&iter))
    {
        assert(iter->current->valid);

        self->current = iter;
        self->valid = 1;

        if (iter->skip == 1)
            goto start;
    }
    else
        self->valid = 0;
}

int merge_iterator_exceeds_overlap(MergeIterator* self, Variant* key)
{
    assert(self->current);
    return (self->overlap_check &&
            compaction_exceeds_overlap(self->compaction, key));
}

int merge_iterator_valid(MergeIterator* self)
{
    return self->valid;
}

Variant* merge_iterator_key(MergeIterator* self)
{
    return self->current->current->key;
}

Variant* merge_iterator_value(MergeIterator* self)
{
    return self->current->current->value;
}

OPT merge_iterator_opt(MergeIterator* self)
{
    return self->current->current->opt;
}

