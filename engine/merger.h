#ifndef __MERGER_H__
#define __MERGER_H__

#include "heap.h"
#include "sst.h"
#include "sst_loader.h"

typedef struct _file_range {
    Variant* smallest_key;
    Variant* largest_key;
    Vector* files;
    uint32_t level;
    uint32_t overlaps_from;
} FileRange;

FileRange* file_range_new(uint32_t level);
void file_range_free(FileRange* self);
void file_range_debug(FileRange* self, const char *pre);
uint64_t file_range_size(FileRange* self);

typedef struct _chained_iterator {
    uint32_t num_files;
    uint32_t pos;
    uint32_t overlaps_from;
    unsigned skip:1;
    SSTMetadata** files;
    SSTLoaderIterator* current;
} ChainedIterator;

int chained_iterator_comp(ChainedIterator* a, ChainedIterator* b);
ChainedIterator* chained_iterator_new(uint32_t num_files, SSTMetadata** files);
ChainedIterator* chained_iterator_new_seek(uint32_t num_files, SSTMetadata** files, Variant* key);
void chained_iterator_free(ChainedIterator* iterator);

struct _compaction;

typedef struct _merge_iterator {
    unsigned valid:1;
    unsigned overlap_check:1;
    Heap* minheap;
    ChainedIterator* iterators; //array of iterators
    ChainedIterator* current;
    struct _compaction* compaction;
} MergeIterator;

MergeIterator* merge_iterator_new(struct _compaction* comp);
void merge_iterator_free(MergeIterator* self);
void merge_iterator_next(MergeIterator* self);
int merge_iterator_valid(MergeIterator* self);
int merge_iterator_exceeds_overlap(MergeIterator* self, Variant *key);

Variant* merge_iterator_key(MergeIterator* self);
Variant* merge_iterator_value(MergeIterator* self);
OPT merge_iterator_opt(MergeIterator* self);

#endif
