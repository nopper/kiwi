#ifndef __SST_H__
#define __SST_H__

#include "indexer.h"
#include "skiplist.h"
#include "sst_loader.h"
#include "sst_builder.h"
#include "variant.h"
#include "vector.h"
#include "file.h"
#include "lru.h"

/*
 * We organize the entire SST in directories. The basedir just
 * serves us as a container. We will create a first subdirectory
 * called si/ and then inside it various directories named with
 * the actual level name. For example:
 *   $basedir/si/0/0.sst
 *   $basedir/si/0/1.sst
 *   $basedir/si/1/2.sst
 *   $basedir/si/0.log
 */

typedef struct _sst_metadata {
    uint32_t filenum;
    uint32_t level;
    uint64_t filesize;

    Variant* smallest_key;
    Variant* largest_key;
    SSTLoader* loader;
} SSTMetadata;

SSTMetadata* sst_metadata_new(uint32_t level, uint32_t filenum);
void sst_metadata_free(SSTMetadata* self);

typedef struct _sst {
    char basedir[MAX_FILENAME];

    unsigned under_compaction:1;

    uint32_t last_id;
    uint32_t file_count;
    File* manifest;

    int comp_level;
    double comp_score;

    Vector* targets;
    LRU* cache;

    // Files in level 0 may overlap regarding ranges, while in upper levels
    // this is not allowed
    uint32_t num_files[MAX_LEVELS];
    SSTMetadata** files[MAX_LEVELS];
} SST;

SST* sst_new(const char* basedir);
void sst_free(SST* self);

void sst_merge(SST* self, SkipList* list);
void sst_compact(SST* self);
int sst_file_new(SST* self, uint32_t level, File** file, SSTBuilder** builder, SSTMetadata** meta);
void sst_file_add(SST* self, SSTMetadata* meta);
void sst_file_delete(SST* self, uint32_t level, uint32_t count, SSTMetadata** files);

int sst_get(SST* self, Variant* key, Variant* value);
int sst_find_file(SST* self, uint32_t level, Variant* smallest);
uint32_t sst_pick_level_for_compaction(SST* self, Variant* start, Variant* stop);
int sst_get_overlapping_inputs(SST* self, uint32_t level, Variant* begin, Variant* end, Vector* inputs, Variant** pbegin, Variant** pend);


#endif
