#ifndef __COMPACTION_H__
#define __COMPACTION_H__

#include "variant.h"
#include "vector.h"
#include "sst.h"
#include "sst_builder.h"
#include "merger.h"

struct _compaction {
    int level;

    Vector* outputs; // SSTMetadata**

    FileRange* current_range;
    FileRange* parent_range;
    FileRange* grandparent_range;

    // The following pointers keeps track of the current output file being
    // created.
    File* file;
    SSTBuilder* builder;
    SSTMetadata* meta;

    uint32_t overlap_index;
    uint64_t overlap_bytes;

    SST* sst;
};

typedef struct _compaction Compaction;

Compaction* compaction_new(SST* sst, int level);
void compaction_free(Compaction* self);
void compaction_install(Compaction* self);
int compaction_new_output_file(Compaction* self);
int compaction_exceeds_overlap(Compaction* self, Variant* key);
int compaction_is_base_level_for(Compaction* self, Variant* key);


#endif
