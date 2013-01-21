#include <stdio.h>
#include "compaction.h"
#include "utils.h"

void compaction_free(Compaction* self)
{
    vector_free(self->outputs);
    file_range_free(self->current_range);
    if (self->parent_range) file_range_free(self->parent_range);
    if (self->grandparent_range) file_range_free(self->grandparent_range);
    free(self);
}

Compaction* compaction_new(SST *sst, int level)
{
    if (!(level + 1 < MAX_LEVELS))
        return NULL;

    Compaction* self = calloc(1, sizeof(Compaction));

    FileRange* current;
    FileRange* parents;
    FileRange* missing = NULL;

    Variant* smallest;
    Variant* largest;

    self->sst = sst;
    self->level = level;
    self->outputs = vector_new();

    self->overlap_bytes = 0;
    self->overlap_index = 0;

    current = self->current_range = file_range_new(level);
    parents = self->parent_range = file_range_new(level + 1);
    self->grandparent_range = file_range_new(level + 2);

    current = self->current_range;

    SSTMetadata* meta = *self->sst->files[level];

    smallest = current->smallest_key = meta->smallest_key;
    largest = current->largest_key = meta->largest_key;

    vector_add(current->files, meta);

    if (level == 0)
    {
//        smallest = current->smallest_key = meta->smallest_key;
//        largest = current->largest_key = (self->sst->files[level][self->sst->num_files[0] - 1])->smallest_key;

        sst_get_overlapping_inputs(self->sst, level,
                                   meta->smallest_key, meta->largest_key,
                                   current->files,
                                   &current->smallest_key,
                                   &current->largest_key);
    }

    sst_get_overlapping_inputs(self->sst, level + 1,
                               current->smallest_key,
                               current->largest_key,
                               parents->files,
                               &parents->smallest_key,
                               &parents->largest_key);

    file_range_debug(current, "current");
    file_range_debug(parents, "parent");

    if (vector_count(parents->files) > 0)
    {
        // Check if by adding overlapping files at level+1 we actually miss some
        // files at level that were not selected before

        missing = file_range_new(level);

        if (variant_cmp(current->smallest_key, parents->smallest_key) < 0)
            smallest = missing->smallest_key = current->smallest_key;
        else
            smallest = missing->smallest_key = parents->smallest_key;

        if (variant_cmp(current->largest_key, parents->largest_key) > 0)
            largest = missing->largest_key = current->largest_key;
        else
            largest = missing->largest_key = parents->largest_key;

        sst_get_overlapping_inputs(self->sst, level,
                                   missing->smallest_key,
                                   missing->largest_key,
                                   missing->files,
                                   &missing->smallest_key,
                                   &missing->largest_key);

        file_range_debug(missing, "missing");

        uint64_t current_size = file_range_size(current);
        uint64_t parents_size = file_range_size(parents);
        uint64_t missing_size = file_range_size(missing);

        if ((vector_count(current->files) < vector_count(missing->files)) &&
            (missing_size + parents_size < EXPANSION_LIMIT))
        {
            if (sst_get_overlapping_inputs(self->sst, level + 1,
                                           missing->smallest_key,
                                           missing->largest_key,
                                           NULL, NULL, NULL) == \
                vector_count(parents->files))
            {
                INFO("Expanding %d+%d files (%"PRIu64"+%"PRIu64" bytes) to "
                     "%d+%d files (%ld+%ld bytes) in output level %d",
                     vector_count(self->current_range->files),
                     vector_count(self->parent_range->files),
                     current_size, parents_size,
                     vector_count(missing->files),
                     vector_count(self->parent_range->files),
                     missing_size, parents_size, level);

                file_range_free(self->current_range);
                self->current_range = missing;

                missing = NULL;
            }
        }
    }

    if (missing)
    {
        file_range_free(self->current_range);
        self->current_range = missing;
        missing = NULL;
    }

    if (level + 2 < MAX_LEVELS)
    {
        sst_get_overlapping_inputs(self->sst, level + 1,
                                   smallest, largest,
                                   self->grandparent_range->files,
                                   &self->grandparent_range->smallest_key,
                                   &self->grandparent_range->largest_key);

        // After finding the overlapping parents we also want to find the first
        // culprit that overlaps with the first grandparent. Mark it in order
        // to speed up various checks during the merge.

        int curr_found = 0, par_found = 0;
        for (uint32_t i = 0; i < vector_count(self->grandparent_range->files); i++)
        {
            SSTMetadata* b = *((SSTMetadata **)vector_data(self->grandparent_range->files) + i);

            for (uint32_t j = 0; j < vector_count(self->current_range->files) && !curr_found; j++)
            {
                SSTMetadata* a = *((SSTMetadata **)vector_data(self->current_range->files) + j);

                if (range_intersects(a->smallest_key, b->smallest_key, a->largest_key, b->largest_key))
                {
                    self->current_range->overlaps_from = j;
                    curr_found = 1;
                    break;
                }
            }

            for (uint32_t j = 0; i < vector_count(self->parent_range->files) && !par_found; j++)
            {
                SSTMetadata* a = *((SSTMetadata **)vector_data(self->parent_range->files) + j);

                if (range_intersects(a->smallest_key, b->smallest_key, a->largest_key, b->largest_key))
                {
                    self->parent_range->overlaps_from = j;
                    par_found = 1;
                    break;
                }
            }

            if (curr_found && par_found)
                break;
        }
    }

    file_range_debug(self->current_range, "final current");
    file_range_debug(self->parent_range, "final parent");

    if (vector_count(self->current_range->files) == 1 &&
        vector_count(self->parent_range->files) == 0 &&
        file_range_size(self->grandparent_range) <= GRANDPARENT_OVERLAP)
    {
//#ifdef BACKGROUND_MERGE
//        pthread_mutex_lock(&self->sst->lock);
//#endif

        SSTMetadata* old_meta = (SSTMetadata*)vector_get(self->current_range->files, 0);

        uint32_t new_level = old_meta->level + 1, new_filenum = old_meta->filenum;

        File* file = sst_filename_new(self->sst, new_level, new_filenum);

        INFO("Moving %s to %s", old_meta->loader->file->filename, file->filename);

        rename(old_meta->loader->file->filename, file->filename);

        SSTMetadata* new_meta = sst_metadata_new(new_level, new_filenum);

        Buffer* a = new_meta->smallest_key;
        Buffer* b = new_meta->largest_key;

        new_meta->smallest_key = old_meta->smallest_key;
        new_meta->largest_key = old_meta->largest_key;
        old_meta->smallest_key = a;
        old_meta->largest_key = b;

        sst_file_delete(self->sst, old_meta->level, 1, (SSTMetadata**)vector_data(self->current_range->files));

        new_meta->loader = sst_loader_new(self->sst->cache, file, new_level, new_filenum);
        new_meta->filesize = file_size(file);

        sst_file_add(self->sst, new_meta);

//#ifdef BACKGROUND_MERGE
//        pthread_mutex_unlock(&self->sst->lock);
//#endif

        compaction_free(self);
        return NULL;
    }

    INFO("Compacting %d+%d files (%" PRIu64 "+%" PRIu64 " bytes) in output level %d",
         vector_count(self->current_range->files),
         vector_count(self->parent_range->files),
         file_range_size(self->current_range),
         file_range_size(self->parent_range), level + 1);

    return self;
}

static void _compaction_close_pending(Compaction* self)
{
    if (self->file)
    {
        sst_builder_free(self->builder);
        file_close(self->file);

        // Now we need to create an sst loader and insert it in the right place
        // and we just reuse the file object we have
        self->meta->filesize = file_size(self->file);
        self->meta->loader = sst_loader_new(self->sst->cache, self->file, self->meta->level, self->meta->filenum);

        vector_add(self->outputs, (void**)self->meta);

        self->file = NULL;
        self->meta = NULL;
        self->builder = NULL;
    }
}

int compaction_new_output_file(Compaction* self)
{
    _compaction_close_pending(self);
    return sst_file_new(self->sst, self->level + 1, &self->file, &self->builder, &self->meta);
}

int compaction_exceeds_overlap(Compaction* self, Variant* key)
{
    SSTMetadata** files = (SSTMetadata**)vector_data(self->parent_range->files);

    while (self->overlap_index < vector_count(self->parent_range->files) &&
           variant_cmp(key, (*(files + self->overlap_index))->largest_key) > 0)
    {
        self->overlap_bytes += (*(files + self->overlap_index))->filesize;
        self->overlap_index++;
    }

    if (self->overlap_bytes > GRANDPARENT_OVERLAP)
    {
        self->overlap_bytes = 0;
        return 1;
    }

    return 0;
}

int compaction_is_base_level_for(Compaction* self, Variant* key)
{
    for (uint32_t level = self->level + 2; level < MAX_LEVELS; level++)
    {
        for (uint32_t i = 0; i < self->sst->num_files[level]; i++)
        {
            SSTMetadata* meta = *(self->sst->files[level] + i);
            if (variant_cmp(key, meta->largest_key) <= 0)
            {
                if (variant_cmp(key, meta->smallest_key) >= 0)
                    return 0;
                break;
            }
        }
    }
    return 1;
}

void compaction_install(Compaction* self)
{
    _compaction_close_pending(self);

#ifdef BACKGROUND_MERGE
    pthread_mutex_lock(&self->sst->lock);
#endif

    sst_file_delete(self->sst, self->current_range->level,
                    vector_count(self->current_range->files),
                    (SSTMetadata**)vector_data(self->current_range->files));

    sst_file_delete(self->sst, self->parent_range->level,
                    vector_count(self->parent_range->files),
                    (SSTMetadata**)vector_data(self->parent_range->files));

    for (uint32_t i = 0; i < vector_count(self->outputs); i++)
    {
        SSTMetadata* meta = (SSTMetadata*)vector_get(self->outputs, i);
        INFO("Installing file %s (%ld bytes) to level %d",
             meta->loader->file->filename, meta->filesize, meta->level);
        INFO("Smallest: %.*s Largest: %.*s",
             meta->smallest_key->length, meta->smallest_key->mem,
             meta->largest_key->length, meta->largest_key->mem);
        sst_file_add(self->sst, meta);
    }

    // TODO: without actually writing the manifest at every add just write it
    // here at the end of the function

#ifdef BACKGROUND_MERGE
    pthread_mutex_unlock(&self->sst->lock);
#endif
}
