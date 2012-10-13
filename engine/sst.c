#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <inttypes.h>
#include <assert.h>
#include "sst.h"
#include "memtable.h"
#include "sst_builder.h"
#include "variant.h"
#include "file.h"
#include "utils.h"
#include "heap.h"
#include "vector.h"
#include "compaction.h"

static void _print_summary(SST* self)
{
    for (uint32_t level = 0; level < MAX_LEVELS; level++)
    {
        INFO("--- Level %d ---", level);

        for (uint32_t i = 0; i < self->num_files[level]; i++)
        {
            INFO("Metadata filenum:%d smallest: %.*s largest: %.*s",
                 (*(self->files[level] + i))->filenum,
                 (*(self->files[level] + i))->smallest_key->length,
                 (*(self->files[level] + i))->smallest_key->mem,
                 (*(self->files[level] + i))->largest_key->length,
                 (*(self->files[level] + i))->largest_key->mem);
        }
    }
}

static uint64_t _size_for_level(SST* self, uint32_t level)
{
    uint64_t size = 0;
    for (uint32_t i = 0; i < self->num_files[level]; i++)
        size += (self->files[level][i])->filesize;
    INFO("Total bytes in level %d = %" PRIu64, level, size);
    return size;
}

static double _max_size_for_level(uint32_t level)
{
    double result = 10 * 1048576.0;
    while (level > 1)
    {
        result *= 10;
        level--;
    }
    return result;
}

static void _evaluate_compaction(SST* self)
{
    int comp_level = -1;
    double comp_score = -1;

    for (uint32_t level = 0; level < MAX_LEVELS; level++)
    {
        double score;

        if (level == 0)
        {
            score = self->num_files[0] / MAX_FILES_LEVEL0;
        }
        else
        {
            score = (double)_size_for_level(self, level) / _max_size_for_level(level);
        }

        INFO("Score for level %d is %.3f", level, (float)score);

        if (score > comp_score)
        {
            comp_score = score;
            comp_level = level;
        }
    }

    self->comp_score = comp_score;
    self->comp_level = comp_level;
}

static void _schedule_compaction(SST* self)
{
    _evaluate_compaction(self);
    sst_compact(self);
}

static int _compare_by_latest(const SSTMetadata** a, const SSTMetadata** b)
{
    return (*b)->filenum - (*a)->filenum;
}

static int _cmp_metadata_by_smallest_key(const SSTMetadata** a, const SSTMetadata** b)
{
    return string_cmp((*a)->smallest_key->mem, (*b)->smallest_key->mem,
                      (*a)->smallest_key->length, (*b)->smallest_key->length);
}

static void _sort_files(SST* self)
{
    for (uint32_t level = 0; level < MAX_LEVELS; level++)
    {
        qsort(self->files[level], self->num_files[level],
              sizeof(SSTMetadata**),
              (__compar_fn_t)_cmp_metadata_by_smallest_key);
    }

    _print_summary(self);
}

static int _write_manifest(SST* self)
{
    if (!writable_file_new(self->manifest))
    {
        ERROR("Unable to open manifest file %s for writing", self->manifest->filename);
        return 0;
    }

    SSTMetadata* meta;
    Buffer* buff = buffer_new(1024);

    buffer_putvarint32(buff, self->last_id);

    for (uint32_t level = 0; level < MAX_LEVELS; level++)
    {
        buffer_putvarint32(buff, self->num_files[level]);

        for (uint32_t i = 0; i < self->num_files[level]; i++)
        {
            meta = *(self->files[level] + i);
            INFO("Manifest: %d", meta->filenum);
            buffer_putvarint32(buff, meta->filenum);

            buffer_putvarint32(buff, meta->smallest_key->length);
            buffer_putnstr(buff, meta->smallest_key->mem, meta->smallest_key->length);

            buffer_putvarint32(buff, meta->largest_key->length);
            buffer_putnstr(buff, meta->largest_key->mem, meta->largest_key->length);
        }
    }

    file_append(self->manifest, buff);
    file_close(self->manifest);
    buffer_free(buff);

    return 1;
}

static int _read_manifest(SST* self)
{
    // Now let's read the manifest file containing all
    // the information regarding all the files at various levels
    self->manifest = file_new();
    snprintf(self->manifest->filename, MAX_FILENAME, "%s/manifest", self->basedir);

    if (!file_exists(self->manifest))
    {
        INFO("Manifest file not present");
        return 1;
    }

    if (!mmapped_file_new(self->manifest))
    {
        ERROR("Unable to open manifest file for reading");
        return 0;
    }

    // Let's read the manifest file
    const char *start = self->manifest->base;

    File* file;
    SSTMetadata* meta;
    uint32_t curr_level = 0, curr_num = 0;

    start = get_varint32(start, start + 5, &self->last_id);

    while (curr_level < MAX_LEVELS && start < self->manifest->limit)
    {
        start = get_varint32(start, start + 5, &self->num_files[curr_level]);
        self->files[curr_level] = malloc(sizeof(SSTMetadata*) * self->num_files[curr_level]);

        if (!self->files[curr_level])
            PANIC("Unable to allocate enough memory to hold %d files in level %d",
                  self->num_files[curr_level], curr_level);

        for (uint32_t i = 0; i < self->num_files[curr_level] && start < self->manifest->limit; i++)
        {
            start = get_varint32(start, start + 5, &curr_num);

            file = file_new();
            snprintf(file->filename, MAX_FILENAME, "%s/%d/%d.sst", self->basedir, curr_level, curr_num);

            meta = sst_metadata_new(curr_level, curr_num);
            uint32_t len = 0;

            start = get_varint32(start, start + 5, &len);
            buffer_putnstr(meta->smallest_key, start, len);
            start += len;

            start = get_varint32(start, start + 5, &len);
            buffer_putnstr(meta->largest_key, start, len);
            start += len;

            INFO("Loading SST file %s for level %d %ld bytets", file->filename, curr_level, file_size(file));
            INFO("Smallest key: %.*s Largest key: %.*s",
                 meta->smallest_key->length, meta->smallest_key->mem,
                 meta->largest_key->length, meta->largest_key->mem);

            meta->loader = sst_loader_new(file, curr_level, curr_num);
            meta->filesize = file_size(file);

            if (meta->loader)
            {
                self->files[curr_level][i] = meta;
                self->file_count++;
            }
            else
            {
                self->num_files[curr_level]--;
                i--;
                sst_metadata_free(meta);
            }


        }

        curr_level++;
    }

    file_close(self->manifest);
    _sort_files(self);
    _schedule_compaction(self);

    return 1;
}

SST* sst_new(const char* basedir)
{
    SST* self = (SST*)malloc(sizeof(SST));

    strncpy(self->basedir, basedir, sizeof(self->basedir));
    strncat(self->basedir, "/si", MAX_FILENAME);
    mkdirp(self->basedir);

    self->file_count = 0;
    self->last_id = 0;
    self->under_compaction = 0;
    self->targets = vector_new(); // Used to speed up the get

    for (uint32_t i = 0; i < MAX_LEVELS; i++)
    {
        self->files[i] = NULL;
        self->num_files[i] = 0;
    }

    _read_manifest(self);

    return self;
}

void sst_free(SST* self)
{
    _write_manifest(self);

    for (uint32_t i = 0; i < MAX_LEVELS; i++)
    {
        if (!self->files[i])
            continue;

        for (uint32_t j = 0; j < self->num_files[i]; j++)
            sst_metadata_free(self->files[i][j]);

        free(self->files[i]);
    }

    vector_free(self->targets);
    free(self);
}

static void _sst_file_delete(uint32_t tlen, uint32_t len, SSTMetadata** targets, SSTMetadata** arr)
{
    int i = 0, dst = 0, src = 0;

    while (i < tlen && src < len)
    {
        SSTMetadata* target = *(targets + i);
        SSTMetadata* current = *(arr + dst);

        if (target == current)
        {
            src++;
            i++;
        }
        else
        {
            dst++;
            src++;
        }

        arr[dst] = arr[src];
    }

    while (src < len)
        arr[dst++] = arr[src++];

    // Just to get a clean crash! Trust me I am not an engineer
    while (dst < len)
        arr[dst++] = NULL;
}

void sst_file_delete(SST* self, uint32_t level, uint32_t count, SSTMetadata** files)
{
    assert(level < MAX_LEVELS);

    _sst_file_delete(count, self->num_files[level], files, self->files[level]);
    self->files[level] = realloc(self->files[level], sizeof(SSTMetadata*) * self->num_files[level]);

    self->num_files[level] -= count;
    self->file_count -= count;

    for (int i = 0; i < count; i++)
    {
        SSTMetadata* meta = *(files + i);
        INFO("Deleting %s", meta->loader->file->filename);
        sst_metadata_free(meta);
    }
}

void sst_file_add(SST* self, SSTMetadata* meta)
{
    self->file_count++;
    self->files[meta->level] = realloc(self->files[meta->level], sizeof(SSTMetadata*) * (self->num_files[meta->level] + 1));
    *(self->files[meta->level] + self->num_files[meta->level]++) = meta;

    _write_manifest(self);

    _sort_files(self);
    _schedule_compaction(self);
}

int sst_file_new(SST* self, uint32_t level, File** file, SSTBuilder** builder, SSTMetadata** meta)
{
    uint32_t filenum = self->last_id++;

    File* file_ = file_new();

    snprintf(file_->filename, MAX_FILENAME, "%s/%d", self->basedir, level);
    mkdirp(file_->filename);

    snprintf(file_->filename, MAX_FILENAME, "%s/%d/%d.sst", self->basedir, level, filenum);

    if (!writable_file_new(file_))
    {
        ERROR("Unable to open file %s for writing", file_->filename);

        file_free(file_);
        return 0;
    }

    *file = file_;
    *builder = sst_builder_new(file_);
    *meta = sst_metadata_new(level, filenum);

    return 1;
}

static void _sst_merge_into(SkipNode* node, size_t count, SSTMetadata* meta, File* file, SSTBuilder* builder)
{
    OPT opt;
    Variant* key = buffer_new(1024);
    Variant* value = buffer_new(1024);

    for (int i = 0; i < count; i++)
    {
        memtable_extract_node(node, key, value, &opt);

        if (i == 0)
            buffer_putnstr(meta->smallest_key, key->mem, key->length);

        if (i == count - 1)
            buffer_putnstr(meta->largest_key, key->mem, key->length);

        sst_builder_add(builder, key, value);
        node = NODE_FWD(node, 0);
    }

    sst_builder_free(builder);
    file_close(file);

    // Now we need to create an sst loader and insert it in the right place
    // and we just reuse the file object we have
    meta->filesize = file_size(file);
    meta->loader = sst_loader_new(file, meta->level, meta->filenum);
}

void sst_merge(SST* self, SkipList* list)
{
    INFO("Compacting the memtable to a SST file");

    File* file;
    SSTBuilder* builder;
    SSTMetadata* meta;
    uint32_t level = 0;

    SkipNode* first = skiplist_first(list);
    SkipNode* last = skiplist_last(list);

    OPT opt;
    Variant* smallest = buffer_new(1);
    Variant* largest = buffer_new(1);

    memtable_extract_node(first, smallest, NULL, &opt);
    memtable_extract_node(last, largest, NULL, &opt);

    level = sst_pick_level_for_compaction(self, smallest, largest);

    buffer_free(smallest);
    buffer_free(largest);

    if (!sst_file_new(self, level, &file, &builder, &meta))
    {
        ERROR("Unable to compact memtable");
        return;
    }

    INFO("Compaction of %d elements started", list->count);
    _sst_merge_into(first, list->count, meta, file, builder);
    sst_file_add(self, meta);
    INFO("Compaction of %d elements finished", list->count);
}

int sst_get(SST* self, Variant* key, Variant* value)
{
    vector_clear(self->targets);

    for (int level = 0; level < MAX_LEVELS; level++)
    {
        if (self->num_files[level] == 0)
            continue;

        if (level == 0)
        {
            for (uint32_t i = 0; i < self->num_files[level]; i++)
            {
                if (variant_cmp(key, self->files[level][i]->smallest_key) >= 0 &&
                    variant_cmp(key, self->files[level][i]->largest_key) <= 0)
                    vector_add(self->targets, self->files[level][i]);
            }

            qsort(vector_data(self->targets),
                  vector_count(self->targets),
                  sizeof(SSTMetadata**), (__compar_fn_t)_compare_by_latest);
        }
        else
        {
            uint32_t start = sst_find_file(self, level, key);

            if (start >= self->num_files[level] ||
                variant_cmp(key, self->files[level][start]->smallest_key) < 0)
                continue;

            vector_add(self->targets, self->files[level][start]);
        }


        for (uint32_t i = 0; i < vector_count(self->targets); i++)
        {
//            DEBUG("Looking for key %.*s inside %s", key->length, key->mem, ((SSTMetadata*)vector_get(self->targets, i))->loader->file->filename);

            if (sst_loader_get(((SSTMetadata*)vector_get(self->targets, i))->loader, key, value) == 1)
                return 1;
        }
    }

    return 0;
}

SSTMetadata* sst_metadata_new(uint32_t level, uint32_t filenum)
{
    SSTMetadata* self = malloc(sizeof(SSTMetadata));
    self->level = level;
    self->filenum = filenum;
    self->smallest_key = buffer_new(1);
    self->largest_key  = buffer_new(1);
    self->loader = NULL;
    return self;
}

void sst_metadata_free(SSTMetadata* self)
{
    buffer_free(self->smallest_key);
    buffer_free(self->largest_key);

    if (self->loader)
        sst_loader_free(self->loader);

    free(self);
}

int sst_get_overlapping_inputs(SST* self, uint32_t level, Variant* begin, Variant* end, Vector* inputs, Variant** pbegin, Variant** pend)
{
    int additions = 0;
    int first_set = 0;

    if (inputs != NULL)
        vector_clear(inputs);

    for (int i = 0; i < self->num_files[level]; i++)
    {
        SSTMetadata* target = *(self->files[level] + i);

        INFO("Range: %.*s End: %.*s", begin->length, begin->mem, end->length, end->mem);
        INFO("File : %.*s end: %.*s", target->smallest_key->length, target->smallest_key->mem, target->largest_key->length, target->largest_key->mem);

        if (!begin || !end || !range_intersects(begin, target->smallest_key, end, target->largest_key))
            continue;

        if (level == 0)
        {
            if (!first_set && begin && string_cmp(target->smallest_key->mem, begin->mem, target->smallest_key->length, begin->length) < 0)
            {
                first_set = 1; // Can grow back only once since these sequences are sorted
                begin = target->smallest_key;
            }
            if (end && string_cmp(target->largest_key->mem, end->mem, target->largest_key->length, end->length) > 0)
                end = target->largest_key;
        }

        INFO("      Overlap!");
        additions++;

        if (inputs != NULL)
            vector_add(inputs, target);
    }

    if (pbegin)
        *pbegin = begin;
    if (pend)
        *pend = end;

    return additions;
}

int sst_find_file(SST* self, uint32_t level, Variant* smallest)
{
    uint32_t left = 0;
    uint32_t right = self->num_files[level];

    while (left < right)
    {
        uint32_t mid = (left + right) / 2;
        SSTMetadata* meta = *(self->files[level] + mid);
        if (variant_cmp(meta->largest_key, smallest) < 0)
            left = mid + 1;
        else
            right = mid;
    }

    return right;
}

int sst_range_overlaps(SST* self, uint32_t level, Variant* start, Variant* stop)
{
    SSTMetadata* curr;

    if (level == 0)
    {
        for (uint32_t i = 0; i < self->num_files[level]; i++)
        {
            curr = *(self->files[level] + i);
            if (range_intersects(start, curr->smallest_key, stop, curr->largest_key))
                return 1;
        }

        return 0;
    }

    uint32_t pos = sst_find_file(self, level, start);

    if (pos >= self->num_files[level])
        return 0;

    curr = *(self->files[level] + pos);
    return range_intersects(start, curr->smallest_key, stop, curr->largest_key);
}

uint32_t sst_pick_level_for_compaction(SST* self, Variant* start, Variant* stop)
{
    uint32_t level = 0;

    if (!sst_range_overlaps(self, level, start, stop))
    {
        uint64_t size = 0;
        Vector* inputs = vector_new();

        while (level < MAX_MEM_COMPACT_LEVEL)
        {
            if (sst_range_overlaps(self, level + 1, start, stop))
                break;

            sst_get_overlapping_inputs(self, level + 2, start, stop, inputs, NULL, NULL);

            for (uint32_t i = 0; i < vector_count(inputs); i++)
                size += ((SSTMetadata*)vector_get(inputs, i))->filesize;

            if (size > GRANDPARENT_OVERLAP)
                break;

            level++;
        }
    }

    INFO("Using level %d for memtable compaction", level);
    return level;
}

void sst_compact(SST* self)
{

    if (self->under_compaction)
        return;

    INFO("Starting compaction. Compaction level: %d Score: %f", self->comp_level, self->comp_score);
    Compaction* comp = NULL;

    if (self->comp_score >= 1)
        comp = compaction_new(self, self->comp_level);

    if (!comp)
        return;

    self->under_compaction = 1;
    int needs_reset = 0, drop = 0;

    Variant* key;
    Variant* value;
    MergeIterator* iter;

    for (iter = merge_iterator_new(comp);
         merge_iterator_valid(iter); merge_iterator_next(iter))
    {
        key = merge_iterator_key(iter);
        value = merge_iterator_value(iter);

        // Check to see if the actual key is a deletion mark
        if (value->length == 0 && compaction_is_base_level_for(comp, key))
        {
            drop = 1;
            continue;
        }

        drop = 0;

        if (!comp->builder || needs_reset)
        {
            needs_reset = 0;
            compaction_new_output_file(comp);

            buffer_clear(comp->meta->smallest_key);
            buffer_putnstr(comp->meta->smallest_key, key->mem, key->length);

            INFO("New output file for level %d: %s", comp->level, comp->file->filename);
        }
        else if (merge_iterator_exceeds_overlap(iter, key))
        {
            buffer_clear(comp->meta->largest_key);
            buffer_putnstr(comp->meta->largest_key, key->mem, key->length);
            needs_reset = 1;
        }

        sst_builder_add(comp->builder, key, value);
    }

    if (drop)
    {
        // If we are it means that the block was ended with a dropped key
        // You have to check out this to see if it safe
        key = comp->builder->data_block->last_key;
        WARN("SST file %s was ended by a dropped key.", comp->file->filename);
        WARN("Check out the the key %s is actually the last key of the file", key->length, key->mem);
    }

    buffer_clear(comp->meta->largest_key);
    buffer_putnstr(comp->meta->largest_key, key->mem, key->length);

    compaction_install(comp);
    merge_iterator_free(iter);
    compaction_free(comp);
}
