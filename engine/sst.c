#define _BSD_SOURCE
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
    //INFO("Total bytes in level %d = %" PRIu64, level, size);
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

    for (int level = 0; level < MAX_LEVELS; level++)
    {
        double score;

        if (level == 0)
        {
            score = (double)self->num_files[0] / (double)MAX_FILES_LEVEL0;
            double size_score = (double)_size_for_level(self, 0) / _max_size_for_level(0);

            if (size_score > score)
            {
                //DEBUG("Using file size as score factor for level 0");
                score = size_score;
            }
        }
        else
            score = (double)_size_for_level(self, level) / _max_size_for_level(level);

        //DEBUG("Score for level %d is %.3f", level, (float)score);

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

    if (self->comp_score >= 1)
    {
#ifndef BACKGROUND_MERGE
        sst_compact(self);
#else
        pthread_mutex_lock(&self->cv_lock);
        self->merge_state |= MERGE_STATUS_COMPACT;
        pthread_cond_signal(&self->cv);
        pthread_mutex_unlock(&self->cv_lock);
#endif
    }
}

#ifdef BACKGROUND_MERGE
void sst_merge_real(SST* self, SkipList* list);

static void merge_thread(void* data)
{
    SST* sst = (SST*)data;

    while (1)
    {
        pthread_mutex_lock(&sst->cv_lock);

        while (sst->merge_state == 0)
            pthread_cond_wait(&sst->cv, &sst->cv_lock);

        if ((sst->merge_state & MERGE_STATUS_INPUT) == MERGE_STATUS_INPUT &&
            sst->immutable)
        {
            DEBUG("The merge thread received a MERGE job");
            INFO("Merging inside compaction thread");

            sst_merge_real(sst, sst->immutable_list);

            SkipList* toclean = sst->immutable_list;

            INFO("Merge successfully completed");
            INFO("Freeing up immutable skiplist");

            SkipNode* first = skiplist_first(toclean);

            for (int i = 0; i < toclean->count; i++)
            {
                free(first->data);
                first = first->forward[0];
            }

            skiplist_free(toclean);
        }

        if ((sst->merge_state & MERGE_STATUS_EXIT) == MERGE_STATUS_EXIT)
        {
            DEBUG("Exiting from the merge thread as user requested");
            pthread_mutex_unlock(&sst->cv_lock);
            pthread_exit(0);
        }

        if ((sst->merge_state & MERGE_STATUS_COMPACT) == MERGE_STATUS_COMPACT)
        {
            // We have already evaluated the score. Just execute the job
            DEBUG("The merge thread received a COMPACTION job");
            sst_compact(sst);
        }

        sst->immutable = NULL;
        sst->immutable_list = NULL;
        sst->merge_state = 0;

        pthread_mutex_unlock(&sst->cv_lock);
    }
}
#endif

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
              _cmp_metadata_by_smallest_key);
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
            buffer_putvarint32(buff, meta->filenum);

            buffer_putvarint32(buff, meta->smallest_key->length);
            buffer_putnstr(buff, meta->smallest_key->mem, meta->smallest_key->length);

            buffer_putvarint32(buff, meta->largest_key->length);
            buffer_putnstr(buff, meta->largest_key->mem, meta->largest_key->length);

            buffer_putvarint32(buff, meta->allowed_seeks);
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

            start = get_varint32(start, start + 5, &meta->allowed_seeks);

            INFO("Loading SST file %s for level %d %ld bytes", file->filename, curr_level, file_size(file));

            meta->loader = sst_loader_new(self->cache, file, curr_level, curr_num);
            meta->filesize = file_size(file);

            if (meta->allowed_seeks < 0)
                meta->allowed_seeks = meta->filesize / 16384;

            INFO("Smallest key: %.*s Largest key: %.*s seeks: %d",
                 meta->smallest_key->length, meta->smallest_key->mem,
                 meta->largest_key->length, meta->largest_key->mem,
                 meta->allowed_seeks);


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

    self->cache = lru_new(LRU_CACHE_SIZE);

    self->comp_level = -1;
    self->comp_score = -1;

    for (uint32_t i = 0; i < MAX_LEVELS; i++)
    {
        self->files[i] = NULL;
        self->num_files[i] = 0;
    }

    _read_manifest(self);

#ifdef BACKGROUND_MERGE
    self->merge_state = 0;
    self->immutable = NULL;
    self->immutable_list = NULL;

    pthread_mutex_init(&self->lock, NULL);
    pthread_mutex_init(&self->cv_lock, NULL);
    pthread_cond_init(&self->cv, NULL);

    pthread_create(&self->merge_thread, NULL, merge_thread, self);
#endif

    return self;
}

void sst_free(SST* self)
{
#ifdef BACKGROUND_MERGE
    INFO("Sending termination message to the detached thread");

    pthread_mutex_lock(&self->cv_lock);
    self->merge_state |= MERGE_STATUS_EXIT;
    pthread_cond_signal(&self->cv);
    pthread_mutex_unlock(&self->cv_lock);

    INFO("Waiting the merger thread");
    pthread_join(self->merge_thread, NULL);
#endif

    _write_manifest(self);

    if (self->manifest)
        file_free(self->manifest);

    for (uint32_t i = 0; i < MAX_LEVELS; i++)
    {
        if (!self->files[i])
            continue;

        for (uint32_t j = 0; j < self->num_files[i]; j++)
            sst_metadata_free(self->files[i][j]);

        free(self->files[i]);
    }

    vector_free(self->targets);
    lru_free(self->cache);
    free(self);
}

static void _sst_file_delete(uint32_t tlen, uint32_t len, SSTMetadata** targets, SSTMetadata** arr)
{
    int i = 0, dst = 0, src = 0;

    while (i < tlen && src < len)
    {
        SSTMetadata* target = targets[i];//*(targets + i);
        SSTMetadata* current = arr[dst];//*(arr + dst);

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
        unlink(meta->loader->file->filename);
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
#ifndef BACKGROUND_MERGE
    _schedule_compaction(self);
#endif
}

File* sst_filename_new(SST* self, uint32_t level, uint32_t filenum)
{
    File* file_ = file_new();
    snprintf(file_->filename, MAX_FILENAME, "%s/%d", self->basedir, level);
    mkdirp(file_->filename);
    snprintf(file_->filename, MAX_FILENAME, "%s/%d/%d.sst", self->basedir, level, filenum);
    return file_;
}

int sst_file_new(SST* self, uint32_t level, File** file, SSTBuilder** builder, SSTMetadata** meta)
{
    uint32_t filenum = self->last_id++;
    File* file_ = sst_filename_new(self, level, filenum);

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

static void _sst_merge_into(SST* self, SkipNode* node, SkipNode* last, size_t count, SSTMetadata* meta, File* file, SSTBuilder* builder)
{
    OPT opt;
    Variant* key = buffer_new(1024);
    Variant* value = buffer_new(1024);

    for (int i = 0; i < count/* && node != last*/; i++)
    {
        memtable_extract_node(node, key, value, &opt);

        if (i == 0)
            buffer_putnstr(meta->smallest_key, key->mem, key->length);

        if (i == count - 1)
            buffer_putnstr(meta->largest_key, key->mem, key->length);

        sst_builder_add(builder, key, value, opt);

#ifndef BACKGROUND_MERGE
        free(node->data);
#endif
        node = node->forward[0];
    }

    buffer_free(key);
    buffer_free(value);

    sst_builder_free(builder);
    file_close(file);

    // Now we need to create an sst loader and insert it in the right place
    // and we just reuse the file object we have
    meta->filesize = file_size(file);
    meta->loader = sst_loader_new(self->cache, file, meta->level, meta->filenum);
    meta->allowed_seeks = (meta->filesize / 16384);

    if (meta->allowed_seeks < 100)
        meta->allowed_seeks = 100;
}

void sst_merge(SST* self, MemTable* mem)
#ifdef BACKGROUND_MERGE
{
    pthread_mutex_lock(&self->cv_lock);

    while (self->merge_state != 0)
    {
        pthread_mutex_unlock(&self->cv_lock);
        usleep(1000);
        pthread_mutex_lock(&self->cv_lock);
    }

    // We need to get a reference to the skiplist and to the logfile
    self->immutable = mem;
    self->immutable_list = mem->list;

    self->merge_state |= MERGE_STATUS_INPUT;

    pthread_cond_signal(&self->cv);
    pthread_mutex_unlock(&self->cv_lock);
}

void sst_merge_real(SST* self, SkipList* list)
#endif
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
        PANIC("Unable to compact memtable");

    INFO("Compaction of %d [%d bytes allocated] elements started", list->count, list->allocated);
    _sst_merge_into(self, first, list->hdr, list->count, meta, file, builder);
    INFO("Compaction of %d elements finished", list->count);

#ifdef BACKGROUND_MERGE
    pthread_mutex_lock(&self->lock);
#endif

    // The lock must be held
    sst_file_add(self, meta);

    // At this point we can remove the old log since we have created the file
    log_remove(self->immutable->log, self->immutable->lsn - 1);

#ifdef BACKGROUND_MERGE
    pthread_mutex_unlock(&self->lock);
#endif
}

int sst_get(SST* self, Variant* key, Variant* value)
{
#ifdef BACKGROUND_MERGE
    int ret = 0;

    pthread_mutex_lock(&self->cv_lock);
    if (self->immutable)
    {
        DEBUG("Serving sst_get request from immutable memtable");
        ret = memtable_get(self->immutable_list, key, value);
    }
    pthread_mutex_unlock(&self->cv_lock);

    if (ret)
        return ret;

    pthread_mutex_lock(&self->lock);
#endif

    vector_clear(self->targets);

    for (int level = 0; level < MAX_LEVELS; level++)
    {
        if (self->num_files[level] == 0)
            continue;

        if (level == 0)
        {
            for (uint32_t i = 0; i < self->num_files[level]; i++)
            {
//                INFO("Compare %.*s %.*s = %d", key->length, key->mem, self->files[level][i]->smallest_key->length, self->files[level][i]->smallest_key->mem, variant_cmp(key, self->files[level][i]->smallest_key));
//                INFO("Compare %.*s %.*s = %d", key->length, key->mem, self->files[level][i]->largest_key->length, self->files[level][i]->largest_key->mem, variant_cmp(key, self->files[level][i]->largest_key));

                if (variant_cmp(key, self->files[level][i]->smallest_key) >= 0 &&
                    variant_cmp(key, self->files[level][i]->largest_key) <= 0)
                {
//                    INFO("ADDING IT");
                    vector_add(self->targets, self->files[level][i]);
                }
            }

            qsort(vector_data(self->targets),
                  vector_count(self->targets),
                  sizeof(SSTMetadata**), _compare_by_latest);
        }
        else
        {
            uint32_t start = sst_find_file(self, level, key);

            if (start >= self->num_files[level] ||
                variant_cmp(key, self->files[level][start]->smallest_key) < 0)
                continue;

//            DEBUG("Adding possible target %s", self->files[level][start]->loader->file->filename);

            vector_add(self->targets, self->files[level][start]);
        }
    }


    for (uint32_t i = 0; i < vector_count(self->targets); i++)
    {
//        DEBUG("Looking for key %.*s inside %s", key->length, key->mem, ((SSTMetadata*)vector_get(self->targets, i))->loader->file->filename);
        OPT opt;
        SSTMetadata* target = (SSTMetadata *)vector_get(self->targets, i);

        if (--target->allowed_seeks <= 0)
        {
            _schedule_compaction(self);
            target->allowed_seeks = target->filesize / 16384;
        }

        if (sst_loader_get(target->loader, key, value, &opt) == 1)
        {
#ifdef BACKGROUND_MERGE
            pthread_mutex_unlock(&self->lock);
#endif

            return opt == ADD;
        }
    }

#ifdef BACKGROUND_MERGE
    pthread_mutex_unlock(&self->lock);
#endif

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
    self->allowed_seeks = 100;
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
    //int first_set = 0;

    if (inputs != NULL)
        vector_clear(inputs);

    for (int i = 0; i < self->num_files[level]; i++)
    {
        SSTMetadata* target = self->files[level][i];

        if (range_intersects(begin, target->smallest_key, end, target->largest_key))
        {
            additions++;
            if (inputs)
                vector_add(inputs, target);

            if (string_cmp(target->smallest_key->mem, begin->mem, target->smallest_key->length, begin->length) < 0)
            {
                begin = target->smallest_key;
                if (inputs)
                    vector_clear(inputs);
                i = -1;
            }
            if (string_cmp(target->largest_key->mem, end->mem, target->largest_key->length, end->length) > 0)
            {
                end = target->largest_key;
                if (inputs)
                    vector_clear(inputs);
                i = -1;
            }
        }

#if 0

        //INFO("Range: %.*s End: %.*s", begin->length, begin->mem, end->length, end->mem);
        //INFO("File : %.*s end: %.*s", target->smallest_key->length, target->smallest_key->mem, target->largest_key->length, target->largest_key->mem);

        if (!begin || !end || !range_intersects(begin, target->smallest_key, end, target->largest_key))
            continue;

        //if (level == 0)
        {
            if (!first_set && begin && string_cmp(target->smallest_key->mem, begin->mem, target->smallest_key->length, begin->length) < 0)
            {
                first_set = 1; // Can grow back only once since these sequences are sorted
                begin = target->smallest_key;
            }
            if (end && string_cmp(target->largest_key->mem, end->mem, target->largest_key->length, end->length) > 0)
                end = target->largest_key;
        }

        //INFO("      Overlap!");
        additions++;

        if (inputs != NULL)
            vector_add(inputs, target);
#endif
    }

    DEBUG("Extracted range: [%.*s, %.*s]", begin->length, begin->mem, end->length, end->mem);

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

//    DEBUG("Checking in level %d", level);

    while (left < right)
    {
        uint32_t mid = (left + right) / 2;
        SSTMetadata* meta = *(self->files[level] + mid);

//        DEBUG("left %d right %d mid %d", left, right, mid);
//        DEBUG("Compare %.*s %.*s = %d", meta->largest_key->length, meta->largest_key->mem,  smallest->length, smallest->mem, variant_cmp(meta->largest_key, smallest));

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
            {
                DEBUG("Range [%.*s, %.*s] DOES overlap in level 0. Checking others",
                      start->length, start->mem,
                      stop->length, stop->mem);
                return 1;
            }
        }

        DEBUG("Range [%.*s, %.*s] DOES NOT overlap in level 0. Checking others",
              start->length, start->mem,
              stop->length, stop->mem);

        return 0;
    }

    uint32_t pos = sst_find_file(self, level, start);

    if (pos >= self->num_files[level])
        return 0;

    curr = *(self->files[level] + pos);
    int ret = range_intersects(start, curr->smallest_key, stop, curr->largest_key);

    DEBUG("Range [%.*s, %.*s] DOES%s overlap in level %d. Checking others",
          start->length, start->mem,
          stop->length, stop->mem, ret ? " " : " NOT", level);

    return ret;
}

uint32_t sst_pick_level_for_compaction(SST* self, Variant* start, Variant* stop)
{
    uint32_t level = 0;
    Vector* inputs = NULL;

    if (!sst_range_overlaps(self, level, start, stop))
    {
        uint64_t size = 0;
        inputs = vector_new();

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

    if (inputs)
        vector_free(inputs);

    INFO("Using level %d for memtable compaction [%.*s, %.*s]", level,
         start->length, start->mem,
         stop->length, stop->mem);
    return level;
}

void sst_compact(SST* self)
{
    if (self->under_compaction)
        return;

    Compaction* comp = NULL;

    if (self->comp_score >= 1)
    {
        INFO("Starting compaction. Compaction level: %d Score: %f", self->comp_level, self->comp_score);
        comp = compaction_new(self, self->comp_level);
    }

    if (!comp)
        return;

    self->under_compaction = 1;
    int needs_reset = 0, drop = 0;
    uint64_t count = 0;

    OPT opt = ADD;
    Variant* key = NULL;
    Variant* value = NULL;
    MergeIterator* iter = NULL;

    for (iter = merge_iterator_new(comp);
         merge_iterator_valid(iter); merge_iterator_next(iter))
    {
        key = merge_iterator_key(iter);
        value = merge_iterator_value(iter);
        opt = merge_iterator_opt(iter);

        // Check to see if the actual key is a deletion mark
        if (opt == DEL && compaction_is_base_level_for(comp, key))
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

        count++;
        sst_builder_add(comp->builder, key, value, opt);
    }

    assert(count > 0);

    INFO("Merge successfully completed with %d keys merged", count);

    if (drop && comp->builder->data_block->last_key)
    {
        // If we are it means that the block was ended with a dropped key
        // You have to check out this to see if it safe
        key = comp->builder->data_block->last_key;
        WARN("SST file %s was ended by a dropped key.", comp->file->filename);
        WARN("Check out the the key %.*s is actually the last key of the file", key->length, key->mem);
    }

    buffer_clear(comp->meta->largest_key);
    buffer_putnstr(comp->meta->largest_key, key->mem, key->length);

    compaction_install(comp);
    merge_iterator_free(iter);
    compaction_free(comp);

    self->under_compaction = 0;
}
