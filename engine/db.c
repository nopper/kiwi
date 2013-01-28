#include <string.h>
#include <assert.h>
#include "db.h"
#include "indexer.h"
#include "utils.h"
#include "log.h"

DB* db_open_ex(const char* basedir, uint64_t cache_size)
{
    DB* self = calloc(1, sizeof(DB));

    if (!self)
        PANIC("NULL allocation");

    strncpy(self->basedir, basedir, MAX_FILENAME);
    self->sst = sst_new(basedir, cache_size);

    Log* log = log_new(self->sst->basedir);
    self->memtable = memtable_new(log);

    return self;
}

DB* db_open(const char* basedir)
{
    return db_open_ex(basedir, LRU_CACHE_SIZE);
}

void db_close(DB *self)
{
    INFO("Closing database %d", self->memtable->add_count);

    if (self->memtable->list->count > 0)
    {
        sst_merge(self->sst, self->memtable);
        skiplist_release(self->memtable->list);
        self->memtable->list = NULL;
    }

    sst_free(self->sst);
    log_remove(self->memtable->log, self->memtable->lsn);
    log_free(self->memtable->log);
    memtable_free(self->memtable);
    free(self);
}

int db_add(DB* self, Variant* key, Variant* value)
{
    if (memtable_needs_compaction(self->memtable))
    {
        INFO("Starting compaction of the memtable after %d insertions and %d deletions",
             self->memtable->add_count, self->memtable->del_count);
        sst_merge(self->sst, self->memtable);
        memtable_reset(self->memtable);
    }

    return memtable_add(self->memtable, key, value);
}

int db_get(DB* self, Variant* key, Variant* value)
{
    if (memtable_get(self->memtable->list, key, value) == 1)
        return 1;

    return sst_get(self->sst, key, value);
}

int db_remove(DB* self, Variant* key)
{
    return memtable_remove(self->memtable, key);
}

DBIterator* db_iterator_new(DB* db)
{
    DBIterator* self = calloc(1, sizeof(DBIterator));
    self->iterators = vector_new();
    self->db = db;

    self->sl_key = buffer_new(1);
    self->sl_value = buffer_new(1);

    self->list = db->memtable->list;
    self->prev = self->node = self->list->hdr;

    skiplist_acquire(self->list);

    // Let's acquire the immutable list if any
    pthread_mutex_lock(&self->db->sst->immutable_lock);

    if (self->db->sst->immutable_list)
    {
        skiplist_acquire(self->db->sst->immutable_list);

        self->imm_list = self->db->sst->immutable_list;
        self->imm_prev = self->imm_node = self->imm_list->hdr;
        self->has_imm = 1;
    }

    pthread_mutex_unlock(&self->db->sst->immutable_lock);

    // TODO: At this point we should get the current sequence of the active
    // SkipList in order to avoid polluting the iteration

    self->use_memtable = 1;
    self->use_files = 1;

    self->advance = ADV_MEM | ADV_MEM;

    return self;
}

void db_iterator_free(DBIterator* self)
{
    for (int i = 0; i < vector_count(self->iterators); i++)
        chained_iterator_free((ChainedIterator *)vector_get(self->iterators, i));

    heap_free(self->minheap);
    vector_free(self->iterators);

    buffer_free(self->sl_key);
    buffer_free(self->sl_value);

    if (self->has_imm)
    {
        buffer_free(self->isl_key);
        buffer_free(self->isl_value);
    }

    skiplist_release(self->list);

    if (self->imm_list)
        skiplist_release(self->imm_list);

    free(self);
}

static void _db_iterator_add_level0(DBIterator* self, Variant* key)
{
    // Createa all iterators for scanning level0. If is it possible
    // try to create a chained iterator for non overlapping sequences.

    int i = 0;
    SST* sst = self->db->sst;

    while (i < sst->num_files[0])
    {
        INFO("Comparing %.*s %.*s", key->length, key->mem, sst->files[0][i]->smallest_key->length, sst->files[0][i]->smallest_key->mem);
        if (variant_cmp(key, sst->files[0][i]->smallest_key) < 0)
        {
            i++;
            continue;
        }
        break;
    }

    i -= 1;

    if (i < 0 || i >= sst->num_files[0])
        return;

    int j = i + 1;
    Vector* files = vector_new();
    vector_add(files, sst->files[0][i]);

    INFO("%s", sst->files[0][0]->loader->file->filename);

    while ((i < sst->num_files[0]) && (j < sst->num_files[0]))
    {
        if (!range_intersects(sst->files[0][i]->smallest_key,
                            sst->files[0][i]->largest_key,
                            sst->files[0][j]->smallest_key,
                            sst->files[0][j]->largest_key))
            vector_add(files, sst->files[0][j]);
        else
        {
            size_t num_files = vector_count(files);
            SSTMetadata** arr = vector_release(files);

            vector_add(self->iterators,
                       chained_iterator_new_seek(num_files, arr, key));

            i = j;
            vector_add(files, sst->files[0][i]);
        }

        j++;
    }

    if (vector_count(files) > 0)
    {
        vector_add(self->iterators,
                   chained_iterator_new_seek(vector_count(files),
                                             (SSTMetadata **)files->data, key));

        files->data = NULL;
    }

    vector_free(files);
}

void db_iterator_seek(DBIterator* self, Variant* key)
{
#ifdef BACKGROUND_MERGE
    pthread_mutex_lock(&self->db->sst->lock);
#endif

    _db_iterator_add_level0(self, key);

    int i = 0;
    SST* sst = self->db->sst;
    Vector* files = vector_new();

    for (int level = 1; level < MAX_LEVELS; level++)
    {
        i = sst_find_file(sst, level, key);

        if (i >= sst->num_files[level])
            continue;

        for (; i < sst->num_files[level]; i++)
        {
            DEBUG("Iterator will include: %d [%.*s, %.*s]",
                  sst->files[level][i]->filenum,
                  sst->files[level][i]->smallest_key->length,
                  sst->files[level][i]->smallest_key->mem,
                  sst->files[level][i]->largest_key->length,
                  sst->files[level][i]->largest_key->mem);
            vector_add(files, (void*)sst->files[level][i]);
        }

        size_t num_files = vector_count(files);
        SSTMetadata** arr = vector_release(files);

        vector_add(self->iterators,
                   chained_iterator_new_seek(num_files, arr, key));
    }

#ifdef BACKGROUND_MERGE
    pthread_mutex_unlock(&self->db->sst->lock);
#endif
    vector_free(files);

    self->minheap = heap_new(vector_count(self->iterators), (comparator)chained_iterator_comp);

    for (i = 0; i < vector_count(self->iterators); i++)
        heap_insert(self->minheap, (ChainedIterator*)vector_get(self->iterators, i));

    self->node = skiplist_lookup_prev(self->db->memtable->list, key->mem, key->length);

    if (!self->node)
        self->node = self->db->memtable->list->hdr;

    self->prev = self->node;

    db_iterator_next(self);
}

static void _db_iterator_next(DBIterator* self)
{
    ChainedIterator* iter;

start:

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

                assert(iter->current->valid);
                heap_insert(self->minheap, iter);
            }
            else
                sst_loader_iterator_free(iter->current);
        }
    }

    if (heap_pop(self->minheap, (void**)&iter))
    {
        assert(iter->current->valid);

        self->current = iter;
        self->valid = 1;

        if (iter->skip == 1)
            goto start;

        if (iter->current->opt == DEL)
            goto start;
    }
    else
        self->valid = 0;
}

static void _db_iterator_advance_mem(DBIterator* self)
{
    while (1)
    {
        self->prev = self->node;
        self->list_end = self->node == self->list->hdr;

        if (self->list_end)
            return;

        OPT opt;
        memtable_extract_node(self->node, self->sl_key, self->sl_value, &opt);
        self->node = self->node->forward[0];

        if (opt == ADD)
            break;

        buffer_clear(self->sl_key);
        buffer_clear(self->sl_value);
    }
}

static void _db_iterator_advance_imm(DBIterator* self)
{
    while (self->has_imm)
    {
        self->imm_prev = self->imm_node;
        self->imm_list_end = self->imm_node == self->imm_list->hdr;

        if (self->imm_list_end)
            return;

        OPT opt;
        memtable_extract_node(self->imm_node, self->isl_key, self->isl_value, &opt);
        self->imm_node = self->imm_node->forward[0];

        if (opt == ADD)
            break;

        buffer_clear(self->isl_key);
        buffer_clear(self->isl_value);
    }
}

static void _db_iterator_next_mem(DBIterator* self)
{
    if (self->advance & ADV_MEM) _db_iterator_advance_mem(self);
    if (self->advance & ADV_IMM) _db_iterator_advance_imm(self);

    // Here we need to compare the two keys
    if (self->sl_key && !self->isl_key)
    {
        self->advance = ADV_MEM;
        self->key = self->sl_key;
        self->value = self->sl_value;
    }
    else if (!self->sl_key && self->isl_key)
    {
        self->advance = ADV_IMM;
        self->key = self->isl_key;
        self->value = self->isl_value;
    }
    else
    {
        if (variant_cmp(self->sl_key, self->isl_key) <= 0)
        {
            self->advance = ADV_MEM;
            self->key = self->sl_key;
            self->value = self->sl_value;
        }
        else
        {
            self->advance = ADV_IMM;
            self->key = self->isl_key;
            self->value = self->isl_value;
        }
    }
}

void db_iterator_next(DBIterator* self)
{
    if (self->use_files)
        _db_iterator_next(self);
    if (self->use_memtable)
        _db_iterator_next_mem(self);

    int ret = (self->list_end) ? 1 : -1;

    while (self->valid && !self->list_end)
    {
        ret = variant_cmp(self->key, self->current->current->key);
        //INFO("COMPARING: %.*s %.*s", self->key->length, self->key->mem, self->current->current->key->length,self->current->current->key->mem );

        // Advance the iterator from disk until it's greater than the memtable key
        if (ret == 0)
            _db_iterator_next(self);
        else
            break;
    }

    if (ret <= 0)
    {
        self->use_memtable = 1;
        self->use_files = 0;
    }
    else
    {
        self->use_memtable = 0;
        self->use_files = 1;
    }
}

int db_iterator_valid(DBIterator* self)
{
    return (self->valid || !self->list_end || (self->has_imm && !self->imm_list_end));
}

Variant* db_iterator_key(DBIterator* self)
{
    if (self->use_files)
        return self->current->current->key;
    return self->key;
}

Variant* db_iterator_value(DBIterator* self)
{
    if (self->use_files)
        return self->current->current->value;
    return self->value;
}
