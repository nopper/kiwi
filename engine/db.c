#include <string.h>
#include <assert.h>
#include "db.h"
#include "indexer.h"
#include "utils.h"

DB* db_open(const char* basedir)
{
    DB* self = calloc(1, sizeof(DB));

    if (!self)
        PANIC("NULL allocation");

    strncpy(self->basedir, basedir, MAX_FILENAME);

    self->sst = sst_new(basedir);
    self->memtable = memtable_new();

    return self;
}

void db_close(DB *self)
{
    INFO("Closing database %d", self->memtable->add_count);

    if (self->memtable->add_count > 0)
        sst_merge(self->sst, self->memtable->list);

    memtable_free(self->memtable);
    sst_free(self->sst);
    free(self);
}

int db_add(DB* self, Variant* key, Variant* value)
{
    if (memtable_needs_compaction(self->memtable))
    {
        INFO("Starting compaction of the memtable after %d insertions and %d deletions",
             self->memtable->add_count, self->memtable->del_count);
        sst_merge(self->sst, self->memtable->list);

        memtable_free(self->memtable);
        self->memtable = memtable_new();
    }

    return memtable_add(self->memtable, key, value);
}

int db_get(DB* self, Variant* key, Variant* value)
{
    if (memtable_get(self->memtable, key, value) == 1)
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

    self->prev = self->node = db->memtable->list->hdr;

    self->use_memtable = 1;
    self->use_files = 1;

    return self;
}

void db_iterator_free(DBIterator* self)
{
    vector_free(self->iterators);
    buffer_free(self->sl_key);
    buffer_free(self->sl_value);
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
            vector_add(self->iterators,
                       chained_iterator_new_seek(vector_count(files),
                                                 (SSTMetadata **)vector_release(files), key));

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
            vector_add(files, (void*)sst->files[level][i]);

        vector_add(self->iterators,
                   chained_iterator_new_seek(
                       vector_count(files),
                       (SSTMetadata **)vector_release(files), key));
    }

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

static void _db_iterator_next_mem(DBIterator* self)
{
    while (1)
    {
        self->prev = self->node;
        if (self->node == self->db->memtable->list->hdr)
            return;

        OPT opt;
        memtable_extract_node(self->node, self->sl_key, self->sl_value, &opt);
        self->node = self->node->forward[0];

        if (opt == DEL)
        {
            buffer_clear(self->sl_key);
            buffer_clear(self->sl_value);
        }
        else
            break;
    }
}

void db_iterator_next(DBIterator* self)
{
    if (self->use_files)
        _db_iterator_next(self);
    if (self->use_memtable)
        _db_iterator_next_mem(self);

    int ret = (self->prev != self->db->memtable->list->hdr) ? -1 : 1;

    while (self->valid && self->prev != self->db->memtable->list->hdr)
    {
        ret = variant_cmp(self->sl_key, self->current->current->key);
        INFO("COMPARING: %.*s %.*s", self->sl_key->length, self->sl_key->mem, self->current->current->key->length,self->current->current->key->mem );

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
    return (self->valid || self->prev != self->db->memtable->list->hdr);
}

Variant* db_iterator_key(DBIterator* self)
{
    if (self->use_files)
        return self->current->current->key;
    return self->sl_key;
}

Variant* db_iterator_value(DBIterator* self)
{
    if (self->use_files)
        return self->current->current->value;
    return self->sl_value;
}
