#include <string.h>
#include "db.h"
#include "indexer.h"

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
