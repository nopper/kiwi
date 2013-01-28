#define _BSD_SOURCE
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/types.h>
#include <dirent.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include "log.h"
#include "indexer.h"
#include "skiplist.h"
#include "utils.h"

Log* log_new(const char *basedir)
{
    Log* self = calloc(1, sizeof(Log));

    if (!self)
        PANIC("NULL allocation");

    memset(self->basedir, 0, MAX_FILENAME);
    memcpy(self->basedir, basedir, MAX_FILENAME);

    self->file = file_new();
    self->file_length = 0;

    return self;
}

void log_free(Log* self)
{
    free(self);
}

void log_remove(Log* self, int lsn)
{
    char log_name[MAX_FILENAME];

    memset(log_name, 0, MAX_FILENAME);
    snprintf(log_name, MAX_FILENAME, "%s/%d.log", self->basedir, lsn);
    memcpy(self->name, log_name, MAX_FILENAME);

    INFO("Removing old log file %s", log_name);
    unlink(log_name);
}

static void _load_from(const char* filename, SkipList* list)
{
    int fd;
    struct stat s;

    if ((fd = open(filename, O_RDONLY)) < 0)
        PANIC("Unable to load log file %s", filename);

    if (stat(filename, &s) != 0)
        PANIC("Unable to get the file size of the log file %s", filename);

    void* ptr = mmap(NULL, s.st_size, PROT_READ, MAP_SHARED, fd, 0);

    const char* start = ptr;
    const char* stop = start + s.st_size;

    if (ptr == MAP_FAILED)
        PANIC("Unable to mmap log file %s", filename);

    uint32_t additions = 0, deletions = 0;

    while (start < stop)
    {
        OPT opt = ADD;
        uint32_t klen, vlen;
        const char *key, *value, *encode_start;

        encode_start = start;
        key = start = get_varint32(start, start + 5, &klen);
        start += klen;

        value = start = get_varint32(start, start + 5, &vlen);

        if (vlen == 0)
            opt = DEL;
        else
            vlen -= 1;

        start += vlen;

        if (opt == ADD) additions++;
        if (opt == DEL) deletions++;

        char *object = malloc(start - encode_start);
        memcpy(object, encode_start, start - encode_start);

        //DEBUG("KLEN %d VLEN %d OPT %d Key: %.*s Value: %.*s", klen, vlen, opt, klen, key, vlen, value);

        if (skiplist_insert(list, key, klen, opt, object) == STATUS_OK_DEALLOC)
            free(object);
    }

    if (munmap(ptr, s.st_size) != 0)
        PANIC("Unable to unmap log file %s", filename);

    if (close(fd) < 0)
        PANIC("Unable to close log file %s", filename);

    if (unlink(filename) != 0)
        PANIC("Unable to remove log file %s after recovery", filename);

    INFO("%d operations [%d additions, %d deletions] recovered from %s",
         additions + deletions, additions, deletions, filename);
}

int log_recovery(Log* self, SkipList* skiplist)
{
    struct dirent **namelist;
    int n = scandir(self->basedir, &namelist, 0, alphasort);

    if (n < 0)
        PANIC("scandir error");

    while(n--)
    {
        if (strstr(namelist[n]->d_name, ".log"))
        {
            char log_name[MAX_FILENAME];

            memset(log_name, 0, MAX_FILENAME);
            snprintf(log_name, MAX_FILENAME, "%s/%s", self->basedir, namelist[n]->d_name);

            INFO("Recoverying from %s", log_name);

            _load_from(log_name, skiplist);
        }

        free(namelist[n]);
    }

    free(namelist);
    return 0;
}

void log_next(Log* self, int lsn)
{
    char log_name[MAX_FILENAME];

    // Close previosly opened file if present
    file_close(self->file);

    memset(log_name, 0, MAX_FILENAME);
    snprintf(log_name, MAX_FILENAME, "%s/%d.log", self->basedir, lsn);

    memcpy(self->file->filename, log_name, MAX_FILENAME);
    writable_file_new(self->file);

    self->file_length = 0;

    DEBUG("Log file %s created", self->file->filename);
}

int log_append(Log* self, char *value, size_t length)
{
    // Here we should instruct the File class to do some fsync after
    // a certain amount of insertions.
    file_append_raw(self->file, value, length);
    self->file_length += length;
    return (self->file_length >= LOG_MAXSIZE);
}
