#ifndef __FILE_H__
#define __FILE_H__

#include "indexer.h"
#include "buffer.h"

typedef struct _file {
    int fd;
    uint64_t offset;
    char filename[MAX_FILENAME];

    // Memory mapped informations
    char *base;
    char *limit;
    char *current;

    size_t map_size;
} File;

File* file_new(void);
void file_free(File* self);

int writable_file_new(File* self);
int mmapped_file_new(File* self);

int file_append(File* self, Buffer* data);
int file_append_raw(File* self, const char* data, size_t length);
int file_close(File* self);

uint64_t file_size(File* self);
int file_exists(File* self);

int mkdirp(const char* pathname);

#endif
