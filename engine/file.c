#define _BSD_SOURCE
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <sys/mman.h>
#include "indexer.h"
#include "file.h"

File* file_new(void)
{
    File* self = malloc(sizeof(File));
    self->fd = -1;
    self->offset = self->map_size = 0;
    self->base = self->limit = self->current = NULL;
    return self;
}

void file_free(File* self)
{
    if (self->fd != -1)
        file_close(self);

    free(self);
}

int writable_file_new(File* self)
{
    if ((self->fd = open(self->filename, O_CREAT | O_RDWR | O_TRUNC, 0644)) < 0)
        return 0;

    self->offset = self->map_size = 0;
    self->base = self->limit = self->current = NULL;

    int pagesize = sysconf(_SC_PAGESIZE);
    self->map_size = ((START_MAP_SIZE + pagesize - 1) / pagesize) * pagesize;

    return 1;
}

int sequential_file_new(File* self)
{
    if ((self->fd = open(self->filename, O_RDONLY)) < 0)
        return 0;

    return 1;
}

int mmapped_file_new(File* self)
{
    if ((self->fd = open(self->filename, O_RDONLY)) < 0)
        return 0;

    self->map_size = file_size(self);
    void* ptr = mmap(NULL, self->map_size, PROT_READ, MAP_SHARED, self->fd, 0);

    if (ptr == MAP_FAILED)
    {
        ERROR("Unable to unmap %s: %s", self->filename, strerror(errno));
        return 0;
    }

    INFO("Mapping of %d bytes for %s", self->map_size, self->filename);

    self->base = ptr;
    self->current = self->limit = (char *)ptr + self->map_size;
    return 1;
}

uint64_t file_size(File* self)
{
    struct stat s;

    if (stat(self->filename, &s) != 0)
        return 0;

    return (uint64_t)s.st_size;
}

int _unmap_region(File* self)
{
    int ret = 1;

    if (self->base)
    {
        size_t size = self->limit - self->base;

        if (munmap(self->base, size) != 0)
        {
            ERROR("Unable to unmap %s: %s", self->filename, strerror(errno));
            ret = 0;
        }
//        else
//            INFO("Unmapping %d bytes for %s", size, self->filename);

        self->offset += size;
        self->base = self->limit = self->current = NULL;

        // Double allocation size until 2^20 bytes (1MB) limit
        if (self->map_size < (2 << 20))
            self->map_size <<= 1;
    }

    return ret;
}

int _map_new_region(File* self)
{
    if (ftruncate(self->fd, self->offset + self->map_size) < 0)
        return 0;

    void* ptr = mmap(NULL, self->map_size, PROT_READ | PROT_WRITE, MAP_SHARED,
                     self->fd, self->offset);

    if (ptr == MAP_FAILED)
    {
        ERROR("Mapping of %d bytes for %s failed: %s", self->map_size, self->filename, strerror(errno));
        return 0;
    }

    //INFO("Mapping of %d bytes for %s", self->map_size, self->filename);

    self->current = self->base = ptr;
    self->limit = self->base + self->map_size;
    return 1;
}

int file_append_raw(File* self, const char* src, size_t length)
{
    size_t left = length;

    while (left > 0)
    {
        size_t avail = self->limit - self->current;

        if (avail == 0)
        {
            if (!_unmap_region(self) || !_map_new_region(self))
                return 0;
        }

        size_t n = (left <= avail) ? left : avail;
        memcpy(self->current, src, n);
        self->current += n;
        src += n;
        left -= n;
    }

    return 1;
}

int file_append(File* self, Buffer* data)
{
    return file_append_raw(self, data->mem, data->length);
}

int file_close(File* self)
{
    int ret = 1;
    size_t unused = self->limit - self->current;

    if (!_unmap_region(self))
    {
        ret = 0;
    }
    else if (unused > 0)
    {
        DEBUG("Truncating file %s to %d bytes", self->filename, self->offset - unused);
        if (ftruncate(self->fd, self->offset - unused) < 0)
            ret = 0;
    }

    fsync(self->fd);

    if (self->fd != -1 && close(self->fd) < 0)
        ret = 0;

    self->fd = -1;
    self->base = self->limit = NULL;

    return ret;
}

int file_exists(File* self)
{
    return access(self->filename, F_OK) == 0;
}

int mkdirp(const char *pathname)
{
    int i, len;
    char dirname[MAX_FILENAME];

    strncpy(dirname, pathname, MAX_FILENAME);
    i = strlen(dirname);
    len = i;

    INFO("Creating directory structure: %s", dirname);

    if (dirname[len - 1] != '/')
        strncat(dirname, "/", MAX_FILENAME);

    len = strlen(dirname);

    for (i = 1; i < len; i++) {
        if (dirname[i] == '/') {
            dirname[i] = 0;
            if(access(dirname, 0) != 0) {
                DEBUG(" -> Creating %s", dirname);
                if(mkdir(dirname,   0755)==-1) {
                    return -1;
                }
            }
            dirname[i] = '/';
        }
    }

    return 0;
}
