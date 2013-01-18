#ifndef __LOG_H__
#define __LOG_H__

#include "config.h"
#include "skiplist.h"

typedef struct _log {
    int log_fd;
    size_t file_length;
    char name[MAX_FILENAME];
    char basedir[MAX_FILENAME];
} Log;

Log* log_new(const char *basedir);
void log_next(Log* self, int lsn);
int log_recovery(Log* self, SkipList* list);
int log_append(Log* self, char *value, size_t length);
void log_remove(Log* self, int lsn);

#endif
