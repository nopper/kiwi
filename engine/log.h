#ifndef __LOG_H__
#define __LOG_H__

#include "file.h"

typedef struct _log_writer {
    File* file;
} LogWriter;

typedef LogWriter LogReader;

LogWriter* log_new(const char *filename);

#endif
