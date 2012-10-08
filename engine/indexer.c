#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <unistd.h>
#include <time.h>
#include <sys/time.h>

#include "indexer.h"
#include "config.h"

void log_msg(int level, char *fmt, ...)
{
    va_list ap;
    char msg[INDEXER_MAX_LOG_MSG];

    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);

    log_msg_raw(level, msg);
}

void log_msg_raw(int level, char *msg)
{
    const char *c = ".-*#";
    FILE *fp;
    char buf[64];

    // fp = (server.logfile == NULL) ? stdout : fopen(server.logfile,"a");
    fp = stdout;

    if (!fp) return;

    int off;
    struct timeval tv;

    gettimeofday(&tv, NULL);
    off = strftime(buf, sizeof(buf), "%d %b %H:%M:%S.", localtime(&tv.tv_sec));
    snprintf(buf + off, sizeof(buf) - off, "%03d", (int)tv.tv_usec / 1000);
    fprintf(fp, "[%d] %s %c %s\n", (int)getpid(), buf, c[level], msg);
    fflush(fp);

    //if (server.logfile) fclose(fp);
}
