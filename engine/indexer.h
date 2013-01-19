#ifndef indexer_indexer_h
#define indexer_indexer_h

#include "config.h"

#define MIN(a,b) (((a)<(b))?(a):(b))
#define MAX(a,b) (((a)>(b))?(a):(b))

#define STRINGIZE(x) STRINGIZE2(x)
#define STRINGIZE2(x) #x
#define LINE_STRING STRINGIZE(__LINE__)

#define TYPE_NO_COMPRESSION     0
#define TYPE_SNAPPY_COMPRESSION 1

#define PRINT

#ifdef PRINT
#define INFO(...)\
    do { log_msg(LEVEL_INFO, __FILE__ ":" LINE_STRING " " __VA_ARGS__); } while (0)

#define DEBUG(...)\
    do { log_msg(LEVEL_DEBUG, __FILE__ ":" LINE_STRING " " __VA_ARGS__); } while (0)

#define WARN(...)\
    do { log_msg(LEVEL_WARNING, __FILE__ ":" LINE_STRING " " __VA_ARGS__); } while (0)

#define ERROR(...)\
    do { log_msg(LEVEL_ERROR, __FILE__ ":" LINE_STRING " " __VA_ARGS__); } while (0)
#else
#define INFO(...) do {} while(0)
#define DEBUG(...) do {} while(0)
#define WARN(...) do {} while(0)
#define ERROR(...) do {} while(0)
#endif

#define PANIC(...)\
    do { log_msg(LEVEL_ERROR, __FILE__ ":" LINE_STRING " " __VA_ARGS__); abort(); exit(1); } while (0)

void log_msg(int level, char *fmt, ...);
void log_msg_raw(int level, char *msg);

#endif
