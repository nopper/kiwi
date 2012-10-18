#ifndef __LRU_H__
#define __LRU_H__

#include <sys/types.h>
#include <inttypes.h>

typedef struct _lru {
} LRU;

LRU* lru_new(uint64_t size);
void lru_set();
#endif
