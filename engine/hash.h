#ifndef __HASH_H__
#define __HASH_H__

#include <sys/types.h>
#include <stdint.h>

uint32_t hash(const char* data, size_t n, uint32_t seed);

#endif
