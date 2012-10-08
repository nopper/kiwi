#ifndef __UTILS_H__
#define __UTILS_H__

#include <stdint.h>
#include <stdlib.h>
#include "variant.h"

size_t varint_length(uint64_t v);
char* encode_varint32(char* dst, uint32_t v);
char* encode_varint64(char* dst, uint64_t v);

const char* get_varint32(const char* p, const char* limit, uint32_t* value);
const char* get_varint64(const char* p, const char* limit, uint64_t* value);

uint32_t get_int32(const char* ptr);
uint64_t get_int64(const char* ptr);

long long get_ustime_sec(void);
void int3(void);

int string_cmp(const char *s1, const char *s2, size_t ln, size_t lm);
int variant_cmp(const Variant* a, const Variant* b);
int range_intersects(Variant* astart, Variant* bstart, Variant* astop, Variant* bstop);

#endif
