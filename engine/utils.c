#include "utils.h"
#include "indexer.h"
#include <signal.h>
#include <string.h>
#include <sys/time.h>

size_t varint_length(uint64_t v)
{
    unsigned int len = 1;
    while (v >= 128)
    {
        v >>= 7;
        len++;
    }
    return len;
}

char* encode_varint64(char* dst, uint64_t v)
{
    static const int B = 128;
    unsigned char* ptr = (unsigned char *)dst;

    while (v >= B) {
        *(ptr++) = (v & (B-1)) | B;
        v >>= 7;
    }

    *(ptr++) = (unsigned char)v;
    return (char*)ptr;
}

char* encode_varint32(char* dst, uint32_t v)
{
    static const int B = 128;
    unsigned char* ptr = (unsigned char *)dst;

    if (v < (1<<7)) {
        *(ptr++) = v;
    } else if (v < (1<<14)) {
        *(ptr++) = v | B;
        *(ptr++) = v>>7;
    } else if (v < (1<<21)) {
        *(ptr++) = v | B;
        *(ptr++) = (v>>7) | B;
        *(ptr++) = v>>14;
    } else if (v < (1<<28)) {
        *(ptr++) = v | B;
        *(ptr++) = (v>>7) | B;
        *(ptr++) = (v>>14) | B;
        *(ptr++) = v>>21;
    } else {
        *(ptr++) = v | B;
        *(ptr++) = (v>>7) | B;
        *(ptr++) = (v>>14) | B;
        *(ptr++) = (v>>21) | B;
        *(ptr++) = v>>28;
    }

    return (char*)ptr;
}

const char* get_varint64(const char* p, const char* limit, uint64_t* value)
{
    uint64_t result = 0;
    for (uint32_t shift = 0; shift <= 63 && p < limit; shift += 7) {
        uint64_t byte = *((const unsigned char*)(p));
        p++;
        if (byte & 128) {
            // More bytes are present
            result |= ((byte & 127) << shift);
        } else {
            result |= (byte << shift);
            *value = result;
            return p;
        }
    }
    return NULL;
}

static const char* _get_varint32(const char* p, const char* limit, uint32_t* value)
{
    uint32_t result = 0;
    for (uint32_t shift = 0; shift <= 28 && p < limit; shift += 7) {
        uint32_t byte = *((const unsigned char*)(p));
        p++;
        if (byte & 128) {
            // More bytes are present
            result |= ((byte & 127) << shift);
        } else {
            result |= (byte << shift);
            *value = result;
            return p;
        }
    }
    return NULL;
}

const char* get_varint32(const char* p, const char* limit, uint32_t* value)
{
    uint32_t result = 0;
    result = *((const unsigned char*)(p));
    if ((result & 128) == 0)
    {
        *value = result;
        return ++p;
    }
    return _get_varint32(p, limit, value);
}

inline uint32_t get_int32(const char* ptr)
{
    if (IS_LITTLE_ENDIAN)
    {
        // Load the raw bytes
        uint32_t result;
        memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
        return result;
    }
    else
    {
        return (uint32_t)ptr[0] |
               (uint32_t)ptr[1] << 8 |
               (uint32_t)ptr[2] << 16 |
               (uint32_t)ptr[3] << 24;
    }
}

inline uint64_t get_int64(const char* ptr)
{
    if (IS_LITTLE_ENDIAN)
    {
        // Load the raw bytes
        uint64_t result;
        memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
        return result;
    }
    else
    {
        uint64_t lo = get_int32(ptr);
        uint64_t hi = get_int32(ptr + 4);
        return (hi << 32) | lo;
    }
}

long long get_ustime_sec(void)
{
    struct timeval tv;
    long long ust;

    gettimeofday(&tv, NULL);
    ust = ((long long)tv.tv_sec)*1000000;
    ust += tv.tv_usec;
    return ust / 1000000;
}

void int3(void)
{
    raise(SIGTRAP);
}


inline int string_cmp(const char *s1, const char *s2, size_t ln, size_t lm)
{
    int ret = memcmp(s1, s2, MIN(ln, lm));

    if (ln == lm || ret != 0)
        return ret;

    return (ln < lm) ? -1 : 1;
}

int variant_cmp(const Variant* a, const Variant* b)
{
    return string_cmp(a->mem, b->mem, a->length, b->length);
}

int range_intersects(Variant* astart, Variant* bstart, Variant* astop, Variant* bstop)
{
    return
      /*int ret = */
           !((string_cmp(bstop->mem, astart->mem, bstop->length, astart->length) < 0) ||
             (string_cmp(bstart->mem, astop->mem, bstart->length, astop->length) > 0));
    /*
    DEBUG("RANGE CHECK [%.*s, %.*s] vs [%.*s, %.*s] = %d %d = %d",
          astart->length, astart->mem,
          astop->length, astop->mem,
          bstart->length, bstart->mem,
          bstop->length, bstop->mem,
          string_cmp(bstop->mem, astart->mem, bstop->length, astart->length),
          string_cmp(bstart->mem, astop->mem, bstart->length, astop->length), ret);
    return ret;
    */
}
