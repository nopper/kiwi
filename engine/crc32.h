#ifndef __CRC32_H__
#define __CRC32_H__

#include <stddef.h>
#include <stdint.h>

// Return the crc32c of concat(A, data[0,n-1]) where init_crc is the
// crc32c of some string A.  Extend() is often used to maintain the
// crc32c of a stream of data.
extern uint32_t crc32_extend(uint32_t init_crc, const char* data, size_t n);

// Return the crc32c of data[0,n-1]
inline uint32_t crc32_value(const char* data, size_t n) {
    return crc32_extend(0, data, n);
}

#define KMASK_DELTA 0xa282ead8ul

// Return a masked representation of crc.
//
// Motivation: it is problematic to compute the CRC of a string that
// contains embedded CRCs.  Therefore we recommend that CRCs stored
// somewhere (e.g., in files) should be masked before being stored.
inline uint32_t crc32_mask(uint32_t crc) {
    // Rotate right by 15 bits and add a constant.
    return ((crc >> 15) | (crc << 17)) + KMASK_DELTA;
}

// Return the crc whose masked representation is masked_crc.
inline uint32_t crc32_unmask(uint32_t masked_crc) {
    uint32_t rot = masked_crc - KMASK_DELTA;
    return ((rot >> 17) | (rot << 15));
}

#endif
