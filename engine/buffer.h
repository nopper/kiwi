#ifndef __BUFFER_H__
#define __BUFFER_H__

#include <stdint.h>
#include <stdlib.h>

typedef struct buffer {
    char *mem;
    size_t length;
    size_t allocated;
} Buffer;

Buffer* buffer_new(size_t reserve);
void buffer_free(Buffer* self);
void buffer_extend_by(Buffer* self, size_t len);

void buffer_clear(Buffer* self);
char *buffer_detach(Buffer* self);

void buffer_putc(Buffer* self, const char c);
void buffer_putstr(Buffer* self, const char *str);
void buffer_putnstr(Buffer* self, const char *str, size_t n);

void buffer_putint32(Buffer* self, uint32_t v);
void buffer_putint64(Buffer* self, uint64_t v);
void buffer_putvarint32(Buffer* self, uint32_t v);
void buffer_putvarint64(Buffer* self, uint64_t v);

void buffer_scatf(Buffer* self, const char *fmt, ...);
void buffer_putlong(Buffer* self, uint64_t val);
void buffer_putshort(Buffer* self, short val);

void buffer_dump(Buffer* self);

#endif
