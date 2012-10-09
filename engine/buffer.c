#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include "buffer.h"
#include "indexer.h"
#include "utils.h"

unsigned _next_power(unsigned x)
{
    --x;
    x |= x >> 0x01;
    x |= x >> 0x02;
    x |= x >> 0x04;
    x |= x >> 0x08;
    x |= x >> 0x10;
    return ++x;
}

void buffer_extend_by(Buffer* self, size_t len)
{
    char* buffer;

    len += self->length;
    if (len <= self->allocated)
        return;

    if (!self->allocated)
        self->allocated = 32;

    self->allocated = _next_power(len);
//    DEBUG("Realloc of %d", self->allocated);
    buffer = realloc(self->mem, self->allocated);
    self->mem = buffer;
}

void _string_vprintf(Buffer* self, const char *fmt, va_list ap)
{
    int num_required;

    while ((num_required = vsnprintf(self->mem + self->length, self->allocated - self->length, fmt, ap)) >= self->allocated - self->length)
        buffer_extend_by(self, num_required + 1);

    self->length += num_required;
}

Buffer* buffer_new(size_t reserve)
{
    Buffer* self = malloc(sizeof(Buffer));

    if (!self)
        ERROR("NULL allocation");

    self->mem = NULL;
    self->length = 0;
    self->allocated = 0;

    if (reserve)
        buffer_extend_by(self, reserve + 1);

    return self;
}

void buffer_free(Buffer* self)
{
    if (self->mem)
        free(self->mem);
    free(self);
}

void buffer_clear(Buffer* self)
{
    self->length = 0;

    if (self->mem)
        self->mem[self->length] = '\0';
}

void buffer_putstr(Buffer* self, const char *str)
{
    size_t len = strlen(str);

    buffer_extend_by(self, len + 1);
    memcpy(&self->mem[self->length], str, len);
    self->length += len;
    self->mem[self->length] = '\0';
}

void buffer_putnstr(Buffer* self, const char *str, size_t n)
{
    buffer_extend_by(self, n + 1);
    memcpy(&self->mem[self->length], str, n);
    self->length += n;
    self->mem[self->length] = '\0';
}

void buffer_putc(Buffer* self, const char c)
{
    buffer_extend_by(self, 2);
    self->mem[self->length++] = c;
    self->mem[self->length] = '\0';
}

void buffer_putint32(Buffer* self, uint32_t val)
{
    buffer_extend_by(self, sizeof(int));
    self->mem[self->length++] = val & 0xff;
    self->mem[self->length++] = (val >> 8) & 0xff;
    self->mem[self->length++] = (val >> 16) & 0xff;
    self->mem[self->length++] = (val >> 24) & 0xff;
}

void buffer_putint64(Buffer* self, uint64_t val)
{
    buffer_extend_by(self, sizeof(uint64_t));
    self->mem[self->length++] = val & 0xff;
    self->mem[self->length++] = (val >> 8) & 0xff;
    self->mem[self->length++] = (val >> 16) & 0xff;
    self->mem[self->length++] = (val >> 24) & 0xff;
    self->mem[self->length++] = (val >> 32) & 0xff;
    self->mem[self->length++] = (val >> 40) & 0xff;
    self->mem[self->length++] = (val >> 48) & 0xff;
    self->mem[self->length++] = (val >> 56) & 0xff;
}

void buffer_putvarint32(Buffer* self, uint32_t v)
{
    uint32_t length = varint_length(v);
    buffer_extend_by(self, length);
    encode_varint32(&self->mem[self->length], v);
    self->length += length;
}

void buffer_putvarint64(Buffer* self, uint64_t v)
{
    uint32_t length = varint_length(v);
    buffer_extend_by(self, length);
    encode_varint64(&self->mem[self->length], v);
    self->length += length;
}

void buffer_putshort(Buffer* self, short val)
{
    buffer_extend_by(self, sizeof(short));
    self->mem[self->length++] = val & 0xff;
    self->mem[self->length++] = (val >> 8) & 0xff;
}

void buffer_putlong(Buffer* self, uint64_t val)
{
    buffer_extend_by(self, sizeof(uint64_t));
    self->mem[self->length++] = val & 0xff;
    self->mem[self->length++] = (val >> 8) & 0xff;
    self->mem[self->length++] = (val >> 16) & 0xff;
    self->mem[self->length++] = (val >> 24) & 0xff;
    self->mem[self->length++] = (val >> 32) & 0xff;
    self->mem[self->length++] = (val >> 40) & 0xff;
    self->mem[self->length++] = (val >> 48) & 0xff;
    self->mem[self->length++] = (val >> 56) & 0xff;
}

void buffer_scatf(Buffer* self, const char *fmt, ...)
{
    va_list ap;

    va_start(ap, fmt);
    _string_vprintf(self, fmt, ap);
    va_end(ap);
}

char * buffer_detach(Buffer* self)
{
    char *buffer = self->mem;

    self->length = 0;
    return buffer;
}

void buffer_dump(Buffer* self)
{
    int i;

    printf("--buffer dump:allocated<%u>,length<%u>\n", (unsigned int)self->allocated, (unsigned int)self->length);

    for (i = 0; i < self->length; i++)
        printf("\t[%d] %c\n", i, self->mem[i]);
}
