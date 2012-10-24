#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include "db.h"
#include "sst_loader.h"

#define ADD(x, y) \
    buffer_clear(key); \
    buffer_clear(value); \
    buffer_putstr(key, x); \
    buffer_putstr(value, y); \
    db_add(db, key, value);

#define DEL(x) \
    buffer_clear(key); \
    buffer_putstr(key, x); \
    db_remove(db, key);

#define GET(x) \
    buffer_clear(key); \
    buffer_clear(value); \
    buffer_putstr(key, x); \
    db_get(db, key, value);

void gen_random(char *s, const int len) {
    static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";

    for (int i = 0; i < len; ++i) {
        s[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
    }

    s[len] = 0;
}

int main(int argc, const char * argv[])
{
    int i;
    DB* db = db_open(START_DIRECTORY);
    Variant* key = buffer_new(256);
    Variant* value = buffer_new(256);

    srand(time(NULL));

    for (int i = 0; i < 1000000; i++)
    {
        buffer_clear(key);
        buffer_scatf(key, "%d", i);

        buffer_clear(value);
        gen_random(value->mem, 16);

        db_add(db, key, value);
    }

    db_close(db);

    db = db_open(START_DIRECTORY);

    for (int i = 1000000 - 1; i >= 0; i--)
    {
        buffer_clear(key);
        buffer_scatf(key, "%d", i);
        db_get(db, key, value);
    }

    db_close(db);
    buffer_free(key);
    buffer_free(value);

    return 0;
}

