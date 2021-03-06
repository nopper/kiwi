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


int main(int argc, const char * argv[])
{
    int i;
    DB* db = db_open(START_DIRECTORY);
    Variant* key = buffer_new(256);
    Variant* value = buffer_new(256);

    srand(time(NULL));

//    char skey[64];

//    while (fscanf(stdin, "%s", skey) != EOF)
//    {
//      ADD(skey, skey);
//    }

    ADD("V7", "vertice 0");
    ADD("V6", "vertice 1");
    ADD("E5", "edge 2");

    db_close(db);
    db = db_open(START_DIRECTORY);

    DEL("V7");
    DEL("V6");

    db_close(db);

    db = db_open(START_DIRECTORY);

    DEL("V7");
    DEL("V6");

    db_close(db);

    db = db_open(START_DIRECTORY);
#if 1
    buffer_clear(key);
    buffer_putstr(key, "\0");
    key->length = 1;
    DBIterator* iter = db_iterator_new(db);
    db_iterator_seek(iter, key);

    while (db_iterator_valid(iter))
    {
        Variant* k = db_iterator_key(iter);
        Variant* v = db_iterator_value(iter);
        INFO("====>: %.*s : %.*s", k->length, k->mem, v->length, v->mem);
        db_iterator_next(iter);
    }
    db_iterator_free(iter);
#endif

    GET("E5");
    GET("E5");

    db_close(db);
    buffer_free(key);
    buffer_free(value);

    return 0;
}

