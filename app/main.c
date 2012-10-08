#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include "db.h"
#include "sst_loader.h"

int main(int argc, const char * argv[])
{
    DB* db = db_open(START_DIRECTORY);
    Variant* key = buffer_new(256);
    Variant* value = buffer_new(256);


#if 0

    size_t read;
    size_t len;
    char* line = NULL;

    while ((read = getline(&line, &len, stdin)) != -1)
    {
        if (key->length == 0)
        {
            buffer_clear(value);
            buffer_putnstr(key, line, read - 1);
        }
        else if (value->length == 0)
        {
            buffer_putnstr(value, line, read - 1);
            INFO("Key: %.*s Value: %.*s", key->length, key->mem, value->length, value->mem);

            buffer_clear(key);
            buffer_clear(value);
        }
    }

    if (line)
        free(line);

    //sst_compact(db->sst);

//    SSTLoaderIterator* iter = sst_loader_iterator((*db->sst->sorted_files)->loader);

//    for (; sst_loader_iterator_valid(iter); sst_loader_iterator_next(iter))
//        INFO("Key: %.*s", iter->key->length, iter->key->mem);

#else

#if 1
#include "generated.c"
#else
    buffer_putnstr(key,   "Brittney", 8);
    db_get(db, key, value);
    if (memcmp(value->mem, "diomadonna", 10) != 0) { int3(); printf("ERMHAGHERD: ERROR: %.*s != %.*s\n", value->length, value->mem, 10, "diomadonna");}
#endif

#endif
    db_close(db);
    return 0;
}

