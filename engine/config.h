#ifndef __CONFIG_H__
#define __CONFIG_H__

// Here they go all the definitions

#define INDEXER_MAX_LOG_MSG 1024
#define MAX_FILENAME 255

#define LEVEL_INFO    0
#define LEVEL_DEBUG   1
#define LEVEL_WARNING 2
#define LEVEL_ERROR   3

#define RESTART_INTERVAL 16
//#define SKIPLIST_SIZE 1024 * 1024 * 100// Actually this is not the memory occupation
//#define MAX_SKIPLIST_ALLOCATION 1024 * 1024 * 70

#define SKIPLIST_SIZE 1000000
#define MAX_SKIPLIST_ALLOCATION 8 * (2<<20)

#define POOL_SIZE 1024 * 8
#define BLOCK_SIZE 4096
#define START_MAP_SIZE 1024

#define START_DIRECTORY "/tmp"

#define MAGIC_STR "pedobear"
#define IS_LITTLE_ENDIAN 1
#define FOOTER_SIZE 40

#define MAX_LEVELS 7
#define MAX_FILES_LEVEL0 4
#define MAX_FILES 100

#define EXPANSION_LIMIT 15 * 2 * 1048576
#define GRANDPARENT_OVERLAP 10 * 2 * 1048576
#define MAX_MEM_COMPACT_LEVEL 2

#define WITH_BLOOM_FILTER
#define BITS_PER_KEY 10
#define NUM_PROBES 7

#define LRU_CACHE_SIZE 100
#define BACKGROUND_MERGE

#endif
