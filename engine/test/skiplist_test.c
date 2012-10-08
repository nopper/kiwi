#include "skiplist.h"

int main(int argc, char *argv[])
{
  SkipList* sl = skiplist_new(10);
  skiplist_insert(sl, "miao", 4, 22, ADD);
  skiplist_insert(sl, "dioe", 4, 22, ADD);
  skiplist_insert(sl, "asdd", 4, 22, ADD);
  skiplist_insert(sl, "miaa", 4, 22, ADD);
  skiplist_insert(sl, "miao", 4, 22, ADD);
  skiplist_insert(sl, "msaa", 4, 22, ADD);
  skiplist_insert(sl, "mwao", 4, 22, ADD);
}
