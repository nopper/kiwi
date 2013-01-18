#include "benchmark.h"
#include "leveldb/db.h"
#include "leveldb/cache.h"
#include "leveldb/filter_policy.h"

class LevelDB : public benchmark::Engine {
public:
  void put(const benchmark::Slice &key, const benchmark::Slice &value) {
    leveldb::Status s = db->Put(leveldb::WriteOptions(),
                                leveldb::Slice(key.data(), key.size()),
                                leveldb::Slice(value.data(), value.size()));
  }
  void get(const benchmark::Slice &key, std::string *value) {
    leveldb::Status s = db->Get(leveldb::ReadOptions(), 
                                leveldb::Slice(key.data(), key.size()),
                                value);
  }
  void del(const benchmark::Slice &key) {
    leveldb::Status s = db->Delete(leveldb::WriteOptions(),
                                leveldb::Slice(key.data(), key.size()));

  }
  void init(const char *path, size_t cachesize) {
    leveldb::Options options;
    options.filter_policy = leveldb::NewBloomFilterPolicy(10);
    options.block_cache = leveldb::NewLRUCache(cachesize);  // 100MB cache
    options.create_if_missing = true;
    leveldb::Status status = leveldb::DB::Open(options, path, &db);
    assert(status.ok());
  }
  
  ~LevelDB() { delete db; }

private:
  leveldb::DB *db;
};

int main(int argc, const char** argv) {
  LevelDB *leveldb = new LevelDB;
  benchmark::Benchmark *bench = new benchmark::Benchmark(leveldb);
  bench->parseOptions(argc, argv);
  bench->run();
  delete leveldb;
  delete bench;
  return 0;
}
