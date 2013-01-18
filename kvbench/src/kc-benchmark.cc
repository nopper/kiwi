#include "benchmark.h"
#include "kcpolydb.h"

class KyotoCabinet : public benchmark::Engine {
public:
  void put(const benchmark::Slice &key, const benchmark::Slice &value) {
    db->set(key.data(), key.size(), value.data(), value.size());
  }

  void get(const benchmark::Slice &key, std::string *value) {
    value = db->get(std::string(key.data(), key.size()));
  }
  
  void del(const benchmark::Slice &key) {
    db->remove(key.data(), key.size());
  }

  void init(const char* path, size_t cachesize) {
    int open_options = kyotocabinet::PolyDB::OWRITER |
      kyotocabinet::PolyDB::OCREATE;
    int tune_options = kyotocabinet::TreeDB::TSMALL |
      kyotocabinet::TreeDB::TLINEAR | kyotocabinet::TreeDB::TCOMPRESS; 
    
    db = new kyotocabinet::TreeDB();
    db->tune_page_cache(cachesize);
    db->tune_options(tune_options);
    bool success = db->open(path, open_options);


    assert(success);
  }

  ~KyotoCabinet() { 
    db->close();
    delete db; 
  }
private:
  kyotocabinet::TreeDB *db;
};

int main(int argc, const char** argv) {
  KyotoCabinet *kc = new KyotoCabinet;
  benchmark::Benchmark *bench = new benchmark::Benchmark(kc);
  bench->parseOptions(argc, argv);
  bench->run();
  delete kc;
  delete bench;
  return 0;
}
