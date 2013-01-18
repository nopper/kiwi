#include "benchmark.h"

extern "C" {
#include "kiwi/db.h"
#include "kiwi/variant.h"
}

class Kiwi : public benchmark::Engine {
public:
  void put(const benchmark::Slice &key, const benchmark::Slice &value) {
    Variant k, v;
    k.mem = (char *)key.data();
    k.length = key.size();
    v.mem = (char *)value.data();
    v.length = value.size();

    db_add(db, &k, &v);
  }

  void get(const benchmark::Slice &key, std::string *value) {
    Variant k;
    Variant* v = (Variant *)buffer_new(255);
    k.mem = (char *)key.data();
    k.length = key.size();
    db_get(db, &k, v);

    buffer_free(v);
  }
  
  void del(const benchmark::Slice &key) {
    Variant k;
    k.mem = (char *)key.data();
    k.length = key.size();

    db_remove(db, &k);
  }

  void init(const char* path, size_t cachesize) {
    db = db_open(path);
  }

  ~Kiwi() { 
    db_close(db);
  }
private:
  struct _db* db;
};

int main(int argc, const char** argv) {
  Kiwi *kc = new Kiwi;
  benchmark::Benchmark *bench = new benchmark::Benchmark(kc);
  bench->parseOptions(argc, argv);
  bench->run();
  delete kc;
  delete bench;
  return 0;
}
