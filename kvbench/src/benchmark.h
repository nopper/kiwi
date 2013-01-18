#ifndef _BENCHMARK_H_
#define _BENCHMARK_H_
#include <string>
#include <vector>
#include <pthread.h>
namespace benchmark {
const int MAX_KEY_LEN = 12;

void computePercentile(std::vector<int>* data, int* p99, int* p999, int* p9999, int* max);
struct Latency {
  int p99;
  int p999;
  int p9999;
  int max;
};

template <typename type>
type atomicSwap(type* lhs, type rhs, pthread_mutex_t* lock) {
  type ret;
  ret = *lhs;
  pthread_mutex_lock(lock);
  *lhs = rhs;
  pthread_mutex_unlock(lock);
  return ret;
}

class Slice {
public:
  Slice() : data_(""), size_(0) {}
  Slice(const char* d, size_t n) : data_(d), size_(n) {}
  Slice(const std::string& s) : data_(s.data()), size_(s.size()) {}
  const char* data() const { return data_; }
  size_t size() const { return size_; }
private:
  const char* data_;
  size_t size_;
};

class Benchmark;
class Engine;

struct Worker {
  pthread_t thread;
  int total;
  int get;
  int put;
  int del;
  int rate;
  Benchmark *bench;
  void* (*callable)(void *);
  std::vector<int>* get_latency;
  std::vector<int>* put_latency;
  std::vector<int>* del_latency;
  pthread_mutex_t lock;
  Worker(Benchmark *bench, void* (*callable)(void*)) : 
    total(0), get(0), put(0), del(0),
    bench(bench), callable(callable),
    get_latency(NULL), put_latency(NULL), del_latency(NULL) {
    get_latency = new std::vector<int>;
    put_latency = new std::vector<int>;
    del_latency = new std::vector<int>;
    pthread_mutex_init(&lock, NULL);
  }
  ~Worker() {
    if (get_latency) delete get_latency;
    if (put_latency) delete put_latency;
    if (del_latency) delete del_latency;
  }
  void pushGetLatency(int latency) {
    pthread_mutex_lock(&lock);
    get_latency->push_back(latency);
    pthread_mutex_unlock(&lock);
    get++;
    total++;
  }
  void pushPutLatency(int latency) {
    pthread_mutex_lock(&lock);
    put_latency->push_back(latency);
    pthread_mutex_unlock(&lock);
    put++;
    total++;
  }
  void pushDelLatency(int latency) {
    pthread_mutex_lock(&lock);
    del_latency->push_back(latency);
    pthread_mutex_unlock(&lock);
    del++;
    total++;
  }

  void getLatencyAndReset(Latency* get, Latency* put, Latency *del);  
};



enum CMD_TYPE {READ, WRITE, DELETE};
  
class Benchmark {
public:
  int getratio;
  int putratio;
  int delratio;
  int threads;
  int keysize;
  int datasize;
  int keyrange;
  int rate;
  bool loop;
  std::vector<Worker*> workers;
  Worker* monitor;
  pthread_mutex_t lock;
  int interval;
  int duration; // in minutes
  Engine *engine;
  const char* path;
  size_t cachesize;
  static bool sequential;
  static bool verbose;
  static bool shuttingdown;

  
  Benchmark(Engine *e): getratio(1), putratio(1), delratio(1), threads(1),
                        keysize(12), datasize(8), keyrange(1), 
                        rate(0),loop(false),
                        interval(10), duration(5),
                        engine(e), path("testdb"), 
                        cachesize(4*1048576)
  {
    pthread_mutex_init(&lock, NULL);
  }
  CMD_TYPE randCMD();

  Worker* spawnWorker(void* (*callable)(void*));
  void spawnWorkers();
  void joinWorkers();
  int parseOptions(int argc, const char** argv);
  void cleanup();
  void run();
  void setupSignalHandler();
};


void* benchmark(void *arg);
void* collectStatus(void *arg);
void signalHandler(int signum);
long genInterval(int rate);
class Engine {
public:
  virtual void put(const Slice &key, const Slice &value) = 0;
  
  virtual void get(const Slice &key, std::string *value) = 0;
  
  virtual void del(const Slice &key) = 0;
  
  virtual ~Engine() {};
  
  virtual void init(const char *path, size_t cachesize) = 0;
};


}


#endif /* _BENCHMARK_H_ */
