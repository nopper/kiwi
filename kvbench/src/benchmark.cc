#include <sys/time.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <algorithm>
#include <math.h>
#include <assert.h>
#include "benchmark.h"

namespace benchmark {

bool Benchmark::verbose = false;
bool Benchmark::shuttingdown = false;
bool Benchmark::sequential = false;

static long long ustime(void) {
    struct timeval tv;
    long long ust;

    gettimeofday(&tv, NULL);
    ust = ((long)tv.tv_sec)*1000000;
    ust += tv.tv_usec;
    return ust;
}



static long long mstime(void) {
    struct timeval tv;
    long long mst;

    gettimeofday(&tv, NULL);
    mst = ((long)tv.tv_sec)*1000;
    mst += tv.tv_usec/1000;
    return mst;
}

static void randomizeKey(char *key, int range) {
  size_t r;
  r = random() % range;
  snprintf(key, MAX_KEY_LEN+1, "%012zu", r);
}


void* benchmark(void *arg) {
  Worker *w = (Worker*)arg;
  Benchmark *b = w->bench;
  Engine *e = b->engine;
  CMD_TYPE cmd;
  long long start;
  int latency;
  int rate = w->rate;
  char buf[MAX_KEY_LEN+1];
  std::string value;
  value.reserve(b->datasize);
  size_t lastvalue = 0;
  
  
  while (!Benchmark::shuttingdown) {
    if (Benchmark::sequential)
        snprintf(buf, MAX_KEY_LEN+1, "%012zu", lastvalue++);
    else
        randomizeKey(buf, b->keyrange);
    cmd = b->randCMD();
    start = ustime();
    if (cmd == READ) {
      e->get(Slice(buf, MAX_KEY_LEN), &value);
      latency = (int)(ustime() - start);
      w->pushGetLatency(latency);
      
    } else if (cmd == WRITE) {
      e->put(Slice(buf, MAX_KEY_LEN), value);
      latency = (int)(ustime() - start);
      w->pushPutLatency(latency);
      
    } else {
      e->del(Slice(buf, MAX_KEY_LEN));
      latency = (int)(ustime() - start);
      w->pushDelLatency(latency);
    }
    if (rate) {
      long interval = genInterval(rate);
      if (interval>latency) {
        interval-=latency;
        usleep(interval);
      }
    }
  }

  pthread_exit(NULL);
}

void Benchmark::setupSignalHandler() {
  if (signal(SIGINT, signalHandler) == SIG_ERR) {
    fprintf(stderr, "Failed to setup signal handler\n");
    exit(1);
  }
}

void Worker::getLatencyAndReset(Latency* get, Latency* put, Latency *del) {
  std::vector<int> *latency, *temp;
  if (get) {
    latency = get_latency;
    temp = new std::vector<int>;
    pthread_mutex_lock(&lock);
    get_latency = temp;
    pthread_mutex_unlock(&lock);
    assert(latency != NULL);
    computePercentile(latency, &(get->p99), &(get->p999), &(get->p9999), &(get->max));
    delete latency;
 
  }
  if (put) {
    latency = put_latency;
    temp = new std::vector<int>;
    pthread_mutex_lock(&lock);
    put_latency = temp;
    pthread_mutex_unlock(&lock);
    assert(latency != NULL);
    computePercentile(latency, &(put->p99), &(put->p999), &(put->p9999), &(put->max));
    delete latency;
 
  }
  if (del) {
    latency = del_latency;
    temp = new std::vector<int>;
    pthread_mutex_lock(&lock);
    del_latency = temp;
    pthread_mutex_unlock(&lock);
    assert(latency != NULL);
    computePercentile(latency, &(del->p99), &(del->p999), &(del->p9999), &(del->max));
    delete latency;
  }
}

void signalHandler(int signum) {
  Benchmark::shuttingdown = true;
}    
void* collectStatus(void *arg) {
  Worker *w = (Worker*)arg;
  Benchmark *b = w->bench;
  long long total, last_total = 0, delta;
  long long last_time = 0, time, delta_time;
  long long throughput;
  long long start, end;
  Latency get, put, del;
  start = mstime();
  end = start + b->duration*60*1000;
  
  while (!Benchmark::shuttingdown) {
    time = mstime();
    if (time >= end) break;
    
    total = 0;
    for (int i = 0; i < b->workers.size(); i++) {
      total += b->workers[i]->total;
      b->workers[i]->getLatencyAndReset(&get, &put, &del);
    }
    
    
    if (last_total && last_time) {
      delta = total - last_total;
      delta_time = time - last_time;
      throughput = delta*1000/delta_time;
      printf("%lldqps "
             "get:%d/%d/%d/%d(us) "
             "put:%d/%d/%d/%d(us) "
             "del:%d/%d/%d/%d(us)\n",
             throughput, 
             get.p99, get.p999, get.p9999, get.max,
             put.p99, put.p999, put.p9999, put.max,
             del.p99, del.p999, del.p9999, del.max);
    }
    last_total = total;
    last_time = time;
    sleep(b->interval);
  }
  Benchmark::shuttingdown = true;
  pthread_exit(NULL);
}

void Benchmark::run() {
  setupSignalHandler();
  engine->init(path, cachesize);
  spawnWorkers();
  joinWorkers();
  cleanup();
}
Worker* Benchmark::spawnWorker(void* (*callable)(void*)) {
  pthread_attr_t attr;
  int ret;

  Worker *w = new Worker(this, callable);
  w->rate = rate/threads;
  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
  ret = pthread_create(&(w->thread), &attr, callable, (void*)w);
  pthread_attr_destroy(&attr);
  if (ret){
    printf("ERROR; return code from pthread_create() is %d\n", ret);
    exit(1);
  }
  return w;
}

CMD_TYPE Benchmark::randCMD() {
  int sum = getratio + putratio + delratio;
  int r = random() % sum;
  if (r < getratio) {
    return READ;
  }
  else if (r >= getratio && r < (getratio+putratio))
    return WRITE;
  else 
    return DELETE;
}

void Benchmark::spawnWorkers() {
  Worker *worker;
  for (int i = 0; i < threads; i++) {
    worker = spawnWorker(benchmark);
    if (worker) {
      workers.push_back(worker);
    }
  }
  worker = spawnWorker(collectStatus);
  if (worker)
    monitor = worker;
}

void Benchmark::joinWorkers() {
  void *status;
  int ret;
  for (int i = 0; i < workers.size(); i++) {
    ret = pthread_join(workers[i]->thread, &status);
  }
  ret = pthread_join(monitor->thread, &status);
}


int Benchmark::parseOptions(int argc, const char** argv) {
  bool lastarg;
  int i;
  for (i = 1; i < argc; i++) {
    lastarg = (i == (argc-1));
    if (!strcmp(argv[i], "-get")) {
      if (lastarg) goto invalid;
      getratio = atoi(argv[++i]);
    } else if (!strcmp(argv[i], "-put")) {
      if (lastarg) goto invalid;
      putratio = atoi(argv[++i]);
    } else if (!strcmp(argv[i], "-del")) {
      if (lastarg) goto invalid;
      delratio = atoi(argv[++i]);
    } else if (!strcmp(argv[i], "-threads")) {
      if (lastarg) goto invalid;
      threads = atoi(argv[++i]);
    } else if (!strcmp(argv[i], "-data")) {
      if (lastarg) goto invalid;
      datasize = atoi(argv[++i]);
    } else if (!strcmp(argv[i], "-range")) {
      if (lastarg) goto invalid;
      keyrange = atoi(argv[++i]);
    } else if (!strcmp(argv[i], "-l")) {
      loop = true;
    } else if (!strcmp(argv[i], "-i")) {
      if (lastarg) goto invalid;
      interval = atoi(argv[++i]);
    } else if (!strcmp(argv[i], "-v")) {
      verbose = true;
    } else if (!strcmp(argv[i], "-cache")) {
      if (lastarg) goto invalid;
      cachesize = atoi(argv[++i]);
    } else if (!strcmp(argv[i], "-path")) {
      if (lastarg) goto invalid;
      path = strdup(argv[++i]);
    } else if (!strcmp(argv[i], "-rate")) {
       if (lastarg) goto invalid;
       rate = atoi(argv[++i]);
    } else if (!strcmp(argv[i], "-duration")) {
       if (lastarg) goto invalid;
       duration = atoi(argv[++i]);
    } else if (!strcmp(argv[i], "-sequential")) {
        sequential = true;
    } else {
      goto invalid;
    }
  }
  return i;
 invalid:
  printf("Invalid option \"%s\" or option argument missing\n\n", argv[i]);
  printf(
         "Usage: benchmark -get <ratio> -put <ratio> -del <ratio> "
         "[-threads <num>] [-data <size>] [-range <range>] "
         "[-path <path>] [-duration <minutes>] [-rate <qps>] "
         "[-cache <size>] "
         "[-l] [-i <interval>] [-v]\n\n"
         );

  exit(1);
}


void Benchmark::cleanup() {
  for (int i = 0; i < workers.size(); i++) {
    delete workers[i];
  }
  delete monitor;
}

long genInterval(int rate) {
  long intr_us = (long)(log(1-(double)random() / (double)RAND_MAX)/(-(double)60*rate)
                 *1000000);
  return intr_us;  
}
void computePercentile(std::vector<int>* data, int* p99, int* p999, int* p9999, int* max) {
  assert(data != NULL);
  size_t size = data->size();
  if (data->size() == 0) {
    *p99 = *p999 = *p9999 = *max = 0;
    return;
  }
  sort(data->begin(), data->end());
  if (p99) {
    *p99 = (*data)[(size_t)(size*0.99)];
  }
  if (p999)
    *p999 = (*data)[(size_t)(size*0.999)];
  if (p9999)
    *p9999 = (*data)[(size_t)(size*0.9999)];
  if (max)
    *max = (*data)[size-1];
}

  
}

