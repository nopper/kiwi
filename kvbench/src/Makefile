
uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')
OPTIMIZATION?=-O3
DEPENDENCY_TARGETS=leveldb kyotocabinet


WARN=-Wall
OPT=$(OPTIMIZATION)
DEBUG=-g -ggdb

CXXFLAGS=$(OPT) $(WARN) $(DEBUG)

CXXFLAGS+= -I../deps/leveldb/include -I../deps/kyotocabinet -I../../engine


LDFLAGS=$(DEBUG) 

LIBS=../deps/leveldb/libleveldb.a ../../engine/libindexer.a ../deps/kyotocabinet/libkyotocabinet.a -lpthread -lz -lc -lsnappy

ifeq ($(uname_S),Linux)
  CXX=g++
  LD=g++
else
  CXX=c++
  LD=c++
endif

LEVELDB_BENCHMARK_NAME=leveldb-benchmark
LEVELDB_BENCHMARK_OBJ=benchmark.o leveldb.o
KC_BENCHMARK_NAME=kc-benchmark
KC_BENCHMARK_OBJ=benchmark.o kc-benchmark.o
KIWI_BENCHMARK_NAME=kiwi-benchmark
KIWI_BENCHMARK_OBJ=benchmark.o kiwi-benchmark.o

all: $(LEVELDB_BENCHMARK_NAME) $(KC_BENCHMARK_NAME) $(KIWI_BENCHMARK_NAME)
	@echo "Done :)"

.PHONY: all

include Makefile.dep

dep:
	$(CXX) $(CXXFLAGS) -MM *.cc > Makefile.dep

.PHONY: dep

$(LEVELDB_BENCHMARK_NAME): $(LEVELDB_BENCHMARK_OBJ)
	$(LD) -o $@ $^ $(LIBS)

$(KC_BENCHMARK_NAME): $(KC_BENCHMARK_OBJ)
	$(LD) -o $@ $^ $(LIBS)

$(KIWI_BENCHMARK_NAME): $(KIWI_BENCHMARK_OBJ)
	$(LD) -o $@ $^ $(LIBS)

clean:
	-(rm $(LEVELDB_BENCHMARK_NAME) $(LEVELDB_BENCHMARK_OBJ) \
	$(KC_BENCHMARK_NAME) $(KC_BENCHMARK_OBJ))
