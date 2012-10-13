include ../defs.mk

LIBINDEXER_OBJ = \
	db.o \
	memtable.o \
	indexer.o \
	sst.o \
	sst_builder.o \
	sst_loader.o \
	sst_block_builder.o \
	hash.o \
	bloom_builder.o \
	merger.o \
	compaction.o \
	skiplist.o \
	buffer.o \
	arena.o \
	utils.o \
	crc32.o \
	file.o \
	heap.o \
	vector.o

LIBINDEXER = libindexer.so

all: $(LIBINDEXER)

.PHONY: all

include Makefile.dep

dep:
	$(FINAL_CC) -MM *.c > Makefile.dep

.PHONY: dep

$(LIBINDEXER): $(LIBINDEXER_OBJ)
	$(FINAL_LD) -fPIC -shared -o $@ $^ $(FINAL_LIBS)
	$(FINAL_AR) rcs libindexer.a $(LIBINDEXER_OBJ)

%.o: %.c
	$(FINAL_CC) -fPIC -c $<

clean:
	rm -rf *.o $(LIBINDEXER) libindexer.a

.PHONY: clean
