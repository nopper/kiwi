include ../defs.mk

INDEXER_OBJ = main.o
INDEXER = indexer

all: $(INDEXER)

.PHONY: all

$(INDEXER): $(LIBINDEXER)
	$(FINAL_LD) main.c -o $@ -lindexer -I../engine -L../engine $^

%.o: %.c
	$(FINAL_CC) -c $<

clean:
	rm -rf *.o $(LIBINDEXER) $(INDEXER)

.PHONY: clean
