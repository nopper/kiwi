include ../defs.mk

INDEXER = indexer
STRESS = stress

all: $(INDEXER) $(STRESS)

.PHONY: all

$(INDEXER):
	$(FINAL_LD) main.c -std=c99 -o $@ -lindexer -I../engine -L../engine $^

$(STRESS):
	$(FINAL_LD) stress.c -std=c99 -o $@ -lindexer -I../engine -L../engine $^

%.o: %.c
	$(FINAL_CC) -c $<

clean:
	rm -rf *.o $(STRESS) $(INDEXER)

.PHONY: clean
