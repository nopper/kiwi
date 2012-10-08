include ../../defs.mk

default: all

DB_CLASS = com/kv/indexer/DB.class
DB_SOURCE = com/kv/indexer/DB.java
DB_H = com_kv_indexer_DB.h
DB_OBJ = com_kv_indexer_DB.o
DB_PACKAGE = com.kv.indexer.DB
LIB = libindexer_java.so

all: $(LIB)

$(DB_CLASS):
	$(FINAL_JAVAC) $(DB_SOURCE)

$(DB_H): $(DB_CLASS)
	$(FINAL_JAVAH) $(DB_PACKAGE)

$(LIB): $(DB_CLASS) $(DB_H) $(DB_OBJ)
	$(FINAL_LD) -shared $(DB_OBJ) -o $(LIB)

%.o: %.c
	$(FINAL_CC) -I${JAVA_HOME}/include -I../../engine -I. -c $<

clean:
	rm -rf $(DB_CLASS) $(DB_H) $(DB_OBJ) $(LIB)
