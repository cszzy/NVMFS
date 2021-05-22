CC = gcc
CXX = g++
LDFLAGS = -pthread -g -lpmem -Wformat=0
DEFS = -DNDEBUG
CFLAGS = -I./include
CFLAGS += $(DEFS)
AR = ar
ARFLAGS = rs
LIB_SOURCES =  \
	db/debug.cc  \
	db/dir_db.cc  \
	db/dir_hashtable.cc  \
	db/dir_iterator.cc  \
	db/dir_nvm_node.cc  \
	db/inode_db.cc  \
	db/inode_file.cc  \
	db/inode_hashtable.cc  \
	db/inode_zone.cc  \
	db/metadb.cc  \
	db/nvm_file_allocator.cc  \
	db/nvm_node_allocator.cc  \
	db/thread_pool.cc  \
	util/histogram.cc
	
TEST_TARGET = tests/db_bench \

LIBOBJECTS = $(LIB_SOURCES:.cc=.o)

TARGET_OBJS = $(LIB_SOURCES:.cc=)

LIBRARY = libmetadb.a

LEVELDB_LIBRARY = ../lib/leveldb_v1.20/out-static/libleveldb.a
LEVELDB_INCLUDE = -I../lib/leveldb_v1.20/include

.PHONY: clean
default:  all

all:  clean $(LIBRARY) 

clean: 
	rm -f $(LIBRARY)
	rm -f $(LIBOBJECTS)
	rm -f $(TEST_TARGET)
	rm -f test
	rm -f db_bench

$(LIBOBJECTS): 
	for sou_file in $(TARGET_OBJS) ; do \
	$(CXX) $(CFLAGS) $(LDFLAGS) -c $$sou_file.cc -o $$sou_file.o; \
	done;

$(LIBRARY) : $(LIBOBJECTS)
	rm -f $@
	$(AR) $(ARFLAGS) $@ $^

$(TEST_TARGET) :
	for test_file in $(TEST_TARGET) ; do \
	$(CXX) $(CFLAGS) $$test_file.cc -o $$test_file $(LIBRARY) $(LDFLAGS); \
	done;
db_bench: clean $(LIBRARY)
	$(CXX) $(CFLAGS) tests/db_bench.cc $(LIBRARY) -o db_bench $(LDFLAGS);
test: clean $(LIBRARY)
	$(CXX) $(CFLAGS) tests/write_test.cc $(LIBRARY) -o test $(LDFLAGS);
leveldb_bench: clean $(LIBRARY)
	$(CXX) $(CFLAGS) tests/leveldb_bench.cc $(LIBRARY) $(LEVELDB_INCLUDE) $(LEVELDB_LIBRARY) -o leveldb_bench $(LDFLAGS);

INSTALL_PATH := /usr/local
INCLUDE_PATH := metadb

install-headers:
	install -d $(INSTALL_PATH)/lib
	install -d $(INSTALL_PATH)/include/$(INCLUDE_PATH)
	for header_dir in `find "include/$(INCLUDE_PATH)/" -type d | sed '1d' | sed 's/^include\///g' `; do \
		install -d $(INSTALL_PATH)/include/$(INCLUDE_PATH)/$$header_dir; \
	done
	for header in `find "include/$(INCLUDE_PATH)/" -type f -name "*.h" | sed 's/^include\///g' `; do \
		install -C -m 644 include/$$header $(INSTALL_PATH)/include/$$header; \
	done

install-static: $(LIBRARY) install-headers 
	install -C -m 755 $(LIBRARY) $(INSTALL_PATH)/lib

install:install-static

uninstall:
	rm -rf $(INSTALL_PATH)/include/$(INCLUDE_PATH) \
	$(INSTALL_PATH)/lib/$(LIBRARY) \
