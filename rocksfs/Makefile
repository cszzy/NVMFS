CC = gcc
CXX = g++
LDFLAGS = -pthread -g -Wformat=0 -L/opt/daos/lib64  -ldaos -ldaos_common -luuid -lm -lgurt -ldl -llz4 -lzstd -lz -lpthread -lsnappy -lbz2 
DEFS =  -DNDEBUG -I/opt/daos/include 
CFLAGS += $(DEFS)
AR = ar
ARFLAGS = rs
LIB_SOURCES =  \
	adaptor.cc  \
	config.cc  \
	inode_format.cc  \
	rocksfs.cc  
	


LIBOBJECTS = $(LIB_SOURCES:.cc=.o)

TARGET_OBJS = $(LIB_SOURCES:.cc=)

FUSE_INCLUDE = `pkg-config fuse3 --cflags`
FUSE_LIBRARY = `pkg-config fuse3 --libs`


LIBRARY = librocksfs.a

LEVELDB_LIBRARY = /usr/local/lib/librocksdb.a
LEVELDB_INCLUDE = -I/usr/local/include/rocksdb

.PHONY: clean
default:  all

all:  clean $(LIBOBJECTS) 

clean: 
	rm -f $(LIBOBJECTS)
	rm -f rocksfs_main
	

$(LIBOBJECTS): 
	for sou_file in $(TARGET_OBJS) ; do \
	$(CXX) $(CFLAGS) $(LDFLAGS) $(LEVELDB_INCLUDE) $(FUSE_INCLUDE) -c $$sou_file.cc -o $$sou_file.o; \
	done;

$(LIBRARY) : $(LIBOBJECTS)
	rm -f $@
	$(AR) $(ARFLAGS) $@ $^

rocksfs_main: clean $(LIBOBJECTS)
	$(CXX) $(CFLAGS) rocksfs_main.cc $(LIBOBJECTS) $(LEVELDB_INCLUDE) $(LEVELDB_LIBRARY) $(FUSE_INCLUDE) $(FUSE_LIBRARY) -o rocksfs_main $(LDFLAGS);
