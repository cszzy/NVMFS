CC = gcc
CXX = g++
LDFLAGS = -pthread -g -Wformat=0 -L/opt/daos/lib64 -ldaos -ldaos_common -luuid -lm -lgurt 
DEFS = -I/opt/daos/include  -DNDEBUG
CFLAGS += $(DEFS)
AR = ar
ARFLAGS = rs
LIB_SOURCES =  \
	adaptor.cc  \
	config.cc  \
	inode_format.cc  \
	metafs.cc  
	


LIBOBJECTS = $(LIB_SOURCES:.cc=.o)

TARGET_OBJS = $(LIB_SOURCES:.cc=)

FUSE_INCLUDE = `pkg-config fuse3 --cflags`
FUSE_LIBRARY = `pkg-config fuse3 --libs`


LIBRARY = libmetafs.a


METADB_LIBRARY = ../metadb/libmetadb.a -lpmem
METADB_INCLUDE = -I../metadb/include

.PHONY: clean
default:  all

all:  clean $(LIBOBJECTS) 

clean: 
	rm -f $(LIBOBJECTS)
	rm -f metafs_main
	

$(LIBOBJECTS): 
	for sou_file in $(TARGET_OBJS) ; do \
	$(CXX) $(CFLAGS) $(LDFLAGS) $(METADB_INCLUDE) $(FUSE_INCLUDE) -c $$sou_file.cc -o $$sou_file.o; \
	done;

$(LIBRARY) : $(LIBOBJECTS)
	rm -f $@
	$(AR) $(ARFLAGS) $@ $^

metafs_main: clean $(LIBOBJECTS)
	$(CXX) $(CFLAGS) metafs_main.cc $(LIBOBJECTS) $(METADB_INCLUDE) $(METADB_LIBRARY) $(FUSE_INCLUDE) $(FUSE_LIBRARY) -o metafs_main $(LDFLAGS);
