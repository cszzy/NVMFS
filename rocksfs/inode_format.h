#ifndef _ROCKSFS_INODE_FORMAT_H_
#define _ROCKSFS_INODE_FORMAT_H_

#include <sys/stat.h>
#include <map>
#include <unordered_map>
#include <stdarg.h>

#include "rocksdb/db.h"




// inode definitions
namespace rocksfs {

typedef uint64_t tfs_inode_t;
typedef uint64_t tfs_hash_t;
typedef struct stat tfs_stat_t;

const size_t Inode_padding = 104;
static const char PATH_DELIMITER = '/';
static const int INODE_PADDING = 104;
static const tfs_inode_t ROOT_INODE_ID = 0;
static const char* ROOT_INODE_STAT = "/tmp/";
const uint64_t murmur64_hash_seed = 123;

// 
struct tfs_meta_key_t
{
    tfs_inode_t inode_id;
    tfs_hash_t hash_id;


    const std::string ToString() const 
    {
        return std::string((const char *)this,sizeof(tfs_meta_key_t));
    }
    rocksdb::Slice ToSlice() const {
        return rocksdb::Slice((const char *)this,sizeof(tfs_meta_key_t));
    }
};

struct tfs_inode_header {
  tfs_stat_t fstat;
//   char padding[INODE_PADDING];
  uint32_t has_blob;
  uint32_t namelen;
};

static const size_t TFS_INODE_HEADER_SIZE = sizeof(tfs_inode_header);
static const size_t TFS_INODE_ATTR_SIZE = sizeof(struct stat);



struct tfs_inode_val_t {
  size_t size;
  char* value;

  tfs_inode_val_t() : value(NULL), size(0) {}

  rocksdb::Slice ToSlice() const {
    return rocksdb::Slice((const char *) value, size);
  }

  std::string ToString() const {
    return std::string(value, size);
  }
};


enum kvfs_inode_mode {
    INODE_READ = 0 ,
    INODE_DELETE = 1 ,
    INODE_WRITE = 2,
};


// file handle use in memory 
// for fuse 
// global reference 
struct kvfs_file_handle
{
    tfs_meta_key_t key;
    int flags;
    kvfs_inode_mode mode;
    // file descriptor for big file
    int fd; 
    std::string value;

    kvfs_file_handle(const char * path) :
        flags(0),fd(-1)//,offset(-1)
    {
        InsertHandle(std::string(path),this);
    }



    static kvfs_file_handle * GetHandle(const std::string & path);
    static bool InsertHandle(const std::string & path, kvfs_file_handle * handle);
    static bool DeleteHandle (const std::string & path);
    protected :
    // global hash map for query
    static std::unordered_map <std::string /*path*/, kvfs_file_handle * > handle_map ; 

};

uint64_t murmur64( const void * key, int len, uint64_t seed = 123 );


// #define KVFSDEBUG

#ifndef KVFSDEBUG
#define KVFS_LOG(...)
#else 
class KVFSLogger {
    public:
    static KVFSLogger * GetInstance();
    void Log(const char *file_name,int line ,const char * data ,  ...);
    protected:
    KVFSLogger() :
        logfile_name("fs_log.log")
    {
        fp = fopen(logfile_name.c_str(),"w");
        assert(fp);

    };
    std::string logfile_name;
    FILE * fp;


    static KVFSLogger * instance_;
};

#define KVFS_LOG(...) KVFSLogger::GetInstance()->Log(__FILE__,__LINE__,__VA_ARGS__)

#endif // NDEBUG


}








#endif