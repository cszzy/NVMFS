#ifndef _METAFS_INODE_FORMAT_H_
#define _METAFS_INODE_FORMAT_H_

#include <sys/stat.h>
#include <map>
#include <unordered_map>
#include <stdarg.h>

#include "metadb/db.h"
#include "daos.h"



using namespace metadb;
// inode definitions
namespace metafs {

//typedef uint64_t inode_id_t;
//zzytodo
typedef struct stat tfs_stat_t;

const size_t Inode_padding = 104;
static const char PATH_DELIMITER = '/';
static const int INODE_PADDING = 104;

static const inode_id_t ROOT_INODE_ID = 0;
static const char* ROOT_INODE_STAT = "/tmp/";
const uint64_t murmur64_hash_seed = 123;


struct tfs_inode_header {
  tfs_stat_t fstat;
  uint32_t has_blob;//原来用来标记是否存储到磁盘，现在代表存储到daos
};

static const size_t TFS_INODE_HEADER_SIZE = sizeof(tfs_inode_header);
static const size_t TFS_INODE_ATTR_SIZE = sizeof(struct stat);



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
    inode_id_t key;
    int flags;
    kvfs_inode_mode mode;
    // file descriptor for big file
    int fd; 
    //daos obj handle
    daos_handle_t oh;
    std::string value;

    kvfs_file_handle(inode_id_t key) :
        flags(0),fd(-1)//,offset(-1)
    {
        InsertHandle(key,this);
    }



    static kvfs_file_handle * GetHandle(const inode_id_t key);
    static bool InsertHandle(const inode_id_t key, kvfs_file_handle * handle);
    static bool DeleteHandle (const inode_id_t key);
    protected :
    // global hash map for query
    static std::unordered_map <inode_id_t /*path*/, kvfs_file_handle * > handle_map ; 

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