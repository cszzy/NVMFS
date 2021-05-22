#ifndef _METAFS_METAFS_H_
#define _METAFS_METAFS_H_

#define FUSE_USE_VERSION 35

#include <fuse3/fuse.h>
#include "inode_format.h"
#include "adaptor.h"
#include "config.h"
#include "metadb/db.h"

using namespace metadb;

namespace metafs {

using namespace std;
class MetaFS {
public :
    MetaFS(const kvfs_args & arg);
    virtual ~MetaFS();

    void * Init(struct fuse_conn_info * conn, struct fuse_config *cfg);

    void Destroy(void * data);

    int GetAttr(const char * path ,struct stat * statbuff, struct fuse_file_info *fi);

    int Open(const char * path ,struct fuse_file_info * fi);

    int Read(const char * path,char * buf , size_t size ,off_t offset ,struct fuse_file_info * fi);

    int Write(const char * path , const char * buf,size_t size ,off_t offset ,struct fuse_file_info * fi);

    int Truncate(const char * path ,off_t offset, struct fuse_file_info *fi);

    int Fsync(const char * path,int datasync ,struct fuse_file_info * fi);

    int Release(const char * path ,struct fuse_file_info * fi);

    int Readlink(const char * path ,char * buf,size_t size);

    int Symlink(const char * target , const char * path);

    int Unlink(const char * path);

    int MakeNode(const char * path,mode_t mode ,dev_t dev);

    int MakeDir(const char * path,mode_t mode);

    int OpenDir(const char * path,struct fuse_file_info *fi);

    int ReadDir(const char * path,void * buf ,fuse_fill_dir_t filler,
            off_t offset ,struct fuse_file_info * fi, enum fuse_readdir_flags flag);

    int ReleaseDir(const char * path,struct fuse_file_info * fi);

    int RemoveDir(const char * path);

    int Rename(const char *new_path,const char * old_path, unsigned int flags);
    
    int Access(const char * path,int mask);

    int Chmod(const char * path , mode_t mode, struct fuse_file_info *fi);
    
    int Chown(const char * path, uid_t uid,gid_t gid, struct fuse_file_info *fi);

    int UpdateTimens(const char * path ,const struct timespec tv[2], struct fuse_file_info *fi);

    int RemoveDirContents(inode_id_t key);

protected:
    inline void InitStat(struct stat &statbuf, inode_id_t inode, mode_t mode, dev_t dev);
                
    inline bool PathLookup(const char *path, inode_id_t &key);
    inline bool PathLookup(const char *path, inode_id_t &key, inode_id_t &parent_id, string &fname);
    inline bool ParentPathLookup(const char *path, inode_id_t &parent_id, string &fname);
    string InitInodeValue(inode_id_t inum, mode_t mode, dev_t dev);

    kvfs_file_handle * InitFileHandle(const char * path, struct fuse_file_info * fi, const inode_id_t & key , const std::string & value );
    string GetDiskFilePath(const inode_id_t &inode_id);
    int OpenDiskFile(const inode_id_t &key, const tfs_inode_header* iheader, int flags);
    int TruncateDiskFile(const inode_id_t &key, off_t new_size);
    ssize_t MigrateDiskFileToBuffer(const inode_id_t &key, char* buffer, size_t size);
    int MigrateToDiskFile(const inode_id_t &key, string &value, int &fd, int flags);

    //daos
    int TruncateDaosFile(const inode_id_t &key, off_t new_size);
    ssize_t MigrateDaosFileToBuffer(const inode_id_t &key, char* buffer, size_t size);
    int ReadDaosArray(daos_handle_t oh, d_sg_list_t *sgl, daos_off_t offset, ssize_t *read_size);
    int MigrateToDaosFile(const inode_id_t &key, string &value, int flags);
    int WriteDaosArray(daos_handle_t oh, d_sg_list_t *sgl, daos_off_t offset);

    DBAdaptor * db_;
    KVFSConfig * config_;
    kvfs_args args_;
    bool use_fuse;

    struct fuse_config *cfg_;

};


} // namespace name







#endif