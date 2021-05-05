#include "rocksfs.h"
#include <cstring>
#include <sys/types.h>
#include <unistd.h>
#include <limits>
#include <algorithm>
#include <errno.h>

#define EDBERROR 188  //DB错误
#define ENOTIMPLEMENT 189  //未实现

using namespace std;
namespace rocksfs {

void RocksFS::BuildMetaKey(const tfs_inode_t inode_id,
                                const tfs_hash_t hash_id,
                                tfs_meta_key_t &key) {
    key.inode_id = inode_id;
    key.hash_id = hash_id;
}

void RocksFS::BuildMetaKey(const char *path,
                                const int len,
                                const tfs_inode_t inode_id,
                                tfs_meta_key_t &key) {
  BuildMetaKey(inode_id, murmur64(path, len, inode_id), key);
}

inline static bool IsKeyInDir(const rocksdb::Slice &key,
                              const tfs_meta_key_t &dirkey) {
  const tfs_meta_key_t* rkey = (const tfs_meta_key_t *) key.data();
  return rkey->inode_id == dirkey.inode_id;
//  return (strncmp(key.data(), dirkey.str, 8) == 0);
}

const tfs_inode_header *GetInodeHeader(const std::string &value) {
  return reinterpret_cast<const tfs_inode_header*> (value.data());
}

const tfs_stat_t *GetAttribute(std::string &value) {
  return reinterpret_cast<const tfs_stat_t*> (value.data());
}

size_t GetInlineData(std::string &value, char* buf, size_t offset, size_t size) {
  const tfs_inode_header* header = GetInodeHeader(value);
  size_t realoffset = TFS_INODE_HEADER_SIZE + header->namelen + 1 + offset;
  if (realoffset < value.size()) {
    if (realoffset + size > value.size()) {
      size = value.size() - realoffset;
    }
    memcpy(buf, value.c_str() + realoffset , size);
    return size;
  } else {
    return 0;
  }
}

void UpdateIhandleValue(std::string &value,
                        const char* buf, size_t offset, size_t size) {
  if (offset > value.size()) {
    value.resize(offset);
  }
  value.replace(offset, size, buf, size);
}

void UpdateInodeHeader(std::string &value,
                       tfs_inode_header &new_header) {
  UpdateIhandleValue(value, (const char *) &new_header,
                     0, TFS_INODE_HEADER_SIZE);
}

void UpdateAttribute(std::string &value,
                     const tfs_stat_t &new_fstat) {
  UpdateIhandleValue(value, (const char *) &new_fstat,
                     0, TFS_INODE_ATTR_SIZE);
}

void UpdateInlineData(std::string &value,
                      const char* buf, size_t offset, size_t size) {
  const tfs_inode_header* header = GetInodeHeader(value);
  size_t realoffset = TFS_INODE_HEADER_SIZE + header->namelen + 1 + offset;
  UpdateIhandleValue(value, buf, realoffset, size);
}

void TruncateInlineData(std::string &value, size_t new_size) {
  const tfs_inode_header* header = GetInodeHeader(value);
  size_t target_size = TFS_INODE_HEADER_SIZE + header->namelen + new_size + 1;
  value.resize(target_size);
}

void DropInlineData(std::string &value) {
  const tfs_inode_header* header = GetInodeHeader(value);
  size_t target_size = TFS_INODE_HEADER_SIZE + header->namelen + 1;
  value.resize(target_size);
}

//文件已存在
int RocksFS::TruncateDaosFile(const uint64_t &key, off_t new_size) {
  KVFS_LOG("TruncateDaosFile:size:%llu",new_size);
  daos_handle_t oh;
  daos_obj_id_t oid;
  int ret;

  config_->GetDaosObjId(&oid, key);
  // ret = daos_obj_open(config_->coh, oid, DAOS_OO_RW, &oh, NULL);
  // KVFS_LOG("daos_array_open_with_attr");
  // ret = daos_array_open_with_attr(config_->coh, oid, DAOS_TX_NONE, DAOS_OO_RW, 1, FS_DEFAULT_CHUNK_SIZE, &oh, NULL);
  KVFS_LOG("daos_array_open");
  daos_size_t cell_size, chunk_size;
  ret = daos_array_open(config_->coh, oid, DAOS_TX_NONE, DAOS_OO_RW, &cell_size , &chunk_size, &oh, NULL);
  if(ret != 0){
    KVFS_LOG("error");
  }
  ret = daos_array_set_size(oh, DAOS_TX_NONE, new_size, NULL);
  if(ret){
    daos_array_close(oh, NULL);
    KVFS_LOG("error");
  }
  ret = daos_array_close(oh, NULL);
  if(ret != 0){
    KVFS_LOG("error");
  }
  return ret;
}

//先读取Daos obj到buffer，然后将daos obj删除
ssize_t RocksFS::MigrateDaosFileToBuffer(const uint64_t &key, char* buffer, size_t size) {
  KVFS_LOG("MigrateDaosFileToBuffer:size:%llu",size);
  daos_handle_t oh;
  daos_obj_id_t oid;
  ssize_t actual;
	d_iov_t iov;
	d_sg_list_t sgl;
  int ret;

  config_->GetDaosObjId(&oid, key);
  // ret = daos_obj_open(config_->coh, oid, DAOS_OO_RW, &oh, NULL);
  // KVFS_LOG("daos_array_open_with_attr");
  // ret = daos_array_open_with_attr(config_->coh, oid, DAOS_TX_NONE, DAOS_OO_RW, 1, FS_DEFAULT_CHUNK_SIZE, &oh, NULL);
  KVFS_LOG("daos_array_open");
  daos_size_t cell_size, chunk_size;
  ret = daos_array_open(config_->coh, oid, DAOS_TX_NONE, DAOS_OO_RW, &cell_size , &chunk_size, &oh, NULL);
  if(ret != 0){
    KVFS_LOG("error");
  }

	/** set memory location */
	sgl.sg_nr = 1;
	sgl.sg_nr_out = 0;
	d_iov_set(&iov, buffer, size);
	sgl.sg_iovs = &iov;

	ret = ReadDaosArray(oh, &sgl, 0, &actual);
	if(ret != 0){
    KVFS_LOG("error");
  }

  // ret = daos_obj_punch(oh, DAOS_TX_NONE, 0, NULL);
  ret = daos_array_destroy(oh, DAOS_TX_NONE, NULL);
  if(ret){
    daos_array_close(oh, NULL);
    KVFS_LOG("error");
  }

  ret = daos_array_close(oh, NULL);
  if(ret != 0){
    KVFS_LOG("error");
  }
  // assert(ret == 0);

	return actual;
}

int RocksFS::ReadDaosArray(daos_handle_t oh, d_sg_list_t *sgl, daos_off_t offset, ssize_t *read_size) {
  KVFS_LOG("ReadDaosArray:offset:%llu, size:%llu",offset, read_size);
  daos_size_t		buf_size = 0;
  int i;
  int ret;

	for (i = 0; i < sgl->sg_nr; i++)
		buf_size += sgl->sg_iovs[i].iov_len;

  if (buf_size == 0) {
		*read_size = 0;
		return 0;
	}

  daos_array_iod_t	iod;
  daos_range_t		rg;
  /** set array location */
  iod.arr_nr = 1;
  rg.rg_len = buf_size;
  rg.rg_idx = offset;
  iod.arr_rgs = &rg;
  KVFS_LOG("daos_array_read");
  ret = daos_array_read(oh, DAOS_TX_NONE, &iod, sgl, NULL);
  if(ret != 0){
    KVFS_LOG("error");
  }

  *read_size = iod.arr_nr_read;
  return 0;
}

//zzy:flags没有使用
int RocksFS::MigrateToDaosFile(const uint64_t &key, string &value, int flags) {
  KVFS_LOG("MigrateToDaosFile");
  const tfs_inode_header* iheader = GetInodeHeader(value);
  int ret = 0;
  if (iheader->fstat.st_size > 0 ) {
    const char* buffer = (const char *) iheader +
                         (TFS_INODE_HEADER_SIZE);
    //write to daos, resize daos array, update stat->st_size
    daos_handle_t oh;
    daos_obj_id_t oid;
    d_iov_t iov;
    d_sg_list_t sgl;
    int ret;

    config_->GetDaosObjId(&oid, key);

    /** set memory location */
    sgl.sg_nr = 1;
    sgl.sg_nr_out = 0;
    d_iov_set(&iov, (void *)buffer, iheader->fstat.st_size);
    sgl.sg_iovs = &iov;

    // ret = daos_obj_open(config_->coh, oid, DAOS_OO_RW, &oh, NULL);
    // KVFS_LOG("daos_array_open_with_attr");
    // ret = daos_array_open_with_attr(config_->coh, oid, DAOS_TX_NONE, DAOS_OO_RW, 1, FS_DEFAULT_CHUNK_SIZE, &oh, NULL);
    // KVFS_LOG("daos_array_open");
    // daos_size_t cell_size, chunk_size;
    // ret = daos_array_open(config_->coh, oid, DAOS_TX_NONE, DAOS_OO_RW, &cell_size , &chunk_size, &oh, NULL);
    // if(ret != 0){
    //   KVFS_LOG("error");
    // }
    KVFS_LOG("daos_array_create");
    ret = daos_array_create(config_->coh, oid, DAOS_TX_NONE, 1, FS_DEFAULT_CHUNK_SIZE, &oh, NULL);
    if(ret != 0){
      KVFS_LOG("error");
    }

    ret = WriteDaosArray(oh, &sgl, 0);
    if(ret != 0){
      KVFS_LOG("error");
    }

    ret = daos_array_close(oh, NULL);
    if(ret != 0){
      KVFS_LOG("error");
    }
    // assert(ret == 0);

    DropInlineData(value);
    
  }
  return ret;
}

int RocksFS::WriteDaosArray(daos_handle_t oh, d_sg_list_t *sgl, daos_off_t offset) {
  daos_size_t		buf_size = 0;
  int i;
  int ret;

	for (i = 0; i < sgl->sg_nr; i++)
		buf_size += sgl->sg_iovs[i].iov_len;

  if (buf_size == 0) {
		return 0;
	}

  daos_array_iod_t	iod;
  daos_range_t		rg;

	/** set array location */
	iod.arr_nr = 1;
	rg.rg_len = buf_size;
	rg.rg_idx = offset;
	iod.arr_rgs = &rg;
  KVFS_LOG("daos_array_write");
  ret = daos_array_write(oh, DAOS_TX_NONE, &iod, sgl, NULL);
  if(ret != 0){
    KVFS_LOG("error");
  }

  return ret;
}


void RocksFS::InitStat(tfs_stat_t &statbuf,
                       tfs_inode_t inode,
                       mode_t mode,
                       dev_t dev) {
    statbuf.st_ino = inode;
    statbuf.st_mode = mode;
    statbuf.st_dev = dev;

    if (use_fuse) {
        statbuf.st_gid = cfg_->gid;
        statbuf.st_uid = cfg_->uid;
    } else {
        statbuf.st_gid = 0;
        statbuf.st_uid = 0;
    }

    statbuf.st_size = 0;
    statbuf.st_blksize = 0;
    statbuf.st_blocks = 0;
    if S_ISREG(mode) {
        statbuf.st_nlink = 1;
    } else {
        statbuf.st_nlink = 2;
    }
    time_t now = time(NULL);
    statbuf.st_atim.tv_nsec = 0;
    statbuf.st_mtim.tv_nsec = 0;
    statbuf.st_ctim.tv_sec = now;
    statbuf.st_ctim.tv_nsec = 0;
}

tfs_inode_val_t RocksFS::InitInodeValue(tfs_inode_t inum,
                                        mode_t mode,
                                        dev_t dev,
                                        rocksdb::Slice filename) {
    tfs_inode_val_t ival;
    ival.size = TFS_INODE_HEADER_SIZE + filename.size() + 1;
    ival.value = new char[ival.size];
    tfs_inode_header* header = reinterpret_cast<tfs_inode_header*>(ival.value);
    InitStat(header->fstat, inum, mode, dev);
    header->has_blob = 0;
    header->namelen = filename.size();
    char* name_buffer = ival.value + TFS_INODE_HEADER_SIZE;
    memcpy(name_buffer, filename.data(), filename.size());
    name_buffer[header->namelen] = '\0';
    return ival;
}

std::string RocksFS::InitInodeValue(const std::string& old_value,
                                    rocksdb::Slice filename) {
  //TODO: Optimize avoid too many copies
  std::string new_value = old_value;
  tfs_inode_header header = *GetInodeHeader(old_value);
  new_value.replace(TFS_INODE_HEADER_SIZE, header.namelen+1,
                    filename.data(), filename.size()+1);
  header.namelen = filename.size();
  UpdateInodeHeader(new_value, header);
  return new_value;
}

RocksFS::RocksFS(const kvfs_args & args) : args_(args), db_(nullptr),config_(nullptr),use_fuse(false)
{
    KVFS_LOG("init config..\n");
    config_  = new KVFSConfig();
    config_->Init(args_);

    KVFS_LOG("init config..\n");

    KVFS_LOG("init rocksdb adaptor ..\n");
    db_ = new DBAdaptor();
    db_->Init(config_->GetMetaDir());
    cfg_ = nullptr;
}

RocksFS::~RocksFS(){
  KVFS_LOG("FS exit\n");

}

void RocksFS::FreeInodeValue(tfs_inode_val_t &ival) {
  if (ival.value != NULL) {
    delete [] ival.value;
    ival.value = NULL;
  }
}

bool RocksFS::ParentPathLookup(const char *path,
                               tfs_meta_key_t &key,
                               tfs_inode_t &inode_in_search,
                               const char* &lastdelimiter) {
    const char* lpos = path;
    const char* rpos;
    bool flag_found = true;
    std::string item;
    inode_in_search = ROOT_INODE_ID;
    while ((rpos = strchr(lpos+1, PATH_DELIMITER)) != NULL) {
        if (rpos - lpos > 0) {
            BuildMetaKey(lpos+1, rpos-lpos-1, inode_in_search, key);
            std::string result;
            int ret = db_->Get(key.ToSlice(), result);
            if (ret == 0) {
                inode_in_search = GetAttribute(result)->st_ino;
            } else if (ret == 1){
                errno = ENOENT;
                flag_found = false;
            } else{
               errno = EDBERROR;
               flag_found = false;
            }
            
            if (!flag_found) {
                return false;
            }
        }
        lpos = rpos;
    }
    if (lpos == path) {
        BuildMetaKey(NULL, 0, ROOT_INODE_ID, key);
    }
    lastdelimiter = lpos;
    return flag_found;
}

bool RocksFS::PathLookup(const char *path,
                         tfs_meta_key_t &key) {
  const char* lpos;
  tfs_inode_t inode_in_search;
  if (ParentPathLookup(path, key, inode_in_search, lpos)) {
    const char* rpos = strchr(lpos, '\0');
    if (rpos != NULL && rpos-lpos > 1) {
      BuildMetaKey(lpos+1, rpos-lpos-1, inode_in_search, key);
    }
    return true;
  } else {
    return false;
  }
}

bool RocksFS::PathLookup(const char *path,
                         tfs_meta_key_t &key,
                         rocksdb::Slice &filename) {
  const char* lpos;
  tfs_inode_t inode_in_search;
  if (ParentPathLookup(path, key, inode_in_search, lpos)) {
    const char* rpos = strchr(lpos, '\0');
    if (rpos != NULL && rpos-lpos > 1) {
      BuildMetaKey(lpos+1, rpos-lpos-1, inode_in_search, key);
      filename = rocksdb::Slice(lpos+1, rpos-lpos-1);
    } else {
      filename = rocksdb::Slice(lpos, 1);
    }
    return true;
  } else {
    return false;
  }
}
string RocksFS::GetDiskFilePath(const tfs_meta_key_t &key, tfs_inode_t inode_id){
  string path = config_->GetDataDir() + "/" + std::to_string(inode_id) + "_" +std::to_string(key.hash_id);
  return path;
}
int RocksFS::OpenDiskFile(const tfs_meta_key_t &key, const tfs_inode_header* iheader, int flags) {
  string fpath = GetDiskFilePath(key, iheader->fstat.st_ino);
  fpath += '\0';
  int fd = open(fpath.c_str(), flags | O_CREAT, iheader->fstat.st_mode);
  if(fd < 0) {
    KVFS_LOG("Open: cant open data file %s fd:%d",fpath.c_str(), fd);
  }
  return fd;
}

int RocksFS::TruncateDiskFile(const tfs_meta_key_t &key, tfs_inode_t inode_id, off_t new_size) {
  string fpath = GetDiskFilePath(key, inode_id);
  fpath += '\0';
  return truncate(fpath.c_str(), new_size);
}

ssize_t RocksFS::MigrateDiskFileToBuffer(const tfs_meta_key_t &key, tfs_inode_t inode_id,
                                         char* buffer,
                                         size_t size) {
  string fpath = GetDiskFilePath(key, inode_id);
  fpath += '\0';
  int fd = open(fpath.c_str(), O_RDONLY);
  ssize_t ret = pread(fd, buffer, size, 0);
  close(fd);
  unlink(fpath.c_str());
  return ret;
}


void* RocksFS::Init(struct fuse_conn_info *conn, struct fuse_config *cfg){
    KVFS_LOG("kvfs init .. \n");
    cfg_ = cfg;

    //daos_init
    int ret;
    KVFS_LOG("KVFSConfig : Init daos ..  ");
    ret = daos_init();
    if(ret != 0){
        KVFS_LOG("error,ret:%d", ret);
        assert(1);
    }

    KVFS_LOG("KVFSConfig : Connect to pool ..  ");
    ret = daos_pool_connect(config_->pool_uuid, NULL, DAOS_PC_RW, &config_->poh, &config_->pool_info, NULL);
    if(ret != 0){
        KVFS_LOG("error,ret:%d", ret);
        assert(1);
    }

    KVFS_LOG("KVFSConfig : Connect to container ..  ");
    ret = daos_cont_open(config_->poh, config_->cont_uuid, DAOS_COO_RW, &config_->coh, &config_->cont_info, NULL);
    if(ret != 0){
        KVFS_LOG("error,ret:%d",ret);
        assert(1);
    }

    if(conn != nullptr)
    {
        KVFS_LOG("use fuse .. true");
        use_fuse = true;
    }
    //
    if (config_->IsEmpty()) {
        KVFS_LOG("file system is empty .. create root inode .. ");
        tfs_meta_key_t key;
        BuildMetaKey(NULL, 0, ROOT_INODE_ID, key);
        struct stat statbuf;
        lstat(ROOT_INODE_STAT, &statbuf);
        tfs_inode_val_t value = InitInodeValue(ROOT_INODE_ID,
            statbuf.st_mode, statbuf.st_dev, rocksdb::Slice("\0"));
        if (db_->Put(key.ToSlice(), value.ToSlice()) != 0) {
            KVFS_LOG("rocksdb put error\n");
        }
        FreeInodeValue(value);
    } else {
        KVFS_LOG("not empty ..  ");
    }
    return config_;
}

void RocksFS::Destroy(void * data){
  KVFS_LOG("FS Destroy\n");
  db_->Cleanup();
  delete db_;
  int ret;
  KVFS_LOG("Daos cont close");
  ret = daos_cont_close(config_->coh, NULL);
  if(ret != 0){
      KVFS_LOG("error");
  }

  KVFS_LOG("Daos pool disconnect");
  ret = daos_pool_disconnect(config_->poh, NULL);
  if(ret != 0){
      KVFS_LOG("error");
  }

  KVFS_LOG("Daos fini");
  ret = daos_fini();
  if(ret != 0){
      KVFS_LOG("error");
  }
  delete config_;
}

int RocksFS::GetAttr(const char *path, struct stat *statbuf, struct fuse_file_info *fi) {
    KVFS_LOG("GetAttr:%s\n", path);
    tfs_meta_key_t key;
    if (!PathLookup(path, key)) {
        KVFS_LOG("GetAttr Path Lookup: No such file or directory: %s\n", path);
        return -errno;
    }
    int ret = 0;
    std::string value;
    ret = db_->Get(key.ToSlice(), value);
    if (ret == 0) {
        *statbuf = *(GetAttribute(value));
        return 0;
    } else if (ret == 1){
        return -ENOENT;
    } else {
      return -EDBERROR;
    }
}

kvfs_file_handle * RocksFS::InitFileHandle(const char * path, struct fuse_file_info * fi
        ,const tfs_meta_key_t & key , const std::string & value )
{
       kvfs_file_handle * handle = new kvfs_file_handle(path); 
       handle->key = key;
       handle->value = value;
       handle->flags = fi->flags;
       if( (fi->flags & O_RDWR) >0 ||
               (fi->flags & O_WRONLY) > 0 || 
               (fi->flags & O_TRUNC) > 0)
       {
           handle->mode = INODE_WRITE;
       }
       else 
       {
           handle->mode = INODE_READ;
       }
       if(value != "") // file exists
       {
           const tfs_inode_header *header = reinterpret_cast<const tfs_inode_header *>(value.data());
           if(header->has_blob > 0) // for big file , fill fd 
           {
              //  handle->fd = OpenDiskFile(key, header, fi->flags);
              handle->fd = 1;
           }
           else // small file or dir 
           {
               handle->fd = -1;
           }
       }
       else  // new file
       {

           
       }
       fi->fh = reinterpret_cast <uint64_t >(handle);

       return handle;
}

int RocksFS::Open(const char *path, struct fuse_file_info *fi) {
    KVFS_LOG("Open:%s\n", path);
    tfs_meta_key_t key;
    if (!PathLookup(path, key)) {
        KVFS_LOG("Open: No such file or directory %s\n", path);
        return -errno;
    }
    string value;
    int ret = db_->Get(key.ToSlice(), value);
    if(ret == 0){  //该文件存在
      InitFileHandle(path,fi,key,value);
    } else if (ret == 1){  //该文件不存在
      return -ENOENT;
    } else {
        return -EDBERROR;
    }
    return 0;
}

int RocksFS::Read(const char * path,char * buf , size_t size ,off_t offset ,struct fuse_file_info * fi){
    KVFS_LOG("Read: %s size : %d , offset %d \n",path,size,offset);
    kvfs_file_handle *handle = reinterpret_cast<kvfs_file_handle *>(fi->fh);
    tfs_meta_key_t key = handle->key;
    const tfs_inode_header *header = reinterpret_cast<const tfs_inode_header *>(handle->value.data());
    int ret = 0;
    if (header->has_blob > 0) {  //大文件
      // if (handle->fd < 0) {
      //   handle->fd = OpenDiskFile(key, header, handle->flags);
      //   if (handle->fd < 0)
      //     ret = -EBADF;
      // }
      // if (handle->fd >= 0) {
      //   ret = pread(handle->fd, buf, size, offset);
      // }
      daos_handle_t oh;
      ssize_t actual;
      d_iov_t iov;
      d_sg_list_t sgl;
      daos_obj_id_t oid;

      config_->GetDaosObjId(&oid, key.hash_id);

      // ret = daos_obj_open(config_->coh, oid, DAOS_OO_RO, &oh, NULL);
      KVFS_LOG("daos_array_open");
      daos_size_t cell_size, chunk_size;
      ret = daos_array_open(config_->coh, oid, DAOS_TX_NONE, DAOS_OO_RW, &cell_size , &chunk_size, &oh, NULL);
      if(ret != 0){
        KVFS_LOG("error");
      }
      // handle->oh = oh;
      handle->fd = 1;

      /** set memory location */
      sgl.sg_nr = 1;
      sgl.sg_nr_out = 0;
      d_iov_set(&iov, buf, size);
      sgl.sg_iovs = &iov;

      ret = ReadDaosArray(oh, &sgl, offset, &actual);
      if(ret != 0){
        KVFS_LOG("error");
      }     

      ret = daos_array_close(oh, NULL);
      if(ret != 0){
        KVFS_LOG("error");
      }  
      // assert(ret == 0);
      return actual;
    } else { //小文件
      ret = GetInlineData(handle->value, buf, offset, size);
    }
    return ret;
}

int RocksFS::MigrateToDiskFile(const tfs_meta_key_t &key, string &value, int &fd, int flags) {
  const tfs_inode_header* iheader = GetInodeHeader(value);
  if (fd >= 0) {
    close(fd);
  }
  fd = OpenDiskFile(key, iheader, flags);
  if (fd < 0) {
    fd = -1;
    return -errno;
  }
  int ret = 0;
  if (iheader->fstat.st_size > 0 ) {
    const char* buffer = (const char *) iheader +
                         (TFS_INODE_HEADER_SIZE + iheader->namelen + 1);
    if (pwrite(fd, buffer, iheader->fstat.st_size, 0) !=
        iheader->fstat.st_size) {
      ret = -errno;
    }
    DropInlineData(value);
  }
  return ret;
}

int RocksFS::Write(const char * path , const char * buf,size_t size ,off_t offset ,struct fuse_file_info * fi){
    KVFS_LOG("Write : %s %lld %d\n",path,offset ,size);
    kvfs_file_handle *handle = reinterpret_cast<kvfs_file_handle *>(fi->fh);
    tfs_meta_key_t key = handle->key;
    const tfs_inode_header *header = reinterpret_cast<const tfs_inode_header *>(handle->value.data());
    bool has_larger_size = (header->fstat.st_size < offset + size);
    int ret = 0;
    if (header->has_blob > 0) {  //大文件
      // if (handle->fd < 0) {
      //   handle->fd = OpenDiskFile(key, header, handle->flags);
      //   if (handle->fd < 0)
      //     ret = -EBADF;
      // }
      // if (handle->fd >= 0) {
      //   ret = pwrite(handle->fd, buf, size, offset);
      // }
      // if (ret >= 0 && has_larger_size > 0 ) {
      //   tfs_inode_header new_iheader = *GetInodeHeader(handle->value);
      //   new_iheader.fstat.st_size = offset + size;
      //   UpdateInodeHeader(handle->value, new_iheader);
      //   int res = db_->Put(handle->key.ToSlice(), handle->value);
      //   if(res != 0){
      //     return -EDBERROR;
      //   }
      // }
      daos_handle_t oh;
      ssize_t actual;
      d_iov_t iov;
      d_sg_list_t sgl;
      daos_obj_id_t oid;

      config_->GetDaosObjId(&oid, key.hash_id);

      // ret = daos_obj_open(config_->coh, oid, DAOS_OO_RO, &oh, NULL);
      KVFS_LOG("daos_array_open");
      daos_size_t cell_size, chunk_size;
      ret = daos_array_open(config_->coh, oid, DAOS_TX_NONE, DAOS_OO_RW, &cell_size , &chunk_size, &oh, NULL);
      // KVFS_LOG("daos_array_create");
      // ret = daos_array_create(config_->coh, oid, DAOS_TX_NONE, 1, FS_DEFAULT_CHUNK_SIZE, &oh, NULL);
      if(ret != 0){
        KVFS_LOG("error");
      }
      // handle->oh = oh;
      handle->fd = 1;

      /** set memory location */
      sgl.sg_nr = 1;
      sgl.sg_nr_out = 0;
      d_iov_set(&iov, (void *)buf, size);
      sgl.sg_iovs = &iov;

      ret = WriteDaosArray(oh, &sgl, offset);
      if(ret != 0){
        KVFS_LOG("error");
      }
      
      if (ret >= 0) {
        tfs_inode_header new_iheader = *GetInodeHeader(handle->value);
        new_iheader.fstat.st_size = offset + size;
        UpdateInodeHeader(handle->value, new_iheader);
        int ret = db_->Put(handle->key.ToSlice(), handle->value);
        if(ret != 0){
          KVFS_LOG("error");
          return -EDBERROR;
        }
      }
      ret = size;
  } else {
    if (offset + size > config_->GetThreshold()) {  //转成大文件
      KVFS_LOG("Write :from small file to BigFile");
      //0 .copy inline data 
      char * buffer = new char[ offset + size];

      const char * inline_data = handle->value.data() + TFS_INODE_HEADER_SIZE + header->namelen + 1;
      int inline_data_size = handle->value.size() - (TFS_INODE_HEADER_SIZE + header->namelen + 1);
      
      memcpy(buffer, inline_data, inline_data_size);
      //1 . write data to buffer
      memcpy (buffer + offset , buf, size);
      //2. write buffer to file
      // int fd = OpenDiskFile(key, header, handle->flags);
      // if(fd < 0){
      //   delete [] buffer;
      //   return fd;
      // }
      // ret = pwrite(fd,buffer,offset+size,0);
      daos_handle_t oh;
      ssize_t actual;
      d_iov_t iov;
      d_sg_list_t sgl;
      daos_obj_id_t oid;

      config_->GetDaosObjId(&oid, key.hash_id);

      // ret = daos_obj_open(config_->coh, oid, DAOS_OO_RO, &oh, NULL);
      // KVFS_LOG("daos_array_open_with_attr");
      // ret = daos_array_open_with_attr(config_->coh, oid, DAOS_TX_NONE, DAOS_OO_RW, config_->cell_size, config_->chunk_size, &oh, NULL);
      KVFS_LOG("daos_array_create");
      ret = daos_array_create(config_->coh, oid, DAOS_TX_NONE, 1, FS_DEFAULT_CHUNK_SIZE, &oh, NULL);
      if(ret != 0){
          KVFS_LOG("error,ret:%d", ret);
      }
      // handle->oh = oh;

      /** set memory location */
      sgl.sg_nr = 1;
      sgl.sg_nr_out = 0;
      d_iov_set(&iov, (void *)buffer, offset+size);
      sgl.sg_iovs = &iov;

      ret = WriteDaosArray(oh, &sgl, 0);
      if(ret != 0){
        KVFS_LOG("error");
      }
      ret = size;
      int rc = daos_array_close(oh, NULL);
      if(rc != 0){
        KVFS_LOG("error");
      }
      //3 . delete tmp buffer
      
      delete [] buffer;
      //4 . update inode 
      if(ret >= 0){
        // handle->fd = fd;
        handle->fd = 1;
        tfs_inode_header new_iheader = *GetInodeHeader(handle->value);
        new_iheader.fstat.st_size = offset + size;
        new_iheader.has_blob = 1;
        UpdateInodeHeader(handle->value, new_iheader);
        int res = db_->Put(key.ToSlice(), handle->value);
        if(res != 0){
          return -EDBERROR;
        }
        ret = size;
      }
      
      
    } else {
      UpdateInlineData(handle->value, buf, offset, size);
      ret = size;
      // if(has_larger_size){
      //   tfs_inode_header new_iheader = *GetInodeHeader(handle->value);
      //   new_iheader.fstat.st_size = offset + size;
      //   UpdateInodeHeader(handle->value, new_iheader);
      // }
      tfs_inode_header new_iheader = *GetInodeHeader(handle->value);
      new_iheader.fstat.st_size = offset + size;
      UpdateInodeHeader(handle->value, new_iheader);
      int res = db_->Put(key.ToSlice(), handle->value);
      if(res != 0){
        return -EDBERROR;
      }
    }
    return ret;
  }
  return ret;
}

int RocksFS::Truncate(const char * path ,off_t offset, struct fuse_file_info *fi){
  KVFS_LOG("Truncate:%s %d\n", path, offset);
  tfs_meta_key_t key;
  if (!PathLookup(path, key)) {
      KVFS_LOG("Truncate: No such file or directory %s\n", path);
      return -errno;
  }
    off_t new_size = offset;
    string value;
    int ret = db_->Get(key.ToSlice(), value);
    if(ret == 0){  //该文件存在
      const tfs_inode_header *iheader = reinterpret_cast<const tfs_inode_header *>(value.data());

      if (iheader->has_blob > 0) {
        if (new_size > config_->GetThreshold()) {
          // TruncateDiskFile(key, iheader->fstat.st_ino, new_size);
          TruncateDaosFile(key.inode_id, new_size);
        } else {
          char* buffer = new char[new_size];
          // MigrateDiskFileToBuffer(key, iheader->fstat.st_ino, buffer, new_size);
          MigrateDaosFileToBuffer(key.inode_id, buffer, new_size);
          UpdateInlineData(value, buffer, 0, new_size);
          delete [] buffer;
        }
      } else {
        if (new_size > config_->GetThreshold()) {
          // int fd;
          // if (MigrateToDiskFile(key, value, fd, O_TRUNC|O_WRONLY) == 0) {
          //   if ((ret = ftruncate(fd, new_size)) == 0) {
          //     fsync(fd);
          //   }
          //   close(fd);
          // }
          MigrateToDaosFile(key.inode_id, value, O_TRUNC|O_WRONLY);
        } else {
          TruncateInlineData(value, new_size);
        }
      }
      if (new_size != iheader->fstat.st_size) {
        tfs_inode_header new_iheader = *GetInodeHeader(value);
        new_iheader.fstat.st_size = new_size;
        if (new_size > config_->GetThreshold()) {
          new_iheader.has_blob = 1;
        } else {
          new_iheader.has_blob = 0;
        }
        UpdateInodeHeader(value, new_iheader);
      }
      int res = db_->Put(key.ToSlice(), value);
      if(res != 0){
        return -EDBERROR;
      }
      
    } else if (ret == 1){  //该文件不存在
      return -ENOENT;
    } else {
        return -EDBERROR;
    }

  return ret;
}
int RocksFS::Fsync(const char * path,int datasync ,struct fuse_file_info * fi){
    KVFS_LOG("Fsync: %s\n",path);

    kvfs_file_handle * handle = reinterpret_cast <kvfs_file_handle *>(fi->fh);
    int ret = 0 ;
    if(handle->mode == INODE_WRITE)
    {
        if(handle->fd >=0 )
        {
            //big file
            // ret = fsync(handle->fd);
            ret = 0;
        }
        if(datasync == 0)
        {
            int res = db_->Sync();
            if(res != 0){
              return -EDBERROR;
            }
        }
    }
    return -ret;
}
int RocksFS::Release(const char * path ,struct fuse_file_info * fi){
  KVFS_LOG("Release:%s \n", path);
  kvfs_file_handle* handle = reinterpret_cast<kvfs_file_handle*>(fi->fh);
  tfs_meta_key_t key = handle->key;

  if (handle->mode == INODE_WRITE) {
    const tfs_stat_t *value = GetAttribute(handle->value);
    tfs_stat_t new_value = *value;
    new_value.st_atim.tv_sec = time(NULL);
    new_value.st_atim.tv_nsec = 0;
    new_value.st_mtim.tv_sec = time(NULL);
    new_value.st_mtim.tv_nsec = 0;
    UpdateAttribute(handle->value, new_value);
  }

  int ret = 0;
  // if (handle->fd >= 0) {
  //   ret = close(handle->fd);
  // }
  int res = db_->Put(key.ToSlice(), handle->value);
  if(res != 0){
    return -EDBERROR;
  }
  kvfs_file_handle::DeleteHandle(path);

  if (ret != 0) {
    return -errno;
  } else {
    return 0;
  }
}
int RocksFS::Readlink(const char * path ,char * buf,size_t size){
  KVFS_LOG("Readlink:%s \n", path);
  tfs_meta_key_t key;
  if (!PathLookup(path, key)) {
      KVFS_LOG("Readlink: No such file or directory %s\n", path);
      return -errno;
  }
  
  std::string result;
  int ret = 0;
  ret = db_->Get(key.ToSlice(), result);
  if(ret == 0){
    size_t data_size = GetInlineData(result, buf, 0, size-1);
    buf[data_size] = '\0';
    return 0;
  } else if(ret == 1){
    return -ENOENT;
  } else{
    return -EDBERROR;
  }
}

int RocksFS::Symlink(const char * target , const char * path){
  KVFS_LOG("Symlink:not implement%s %s", target, path);
  return -ENOTIMPLEMENT;
}

int RocksFS::Unlink(const char * path){
  KVFS_LOG("Unlink:%s \n", path);
  tfs_meta_key_t key;
  if (!PathLookup(path, key)) {
    KVFS_LOG("Unlink: No such file or directory %s\n", path);
    return -errno;
  }
  std::string value;
  int ret = 0;
  ret = db_->Get(key.ToSlice(), value);
  if(ret == 0){
    const tfs_inode_header *iheader = reinterpret_cast<const tfs_inode_header *>(value.data());
    if(iheader->has_blob > 0){
      // string fpath = GetDiskFilePath(key, iheader->fstat.st_ino);
      // fpath += '\0';
      // unlink(fpath.c_str());
      daos_handle_t oh;
      daos_obj_id_t oid;
      
      config_->GetDaosObjId(&oid, key.hash_id);

      // ret = daos_obj_open(config_->coh, oid, DAOS_OO_RW, &oh, NULL);
      // KVFS_LOG("daos_array_open_with_attr");
      // ret = daos_array_open_with_attr(config_->coh, oid, DAOS_TX_NONE, DAOS_OO_RW, 1, FS_DEFAULT_CHUNK_SIZE, &oh, NULL);
      KVFS_LOG("daos_array_open");
      daos_size_t cell_size, chunk_size;
      ret = daos_array_open(config_->coh, oid, DAOS_TX_NONE, DAOS_OO_RW, &cell_size , &chunk_size, &oh, NULL);
      if(ret != 0){
        KVFS_LOG("error");
      }
      KVFS_LOG("daos_array_destroy");
      ret = daos_array_destroy(oh, DAOS_TX_NONE, NULL);
      if(ret != 0){
        KVFS_LOG("daos_array_close");
        daos_array_close(oh, NULL);
        if(ret != 0){
          KVFS_LOG("error");
        }
      }
      ret = daos_array_close(oh, NULL);
      if(ret != 0){
        KVFS_LOG("error");
      }
    }
    kvfs_file_handle::DeleteHandle(path);
    int res = db_->Delete(key.ToSlice());
    if(res != 0){
      return -EDBERROR;
    }

    return 0;
  } else if(ret == 1){
    return -ENOENT;
  } else{
    return -EDBERROR;
  }
}

int RocksFS::MakeNode(const char * path,mode_t mode ,dev_t dev){
  KVFS_LOG("MakeNode:%s", path);
  tfs_meta_key_t key;
  rocksdb::Slice filename;
  if (!PathLookup(path, key, filename)) {
    KVFS_LOG("MakeNode: No such file or directory %s\n", path);
    return -errno;
  }


  tfs_inode_val_t value = InitInodeValue(config_->NewInode(), mode | S_IFREG, dev, filename);

  int ret = 0;
  
  ret=db_->Put(key.ToSlice(), value.ToSlice());
  if(ret != 0){
    return -EDBERROR;
  }

  FreeInodeValue(value);

  return 0;
}

int RocksFS::MakeDir(const char * path,mode_t mode){
  KVFS_LOG("MakeDir:%s", path);
  tfs_meta_key_t key;
  rocksdb::Slice filename;
  if (!PathLookup(path, key, filename)) {
    KVFS_LOG("MakeDir: No such file or directory %s\n", path);
    return -errno;
  }

  tfs_inode_val_t value =
    InitInodeValue(config_->NewInode(), mode | S_IFDIR, 0, filename);

  int ret = 0;
  ret=db_->Put(key.ToSlice(), value.ToSlice());
  if(ret != 0){
    return -EDBERROR;
  }

  FreeInodeValue(value);

  return 0;
}

int RocksFS::OpenDir(const char * path,struct fuse_file_info *fi){
  KVFS_LOG("OpenDir:%s", path);
  tfs_meta_key_t key;
  std::string inode;
  if (!PathLookup(path, key)) {
    KVFS_LOG("OpenDir: No such file or directory %s\n", path);
    return -errno;
  }
  std::string value;
  int ret = 0;
  ret = db_->Get(key.ToSlice(), value);
  if(ret == 0){
    kvfs_file_handle * handle = new kvfs_file_handle(path);
    handle->fd = -1;
    handle->key = key;
    handle->flags = fi->flags;
    handle->mode = INODE_READ;
    handle->value = value;
    fi->fh = reinterpret_cast<uint64_t>(handle);

    return 0;
  } else if(ret == 1){
    return -ENOENT;
  } else{
    return -EDBERROR;
  }
  
}

int RocksFS::ReadDir(const char * path,void * buf ,fuse_fill_dir_t filler,off_t offset ,struct fuse_file_info * fi, enum fuse_readdir_flags flag){
  KVFS_LOG("ReadDir:%s", path);
  kvfs_file_handle * handle = reinterpret_cast <kvfs_file_handle *>(fi->fh);
  tfs_meta_key_t childkey;
  int ret = 0;
  tfs_inode_t child_inumber = GetAttribute(handle->value)->st_ino;
  BuildMetaKey(child_inumber,
              (child_inumber == ROOT_INODE_ID) ? 1 : 0,
              childkey);
  Iterator* iter = db_->NewIterator();
  if (filler(buf, ".", NULL, 0, (enum fuse_fill_dir_flags) 0) < 0) {
    KVFS_LOG("filler . error\n");
    return -errno;
  }
  if (filler(buf, "..", NULL, 0, (enum fuse_fill_dir_flags) 0) < 0) {
    KVFS_LOG("filler .. error\n");
    return -errno;
  }
  for (iter->Seek(childkey.ToSlice());
       iter->Valid() && IsKeyInDir(iter->key(), childkey);
       iter->Next()) {
    const char* name_buffer = iter->value().data() + TFS_INODE_HEADER_SIZE;
    if (name_buffer[0] == '\0') {
        continue;
    }
    if (filler(buf, name_buffer, NULL, 0, (enum fuse_fill_dir_flags) 0) < 0) {
      ret = -1;
    }
    if (ret < 0) {
      break;
    }
  }
  delete iter;
  return ret;
}

int RocksFS::ReleaseDir(const char * path,struct fuse_file_info * fi){
  KVFS_LOG("ReleaseDir:%s", path);
  //kvfs_file_handle * handle = reinterpret_cast <kvfs_file_handle *>(fi->fh);
  kvfs_file_handle::DeleteHandle(path);
  return 0;
}

//zzy:todo
int RocksFS::RemoveDir(const char * path){
  KVFS_LOG("RemoveDir:%s", path);
  tfs_meta_key_t key;
  if (!PathLookup(path, key)) {
    KVFS_LOG("Unlink: No such file or directory %s\n", path);
    return -errno;
  }
  std::string value;
  int ret = 0;
  ret = db_->Get(key.ToSlice(), value);

  // //解析出stat
  // const tfs_stat_t* inode_value = GetAttribute(value);
  // //如果是目录，递归删除
  // if(S_ISDIR(inode_value->st_mode)) {
  //   string ppath = path;
  //   KVFS_LOG("Removedir contents %s", path);
  //   ret = RemoveDirContents(key, ppath);
  // } else {
  //   KVFS_LOG("RemoveDir:%s is not a dir!", path);
  //   return -EIO;
  // }

  if(ret == 0){
    kvfs_file_handle::DeleteHandle(path);
    int res = db_->Delete(key.ToSlice());
    if(res != 0){
      return -EDBERROR;
    }

    return 0;
  } else if(ret == 1){
    return -ENOENT;
  } else{
    return -EDBERROR;
  }
}

int RocksFS::RemoveDirContents(tfs_meta_key_t parent_key, string path) {
  KVFS_LOG("RemoveDirContents");
  int ret = 0;
  Iterator* iter = db_->NewIterator();
  tfs_hash_t p_hash_id = parent_key.hash_id;
  parent_key.hash_id = parent_key.inode_id == ROOT_INODE_ID ? 1 : 0;
  

  if(iter == nullptr){
    KVFS_LOG("Iter is NULL");
    return -EIO;
  }

  for (iter->Seek(parent_key.ToSlice());
      iter->Valid() && IsKeyInDir(iter->key(), parent_key);
      iter->Next()) {
    tfs_meta_key_t child_key;
    const tfs_meta_key_t* rkey = (const tfs_meta_key_t *) iter->key().data();
    child_key.inode_id = rkey->inode_id;
    child_key.hash_id = rkey->hash_id;
    //判断是不是原来的父节点
    if(rkey->hash_id == p_hash_id){
      continue;
    }

    const char* name_buffer = iter->value().data() + TFS_INODE_HEADER_SIZE;
    if (name_buffer[0] == '\0') {
        continue;
    }
    string fname = name_buffer;
    std::string value = iter->value().ToString();
    // ret = db_->Get(parent_key.ToSlice(), value);
    // if(ret !=0) {
    //   KVFS_LOG("get error");
    //   return -EIO;
    // }

    const tfs_stat_t* inode_value = GetAttribute(value);
    
    if(S_ISDIR(inode_value->st_mode)) {
      RemoveDirContents(child_key, path+fname);
    }
    else if(S_ISLNK(inode_value->st_mode) || S_ISREG(inode_value->st_mode)) {
      KVFS_LOG("remove file or link\n");
      const tfs_inode_header *iheader = reinterpret_cast<const tfs_inode_header *>(value.data());
      if(iheader->has_blob > 0) {
        daos_handle_t oh;
        daos_obj_id_t oid;
        
        config_->GetDaosObjId(&oid, child_key.hash_id);
        KVFS_LOG("daos_array_open");
        daos_size_t cell_size, chunk_size;
        ret = daos_array_open(config_->coh, oid, DAOS_TX_NONE, DAOS_OO_RW, &cell_size , &chunk_size, &oh, NULL);
        if(ret != 0){
          KVFS_LOG("error");
        }
        // ret = daos_obj_punch(oh, DAOS_TX_NONE, 0, NULL);
        KVFS_LOG("daos_array_destroy");
        ret = daos_array_destroy(oh, DAOS_TX_NONE, NULL);
        if(ret){
          KVFS_LOG("daos_array_close");
          daos_array_close(oh, NULL);
          KVFS_LOG("error");
        }
        ret = daos_array_close(oh, NULL);
        if(ret != 0){
          KVFS_LOG("error");
        }
      }
    }
    if(ret == 0){
      if(kvfs_file_handle::DeleteHandle(path+fname) == false) {
        KVFS_LOG("delete non exist handle!");
      }
      int res = db_->Delete(child_key.ToSlice());
      if(res != 0){
        return -EDBERROR;
      }

      return 0;
    } else if(ret == 1){
      return -ENOENT;
    } else{
      return -EDBERROR;
    }

  }
  delete iter;
  return ret;
}

int RocksFS::Rename(const char *new_path,const char * old_path, unsigned int flags){
  KVFS_LOG("Rename:%s %s", new_path, old_path);
  tfs_meta_key_t old_key;
  if (!PathLookup(old_path, old_key)) {
    KVFS_LOG("OpenDir: No such file or directory %s\n", old_path);
    return -errno;
  }
  tfs_meta_key_t new_key;
  rocksdb::Slice filename;
  if (!PathLookup(new_path, new_key, filename)) {
    KVFS_LOG("OpenDir: No such file or directory %s\n", new_path);
    return -errno;
  }

  std::string old_value;
  int ret = 0;
  ret = db_->Get(old_key.ToSlice(), old_value);
  if(ret == 0){
    const tfs_inode_header *iheader = reinterpret_cast<const tfs_inode_header *>(old_value.data());
    std::string new_value = InitInodeValue(old_value, filename);
    ret=db_->Put(new_key.ToSlice(), new_value);
    if(ret != 0){
      return -EDBERROR;
    }
    ret = db_->Delete(old_key.ToSlice());
    if(ret != 0){
      return -EDBERROR;
    }
    kvfs_file_handle::DeleteHandle(old_path);
    kvfs_file_handle::DeleteHandle(new_path);
    return 0;
  } else if(ret == 1){
    return -ENOENT;
  } else{
    return -EDBERROR;
  }

}

int RocksFS::Access(const char * path,int mask){
  KVFS_LOG("Access:not implement:%s", path);
  //return -ENOTIMPLEMENT;
  return 0;
}

int RocksFS::Chmod(const char * path , mode_t mode, struct fuse_file_info *fi){
  KVFS_LOG("Chmod:%s", path);
  tfs_meta_key_t key;
  if (!PathLookup(path, key)) {
    KVFS_LOG("Chmod: No such file or directory %s\n", path);
    return -errno;
  }
  std::string value;
  int ret = 0;
  ret = db_->Get(key.ToSlice(), value);
  if(ret == 0){
    kvfs_file_handle * handle = kvfs_file_handle ::GetHandle(path);
    string *mu_value = nullptr;
    if(nullptr != handle){
      mu_value = &(handle->value);
    } else {
      mu_value = &value;
    }
    const tfs_stat_t *st_value = GetAttribute(*mu_value);
    tfs_stat_t new_value = *st_value;
    new_value.st_mode = mode;
    UpdateAttribute(*mu_value, new_value);
    ret=db_->Put(key.ToSlice(), *mu_value);
    if(ret != 0){
      return -EDBERROR;
    }
    return 0;
  } else if(ret == 1){
    return -ENOENT;
  } else{
    return -EDBERROR;
  }

}

int RocksFS::Chown(const char * path, uid_t uid,gid_t gid, struct fuse_file_info *fi){
  KVFS_LOG("Chown:%s", path);
  tfs_meta_key_t key;
  if (!PathLookup(path, key)) {
    KVFS_LOG("Chown: No such file or directory %s\n", path);
    return -errno;
  }
  std::string value;
  int ret = 0;
  ret = db_->Get(key.ToSlice(), value);
  if(ret == 0){
    kvfs_file_handle * handle = kvfs_file_handle ::GetHandle(path);
    string *mu_value = nullptr;
    if(nullptr != handle){
      mu_value = &(handle->value);
    } else {
      mu_value = &value;
    }
    const tfs_stat_t *st_value = GetAttribute(*mu_value);
    tfs_stat_t new_value = *st_value;
    new_value.st_uid = uid;
    new_value.st_gid = gid;
    UpdateAttribute(*mu_value, new_value);
    ret=db_->Put(key.ToSlice(), *mu_value);
    if(ret != 0){
      return -EDBERROR;
    }
    return 0;
  } else if(ret == 1){
    return -ENOENT;
  } else{
    return -EDBERROR;
  }
}

int RocksFS::UpdateTimens(const char * path ,const struct timespec tv[2], struct fuse_file_info *fi){
  KVFS_LOG("UpdateTimens:%s", path);
  tfs_meta_key_t key;
  if (!PathLookup(path, key)) {
    KVFS_LOG("Chown: No such file or directory %s\n", path);
    return -errno;
  }
  std::string value;
  int ret = 0;
  ret = db_->Get(key.ToSlice(), value);
  if(ret == 0){
    kvfs_file_handle * handle = kvfs_file_handle ::GetHandle(path);
    string *mu_value = nullptr;
    if(nullptr != handle){
      mu_value = &(handle->value);
    } else {
      mu_value = &value;
    }
    const tfs_stat_t *st_value = GetAttribute(*mu_value);
    tfs_stat_t new_value = *st_value;
    new_value.st_atim.tv_sec = tv[0].tv_sec;
    new_value.st_atim.tv_nsec = tv[0].tv_nsec;
    new_value.st_mtim.tv_sec = tv[1].tv_sec;
    new_value.st_mtim.tv_nsec = tv[1].tv_nsec;
    UpdateAttribute(*mu_value, new_value);
    ret=db_->Put(key.ToSlice(), *mu_value);
    if(ret != 0){
      return -EDBERROR;
    }
    return 0;
  } else if(ret == 1){
    return -ENOENT;
  } else{
    return -EDBERROR;
  }
}
} // namespace name