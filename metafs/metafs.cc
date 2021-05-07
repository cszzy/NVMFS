#include "metafs.h"
#include <cstring>
#include <sys/types.h>
#include <unistd.h>
#include <limits>
#include <algorithm>
#include <errno.h>

#define EDBERROR 188  //DB错误
#define ENOTIMPLEMENT 189  //未实现

using namespace std;
namespace metafs {

//uint128转换为字符串
// static inline void my_uint128_itoa(__uint128_t v, string& s){
//     char temp;
//     int i = 0, j;

//     while(v > 0){
//         s[i++] = v % 10 + '0';
//         v /= 10;
//     }
//     s[i] = '\0';
    
//     j=0;
//     i--;
//     while(j < i){
//         temp = s[j];
//         s[j] = s[i];
//         s[i] = temp;
//         j++;
//         i--;
//     }
// }

const tfs_inode_header *GetInodeHeader(const std::string &value) {
  return reinterpret_cast<const tfs_inode_header*> (value.data());
}

const tfs_stat_t *GetAttribute(std::string &value) {
  return reinterpret_cast<const tfs_stat_t*> (value.data());
}

size_t GetInlineData(std::string &value, char* buf, size_t offset, size_t size) {
  const tfs_inode_header* header = GetInodeHeader(value);
  size_t realoffset = TFS_INODE_HEADER_SIZE + offset;
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
  size_t realoffset = TFS_INODE_HEADER_SIZE + offset;
  UpdateIhandleValue(value, buf, realoffset, size);
}

void TruncateInlineData(std::string &value, size_t new_size) {
  const tfs_inode_header* header = GetInodeHeader(value);
  size_t target_size = TFS_INODE_HEADER_SIZE + new_size;
  value.resize(target_size);
}

void DropInlineData(std::string &value) {
  const tfs_inode_header* header = GetInodeHeader(value);
  size_t target_size = TFS_INODE_HEADER_SIZE;
  value.resize(target_size);
}

void MetaFS::InitStat(tfs_stat_t &statbuf,
                       inode_id_t inode,
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


MetaFS::MetaFS(const kvfs_args & args) : args_(args), db_(nullptr),config_(nullptr),use_fuse(false)
{
    KVFS_LOG("init config..\n");
    config_  = new KVFSConfig();
    config_->Init(args_);

    KVFS_LOG("init metadb adaptor ..\n");
    db_ = new DBAdaptor();
    db_->Init(config_->GetMetaDir());
    cfg_ = nullptr;
    KVFS_LOG("after init metadb adaptor ..\n");
}

MetaFS::~MetaFS(){
  KVFS_LOG("FS exit\n");

}

bool MetaFS::PathLookup(const char *path,
                         inode_id_t &key) {

  const char* lpos = path;
  const char* rpos;
  bool flag_found = true;
  inode_id_t inode_in_search = ROOT_INODE_ID;
  while ((rpos = strchr(lpos+1, PATH_DELIMITER)) != NULL) {
      if (rpos - lpos > 0) {
          string fname(lpos+1, rpos-lpos-1);
          
          int ret = db_->DirGet(inode_in_search, fname, key);
          if (ret == 0) {
              inode_in_search = key;
          } else if (ret == 1){
              KVFS_LOG("PathLookup not find:inode:%d %s %d ptr:%s\n", inode_in_search, fname.c_str(), key, lpos);
              errno = ENOENT;
              flag_found = false;
          } else{
              KVFS_LOG("PathLookup db error:inode:%d %s %d ptr:%s\n", inode_in_search, fname.c_str(), key, lpos);
              errno = EDBERROR;
              flag_found = false;
          }
          
          if (!flag_found) {
              return false;
          }
      }
      lpos = rpos;
  }
  rpos = strchr(lpos, '\0');
  if (rpos != NULL && rpos-lpos > 1) {  //最后一个文件名
    string fname(lpos+1, rpos-lpos-1);
    int ret = db_->DirGet(inode_in_search, fname, key);
    if (ret == 0) {
        return true;
    } else if (ret == 1){
        KVFS_LOG("PathLookup not find:inode:%d %s %d ptr:%s\n", inode_in_search, fname.c_str(), key, lpos);

        errno = ENOENT;
        flag_found = false;
    } else{
        KVFS_LOG("PathLookup db error:inode:%d %s %d ptr:%s\n", inode_in_search, fname.c_str(), key, lpos);

        errno = EDBERROR;
        flag_found = false;
    }
    
    if (!flag_found) {
        return false;
    }
  }

  if (lpos == path) {  //说明是“/”
      key = ROOT_INODE_ID;
  }
  return true;
    
}
bool MetaFS::PathLookup(const char *path, inode_id_t &key, inode_id_t &parent_id, string &fname) {

  const char* lpos = path;
  const char* rpos;
  bool flag_found = true;
  parent_id = ROOT_INODE_ID;
  while ((rpos = strchr(lpos+1, PATH_DELIMITER)) != NULL) {
      if (rpos - lpos > 0) {
          fname.assign(lpos+1, rpos-lpos-1);
          
          int ret = db_->DirGet(parent_id, fname, key);
          if (ret == 0) {
              parent_id = key;
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
  rpos = strchr(lpos, '\0');
  if (rpos != NULL && rpos-lpos > 1) {  //最后一个文件名
    fname.assign(lpos+1, rpos-lpos-1);
    int ret = db_->DirGet(parent_id, fname, key);
    if (ret == 0) {
        return true;
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

  if (lpos == path) {  //说明是“/”
      key = ROOT_INODE_ID;
  }
  return true;
    
}

bool MetaFS::ParentPathLookup(const char *path, inode_id_t &parent_id, string &fname) {

  const char* lpos = path;
  const char* rpos;
  bool flag_found = true;
  parent_id = ROOT_INODE_ID;
  inode_id_t key;
  while ((rpos = strchr(lpos+1, PATH_DELIMITER)) != NULL) {
      if (rpos - lpos > 0) {
          fname.assign(lpos+1, rpos-lpos-1);
          
          int ret = db_->DirGet(parent_id, fname, key);
          if (ret == 0) {
              parent_id = key;
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
  rpos = strchr(lpos, '\0');
  if (rpos != NULL && rpos-lpos > 1) {  //最后一个文件名
    fname.assign(lpos+1, rpos-lpos-1);
  }

  return true;
    
}

// string MetaFS::GetDiskFilePath(const inode_id_t &inode_id){
//   string inode_id_str;
//   my_uint128_itoa(inode_id, inode_id_str);
//   string path = config_->GetDataDir() + "/" + inode_id_str;
//   return path;
// }

string MetaFS::GetDiskFilePath(const inode_id_t &inode_id){
  string path = config_->GetDataDir() + "/" + std::to_string(inode_id);
  return path;
}

int MetaFS::OpenDiskFile(const inode_id_t &key, const tfs_inode_header* iheader, int flags) {
  string fpath = GetDiskFilePath(key);
  fpath += '\0';
  int fd = open(fpath.c_str(), flags | O_CREAT, iheader->fstat.st_mode);
  if(fd < 0) {
    KVFS_LOG("Open: cant open data file %s fd:%d",fpath.c_str(), fd);
  }
  return fd;
}

int MetaFS::TruncateDiskFile(const inode_id_t &key, off_t new_size) {
  string fpath = GetDiskFilePath(key);
  fpath += '\0';
  return truncate(fpath.c_str(), new_size);
}

ssize_t MetaFS::MigrateDiskFileToBuffer(const inode_id_t &key, char* buffer, size_t size) {
  string fpath = GetDiskFilePath(key);
  fpath += '\0';
  int fd = open(fpath.c_str(), O_RDONLY);
  ssize_t ret = pread(fd, buffer, size, 0);
  close(fd);
  unlink(fpath.c_str());
  return ret;
}

int MetaFS::MigrateToDiskFile(const inode_id_t &key, string &value, int &fd, int flags) {
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
                         (TFS_INODE_HEADER_SIZE);
    if (pwrite(fd, buffer, iheader->fstat.st_size, 0) !=
        iheader->fstat.st_size) {
      ret = -errno;
    }
    DropInlineData(value);
  }
  return ret;
}

//文件已存在
int MetaFS::TruncateDaosFile(const inode_id_t &key, off_t new_size) {
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
ssize_t MetaFS::MigrateDaosFileToBuffer(const inode_id_t &key, char* buffer, size_t size) {
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

int MetaFS::ReadDaosArray(daos_handle_t oh, d_sg_list_t *sgl, daos_off_t offset, ssize_t *read_size) {
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
int MetaFS::MigrateToDaosFile(const inode_id_t &key, string &value, int flags) {
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

int MetaFS::WriteDaosArray(daos_handle_t oh, d_sg_list_t *sgl, daos_off_t offset) {
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

string MetaFS::InitInodeValue(inode_id_t inum, mode_t mode, dev_t dev) {
    int value_size = TFS_INODE_HEADER_SIZE;
    string value(value_size, '\0');

    tfs_inode_header header;
    InitStat(header.fstat, inum, mode, dev);
    header.has_blob = 0;
    UpdateInodeHeader(value, header);
    return value;
}


void* MetaFS::Init(struct fuse_conn_info *conn, struct fuse_config *cfg){
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

    if (config_->IsEmpty()) {
        KVFS_LOG("file system is empty .. create root inode .. ");
        //zzy:root id需要修改为daos oid
        inode_id_t key = ROOT_INODE_ID;
        struct stat statbuf;
        lstat(ROOT_INODE_STAT, &statbuf);
        string value = InitInodeValue(ROOT_INODE_ID, statbuf.st_mode, statbuf.st_dev);
        KVFS_LOG("before add root inode");
        int ret = db_->InodePut(key, value);
        if(ret != 0){
          KVFS_LOG("add root inode error");
        } else {
          KVFS_LOG("add root inode success");
        }
    } else {
        KVFS_LOG("not empty ..  ");
    }
    return config_;
}

void MetaFS::Destroy(void * data){
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

int MetaFS::GetAttr(const char *path, struct stat *statbuf, struct fuse_file_info *fi) {
    KVFS_LOG("GetAttr:%s\n", path);
    inode_id_t key;
    if (!PathLookup(path, key)) {
        KVFS_LOG("GetAttr Path Lookup: No such file or directory: %s\n", path);
        return -errno;
    }
    KVFS_LOG("after pathlookup\n");
    int ret = 0;
    std::string value;
    ret = db_->InodeGet(key, value);
    if (ret == 0) {
      KVFS_LOG("get attr\n");
      *statbuf = *(GetAttribute(value));
      return 0;
    } else if (ret == 1){
      KVFS_LOG("false\n");
      return -ENOENT;
    } else {
      KVFS_LOG("false\n");
      return -EDBERROR;
    }
}

//zzy:打开文件时，初始化handle
kvfs_file_handle * MetaFS::InitFileHandle(const char * path, struct fuse_file_info * fi, const inode_id_t & key , const std::string & value )
{
       kvfs_file_handle * handle = new kvfs_file_handle(key); 
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

int MetaFS::Open(const char *path, struct fuse_file_info *fi) {
    KVFS_LOG("Open:%s\n", path);
    inode_id_t key;
    if (!PathLookup(path, key)) {
        KVFS_LOG("Open: No such file or directory %s\n", path);
        return -errno;
    }
    string value;
    int ret = db_->InodeGet(key, value);
    if(ret == 0){  //该文件存在
      InitFileHandle(path,fi,key,value);
    } else if (ret == 1){  //该文件不存在
      return -ENOENT;
    } else {
        return -EDBERROR;
    }
    return 0;
}

//zzy:参考dfs_read
int MetaFS::Read(const char * path,char * buf , size_t size ,off_t offset ,struct fuse_file_info * fi){
    KVFS_LOG("Read: %s size : %d , offset %d \n",path,size,offset);
    kvfs_file_handle *handle = reinterpret_cast<kvfs_file_handle *>(fi->fh);
    inode_id_t key = handle->key;
    const tfs_inode_header *header = reinterpret_cast<const tfs_inode_header *>(handle->value.data());
    int ret = 0;
    if (header->has_blob > 0) {  //大文件 //daos
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

      config_->GetDaosObjId(&oid, key);

      // ret = daos_obj_open(config_->coh, oid, DAOS_OO_RO, &oh, NULL);
      KVFS_LOG("daos_array_open");
      daos_size_t cell_size, chunk_size;
      ret = daos_array_open(config_->coh, oid, DAOS_TX_NONE, DAOS_OO_RW, &cell_size , &chunk_size, &oh, NULL);
      if(ret != 0){
        KVFS_LOG("error");
      }
      handle->oh = oh;
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

//zzy:TODO参考dfs_write
int MetaFS::Write(const char * path , const char * buf,size_t size ,off_t offset ,struct fuse_file_info * fi){
    KVFS_LOG("Write : %s %lld %d\n",path,offset ,size);
    kvfs_file_handle *handle = reinterpret_cast<kvfs_file_handle *>(fi->fh);
    inode_id_t key = handle->key;
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
      daos_handle_t oh;
      ssize_t actual;
      d_iov_t iov;
      d_sg_list_t sgl;
      daos_obj_id_t oid;

      config_->GetDaosObjId(&oid, key);

      // ret = daos_obj_open(config_->coh, oid, DAOS_OO_RO, &oh, NULL);
      KVFS_LOG("daos_array_open");
      daos_size_t cell_size, chunk_size;
      ret = daos_array_open(config_->coh, oid, DAOS_TX_NONE, DAOS_OO_RW, &cell_size , &chunk_size, &oh, NULL);
      // KVFS_LOG("daos_array_create");
      // ret = daos_array_create(config_->coh, oid, DAOS_TX_NONE, 1, FS_DEFAULT_CHUNK_SIZE, &oh, NULL);
      if(ret != 0){
        KVFS_LOG("error");
      }
      handle->oh = oh;
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
      
      // if (ret >= 0 && has_larger_size > 0 ) {
      //   tfs_inode_header new_iheader = *GetInodeHeader(handle->value);
      //   new_iheader.fstat.st_size = offset + size;
      //   UpdateInodeHeader(handle->value, new_iheader);
      //   int ret = db_->InodePut(key, handle->value);
      //   if(ret != 0){
      //     KVFS_LOG("error");
      //     return -EDBERROR;
      //   }
      // }
      if (ret >= 0) {
        tfs_inode_header new_iheader = *GetInodeHeader(handle->value);
        new_iheader.fstat.st_size = offset + size;
        UpdateInodeHeader(handle->value, new_iheader);
        int ret = db_->InodePut(key, handle->value);
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

      const char * inline_data = handle->value.data() + TFS_INODE_HEADER_SIZE;
      int inline_data_size = handle->value.size() - TFS_INODE_HEADER_SIZE;
      
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

      config_->GetDaosObjId(&oid, key);

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
        int ret = db_->InodePut(key, handle->value);
        if(ret != 0){
          return -EDBERROR;
        }
        ret = size;
      }
 
    } else {
      UpdateInlineData(handle->value, buf, offset, size);
      ret = size;
      //直接更新size，不知道会不会出bug
      // if(has_larger_size){
      //   tfs_inode_header new_iheader = *GetInodeHeader(handle->value);
      //   new_iheader.fstat.st_size = offset + size;
      //   UpdateInodeHeader(handle->value, new_iheader);
      // }
      tfs_inode_header new_iheader = *GetInodeHeader(handle->value);
      new_iheader.fstat.st_size = offset + size;
      UpdateInodeHeader(handle->value, new_iheader);
      int ret = db_->InodePut(key, handle->value);
      if(ret != 0){
        return -EDBERROR;
      }
    }
    return ret;
  }
  return ret;
}

//zzy:裁剪文件变为offset大小，参考dfuse_truncate
int MetaFS::Truncate(const char * path ,off_t offset, struct fuse_file_info *fi){
  KVFS_LOG("Truncate:%s %d\n", path, offset);
  inode_id_t key;
  if (!PathLookup(path, key)) {
      KVFS_LOG("Truncate: No such file or directory %s\n", path);
      return -errno;
  }
    off_t new_size = offset;
    string value;
    int ret = db_->InodeGet(key, value);
    if(ret == 0){  //该文件存在
      const tfs_inode_header *iheader = reinterpret_cast<const tfs_inode_header *>(value.data());

      if (iheader->has_blob > 0) {//daos中
        if (new_size > config_->GetThreshold()) {//裁剪后文件大小仍然大于阈值，仍然存储在daos中，参考dfuse_truncate
          // TruncateDiskFile(key,new_size);
          TruncateDaosFile(key, new_size);
        } else {//裁剪后的大小小于阈值，需要将daos中数据读出迁移到Itable中，删除daos中的数据
          char* buffer = new char[new_size];
          // MigrateDiskFileToBuffer(key, buffer, new_size);
          MigrateDaosFileToBuffer(key, buffer, new_size);
          UpdateInlineData(value, buffer, 0, new_size);
          delete [] buffer;
        }
      } else {
        if (new_size > config_->GetThreshold()) {//裁剪后大小大于阈值，将数据从Itable迁移到daos
          // int fd;
          // if (MigrateToDiskFile(key, value, fd, O_TRUNC|O_WRONLY) == 0) {
          //   if ((ret = ftruncate(fd, new_size)) == 0) {
          //     fsync(fd);
          //   }
          //   close(fd);
          // }
          MigrateToDaosFile(key, value, O_TRUNC|O_WRONLY);
        } else {
          TruncateInlineData(value, new_size);
        }
      }

      //update stat
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
      int ret = db_->InodePut(key, value);
      if(ret != 0){
        return -EDBERROR;
      }
      
    } else if (ret == 1){  //该文件不存在
      return -ENOENT;
    } else {
        return -EDBERROR;
    }

  return ret;
}

//zzy:使用DAOS时什么也不做（DOAS自己说要生成一个快照，还未实现）
int MetaFS::Fsync(const char * path,int datasync ,struct fuse_file_info * fi){
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
            int ret = db_->Sync();
            if(ret != 0){
              return -EDBERROR;
            }
        }
    }
    return -ret;
}

int MetaFS::Release(const char * path ,struct fuse_file_info * fi){
  KVFS_LOG("Release:%s \n", path);
  kvfs_file_handle* handle = reinterpret_cast<kvfs_file_handle*>(fi->fh);
  inode_id_t key = handle->key;

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
  //   // ret = close(handle->fd);
  //   KVFS_LOG("daos_array_close");
  //   ret = daos_array_close(handle->oh, NULL);
  //   if(ret) {
  //     KVFS_LOG("error");
  //   }
  // }
  ret = db_->InodePut(key, handle->value);
  if(ret != 0){
    return -EDBERROR;
  }
  kvfs_file_handle::DeleteHandle(key);

  if (ret != 0) {
    return -errno;
  } else {
    return 0;
  }
}

int MetaFS::Readlink(const char * path ,char * buf,size_t size){
  KVFS_LOG("Readlink:%s \n", path);
  inode_id_t key;
  if (!PathLookup(path, key)) {
      KVFS_LOG("Readlink: No such file or directory %s\n", path);
      return -errno;
  }
  
  std::string result;
  int ret = 0;
  ret = db_->InodeGet(key, result);
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

//zzy
int MetaFS::Symlink(const char * target , const char * path){
  // KVFS_LOG("Symlink:not implement");
  // return -ENOTIMPLEMENT;
  KVFS_LOG("Symlink: path:%s, target:%s.", path, target);
  inode_id_t key;
  inode_id_t parent_id;
  string fname;
  if (!PathLookup(path, key, parent_id, fname)) {
    KVFS_LOG("Symlink: No such file or directory %s\n", path);
    return -errno;
  }
  std::string value;
  int ret = 0;
  ret = db_->InodeGet(key, value);
  if(ret == 0){
    UpdateInlineData(value, target, 0, strlen(target));
    ret = db_->InodePut(key, value);
    if(ret != 0){
      return -EDBERROR;
    }
  }
  return ret;
}

//zzy:删除文件
int MetaFS::Unlink(const char * path){
  KVFS_LOG("Unlink:%s \n", path);
  inode_id_t key;
  inode_id_t parent_id;
  string fname;
  if (!PathLookup(path, key, parent_id, fname)) {
    KVFS_LOG("Unlink: No such file or directory %s\n", path);
    return -errno;
  }
  KVFS_LOG("PathLookup find:pid:%llu,fname:%s,key:%llu",parent_id, path, key);
  std::string value;
  int ret = 0;
  ret = db_->InodeGet(key, value);
  if(ret == 0){
    const tfs_inode_header *iheader = reinterpret_cast<const tfs_inode_header *>(value.data());
    if(iheader->has_blob > 0){//存储在daos
      //zzy:daos_obj_open,daos_obj_punch,参考dfs remove_entry
      // string fpath = GetDiskFilePath(key);
      // fpath += '\0';
      // unlink(fpath.c_str());

      daos_handle_t oh;
      daos_obj_id_t oid;
      
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
      // assert(ret == 0);
    }
    kvfs_file_handle::DeleteHandle(key);//删除对应的handle
    int ret = db_->DirDelete(parent_id, fname);
    if(ret != 0){
      return -EDBERROR;
    }
    ret = db_->InodeDelete(key);
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

//zzy：需要把key生成为128位的
int MetaFS::MakeNode(const char * path,mode_t mode ,dev_t dev){
  KVFS_LOG("MakeNode:%s", path);
  inode_id_t parent_id;
  string filename;
  if (!ParentPathLookup(path, parent_id, filename)) {
    KVFS_LOG("MakeNode: No such file or directory %s\n", path);
    return -errno;
  }
  // uint64_t key = config_->NewInode();
  //生成128位的key(inode_id)
  inode_id_t key;
  config_->GetObjId(key);

  string value = InitInodeValue(key, mode | S_IFREG, dev);
  
  int ret=db_->DirPut(parent_id, filename, key);
  if(ret != 0){
    KVFS_LOG("MakeNode dirput error: %d %s %d\n", parent_id, filename.c_str(), key);
    return -EDBERROR;
  }
  ret = db_->InodePut(key, value);
  if(ret != 0){
    KVFS_LOG("MakeNode inodeput error: %d %s %d\n", parent_id, filename.c_str(), key);
    return -EDBERROR;
  }

  return 0;
}

//zzy：需要把key转换为128位的
int MetaFS::MakeDir(const char * path,mode_t mode){
  KVFS_LOG("MakeDir:%s", path);
  inode_id_t parent_id;
  string filename;
  if (!ParentPathLookup(path, parent_id, filename)) {
    KVFS_LOG("MakeDir: No such file or directory %s\n", path);
    return -errno;
  }

  // inode_id_t key = config_->NewInode();
  inode_id_t key;
  config_->GetObjId(key);

  string value = InitInodeValue(key, mode | S_IFDIR, 0);

  int ret=db_->DirPut(parent_id, filename, key);
  if(ret != 0){
    return -EDBERROR;
  }
  ret = db_->InodePut(key, value);
  if(ret != 0){
    return -EDBERROR;
  }

  return 0;
}

int MetaFS::OpenDir(const char * path,struct fuse_file_info *fi){
  KVFS_LOG("OpenDir:%s", path);
  inode_id_t key;
  if (!PathLookup(path, key)) {
    KVFS_LOG("OpenDir: No such file or directory %s\n", path);
    return -errno;
  }
  std::string value;
  int ret = 0;
  ret = db_->InodeGet(key, value);
  if(ret == 0){
    kvfs_file_handle * handle = new kvfs_file_handle(key);
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

int MetaFS::ReadDir(const char * path,void * buf ,fuse_fill_dir_t filler,off_t offset ,struct fuse_file_info * fi, enum fuse_readdir_flags flag){
  KVFS_LOG("ReadDir:%s", path);
  kvfs_file_handle * handle = reinterpret_cast <kvfs_file_handle *>(fi->fh);
  inode_id_t key = handle->key;
  int ret = 0;
  
  Iterator* iter = db_->DirGetIterator(key);
  if (filler(buf, ".", NULL, 0, (enum fuse_fill_dir_flags) 0) < 0) {
    KVFS_LOG("filler . error\n");
    return -errno;
  }
  if (filler(buf, "..", NULL, 0, (enum fuse_fill_dir_flags) 0) < 0) {
    KVFS_LOG("filler .. error\n");
    return -errno;
  }
  if(iter == nullptr){
    return 0;
  }
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    string fname = iter->fname();
    fname += '\0';
    const char* name_buffer = fname.data();
    if (name_buffer[0] == '\0') {
        continue;
    }
    KVFS_LOG("readdir: %s\n", name_buffer);
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

int MetaFS::ReleaseDir(const char * path,struct fuse_file_info * fi){
  KVFS_LOG("ReleaseDir:%s", path);
  kvfs_file_handle * handle = reinterpret_cast <kvfs_file_handle *>(fi->fh);
  inode_id_t key = handle->key;
  kvfs_file_handle::DeleteHandle(key);
  return 0;
}

//zzy:TODO这个函数写的有问题：不应该只删除dir吧,参考daos的remove_dir_contents
int MetaFS::RemoveDir(const char * path){
  //感觉要删除整个目录的所有文件
  KVFS_LOG("RemoveDir:%s", path);
  inode_id_t key;
  inode_id_t parent_id;
  string fname;
  if (!PathLookup(path, key, parent_id, fname)) {
    KVFS_LOG("Unlink: No such file or directory %s\n", path);
    return -errno;
  }
  std::string value;
  int ret = 0;
  ret = db_->InodeGet(key, value);
  if(ret == 0){
    kvfs_file_handle::DeleteHandle(key);
    int ret = db_->DirDelete(parent_id, fname);
    if(ret != 0){
      return -EDBERROR;
    }
    ret = db_->InodeDelete(key);
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

// //zzy:TODO这个函数写的有问题：不应该只删除dir吧,参考daos的remove_dir_contents
// int MetaFS::RemoveDir(const char * path){
//   //感觉要删除整个目录的所有文件
//   KVFS_LOG("RemoveDir:%s", path);
//   inode_id_t key;
//   inode_id_t parent_id;
//   string fname;
//   if (!PathLookup(path, key, parent_id, fname)) {
//     KVFS_LOG("Unlink: No such file or directory %s\n", path);
//     return -errno;
//   }
//   std::string value;
//   int ret = 0;
//   ret = db_->InodeGet(key, value);
//   //解析出stat
//   const tfs_stat_t* inode_value = GetAttribute(value);
//   //如果是目录，递归删除
//   if(S_ISDIR(inode_value->st_mode)) {
//     ret = RemoveDirContents(key);
//   } else {
//     KVFS_LOG("RemoveDir:%s is not a dir!", path);
//     return 0;
//   }
  
//   if(ret == 0){
//     kvfs_file_handle::DeleteHandle(key);
//     int ret = db_->DirDelete(parent_id, fname);
//     if(ret != 0){
//       return -EDBERROR;
//     }
//     ret = db_->InodeDelete(key);
//     if(ret != 0){
//       return -EDBERROR;
//     }

//     return 0;
//   } else if(ret == 1){
//     return -ENOENT;
//   } else{
//     return -EDBERROR;
//   }
  
// }

int MetaFS::RemoveDirContents(inode_id_t parent_id) {
  int ret = 0;

  KVFS_LOG("RemoveDirContents\n");
  
  Iterator* iter = db_->DirGetIterator(parent_id);
  if(iter == nullptr){
    return 0;
  }

  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    string fname = iter->fname();
    fname += '\0';
    if(fname[0] = '\0') {
      continue;
    }
    inode_id_t child_id;
    db_->DirGet(parent_id, fname, child_id);
    std::string value;
    ret = db_->InodeGet(child_id, value);
    if(ret != 0){
        KVFS_LOG("error");
    }
    //解析出stat
    const tfs_stat_t* inode_value = GetAttribute(value);
    //如果是目录，递归删除
    if(S_ISDIR(inode_value->st_mode)) {
      RemoveDirContents(child_id);
    } else if(S_ISLNK(inode_value->st_mode) || S_ISREG(inode_value->st_mode)) {
      // remove itself
      KVFS_LOG("remove link\n");
      const tfs_inode_header *iheader = reinterpret_cast<const tfs_inode_header *>(value.data());
      if(iheader->has_blob > 0){//存储在daos
        //zzy:daos_obj_open,daos_obj_punch,参考dfs remove_entry
        // string fpath = GetDiskFilePath(key);
        // fpath += '\0';
        // unlink(fpath.c_str());

        daos_handle_t oh;
        daos_obj_id_t oid;
        
        config_->GetDaosObjId(&oid, child_id);

        // ret = daos_obj_open(config_->coh, oid, DAOS_OO_RW, &oh, NULL);
        // KVFS_LOG("daos_array_open_with_attr");
        // ret = daos_array_open_with_attr(config_->coh, oid, DAOS_TX_NONE, DAOS_OO_RW, 1, FS_DEFAULT_CHUNK_SIZE, &oh, NULL);
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
        // assert(ret == 0);
      }
    }

    kvfs_file_handle::DeleteHandle(child_id);
    ret = db_->DirDelete(parent_id, fname);
    if(ret != 0){
      KVFS_LOG("error");
    }
    ret = db_->InodeDelete(child_id);
    if(ret != 0){
      KVFS_LOG("error");
    }
  }
  delete iter;

  return ret;
}


int MetaFS::Rename(const char *new_path,const char * old_path, unsigned int flags){
  KVFS_LOG("Rename:%s %s", new_path, old_path);
  inode_id_t old_key;
  inode_id_t old_parent_key;
  string old_fname;
  if (!PathLookup(old_path, old_key, old_parent_key, old_fname)) {
    KVFS_LOG("OpenDir: No such file or directory %s\n", old_path);
    return -errno;
  }
  inode_id_t new_key = old_key;
  inode_id_t new_parent_key;
  string new_fname;
  if (!ParentPathLookup(new_path, new_parent_key, new_fname)) {
    KVFS_LOG("OpenDir: No such file or directory %s\n", new_path);
    return -errno;
  }
  int ret = db_->DirPut(new_parent_key, new_fname,new_key);
  if(ret != 0){
    return -EDBERROR;
  }
  ret = db_->DirDelete(old_parent_key,old_fname);
  if(ret != 0){
    return -EDBERROR;
  }
  kvfs_file_handle::DeleteHandle(old_key);
  return 0;

}

int MetaFS::Access(const char * path,int mask){
  KVFS_LOG("Access:not implement:%s", path);
  //return -ENOTIMPLEMENT;
  return 0;
}

int MetaFS::Chmod(const char * path , mode_t mode, struct fuse_file_info *fi){
  KVFS_LOG("Chmod:%s", path);
  inode_id_t key;
  if (!PathLookup(path, key)) {
    KVFS_LOG("Chmod: No such file or directory %s\n", path);
    return -errno;
  }
  std::string value;
  int ret = 0;
  ret = db_->InodeGet(key, value);
  if(ret == 0){
    kvfs_file_handle * handle = kvfs_file_handle ::GetHandle(key);
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
    ret=db_->InodePut(key, *mu_value);
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

int MetaFS::Chown(const char * path, uid_t uid,gid_t gid, struct fuse_file_info *fi){
  KVFS_LOG("Chown:%s", path);
  inode_id_t key;
  if (!PathLookup(path, key)) {
    KVFS_LOG("Chown: No such file or directory %s\n", path);
    return -errno;
  }
  std::string value;
  int ret = 0;
  ret = db_->InodeGet(key, value);
  if(ret == 0){
    kvfs_file_handle * handle = kvfs_file_handle ::GetHandle(key);
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
    ret=db_->InodePut(key, *mu_value);
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

int MetaFS::UpdateTimens(const char * path ,const struct timespec tv[2], struct fuse_file_info *fi){
  KVFS_LOG("UpdateTimens:%s", path);
  inode_id_t key;
  if (!PathLookup(path, key)) {
    KVFS_LOG("Chown: No such file or directory %s\n", path);
    return -errno;
  }
  std::string value;
  int ret = 0;
  ret = db_->InodeGet(key, value);
  if(ret == 0){
    kvfs_file_handle * handle = kvfs_file_handle ::GetHandle(key);
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
    ret=db_->InodePut(key, *mu_value);
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