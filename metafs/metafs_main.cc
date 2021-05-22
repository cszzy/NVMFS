#include <stdio.h>
#include <iostream>
#include <unistd.h>

#include "metafs.h"

using namespace metafs;

static MetaFS *fs;

int wrap_getattr(const char * path,struct stat * statbuf, struct fuse_file_info *fi)
{
    return fs->GetAttr(path,statbuf, fi);
}
int wrap_readlink(const char * path ,char * link , size_t size)
{
    return fs->Readlink(path,link,size);
}
int wrap_mknod(const char * path , mode_t mode ,dev_t dev)
{
    return fs->MakeNode(path,mode,dev);
}
int wrap_mkdir (const char * path, mode_t mode)
{
    return fs->MakeDir(path,mode);
}
int wrap_unlink(const char * path)
{
    return fs->Unlink(path);
}
int wrap_rmdir(const char * path)
{
    return fs->RemoveDir(path);
}
int wrap_symlink(const char * path , const char * link)
{
    return fs->Symlink(path,link);
}
int wrap_rename(const char * path , const char * newpath, unsigned int flags)
{
    return fs->Rename(path,newpath, flags);
}

int wrap_chmod(const char *path, mode_t mode, struct fuse_file_info *fi) {
      return fs->Chmod(path, mode, fi);
}
int wrap_chown(const char *path, uid_t uid, gid_t gid, struct fuse_file_info *fi) {
      return fs->Chown(path, uid, gid, fi);
}
int wrap_truncate(const char *path, off_t newSize, struct fuse_file_info *fi) {
      return fs->Truncate(path, newSize, fi);
}
int wrap_open(const char *path, struct fuse_file_info *fileInfo) {
      return fs->Open(path, fileInfo);
}
int wrap_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fileInfo) {
      return fs->Read(path, buf, size, offset, fileInfo);
}
int wrap_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fileInfo) {
      return fs->Write(path, buf, size, offset, fileInfo);
}
int wrap_release(const char *path, struct fuse_file_info *fileInfo) {
      return fs->Release(path, fileInfo);
}
int wrap_opendir(const char *path, struct fuse_file_info *fileInfo) {
      return fs->OpenDir(path, fileInfo);
}
int wrap_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fileInfo, enum fuse_readdir_flags flag) {
      return fs->ReadDir(path, buf, filler, offset, fileInfo, flag);
}
int wrap_releasedir(const char *path, struct fuse_file_info *fileInfo) {
      return fs->ReleaseDir(path, fileInfo);
}
void* wrap_init(struct fuse_conn_info *conn, struct fuse_config *cfg) {
      return fs->Init(conn, cfg);
}
int wrap_access(const char *path, int mask) {
      return fs->Access(path, mask);
}
int wrap_utimens(const char *path, const struct timespec tv[2], struct fuse_file_info *fi) {
      return fs->UpdateTimens(path, tv, fi);
}
void wrap_destroy(void * data) {
      fs->Destroy(data);
}

void parse_args(int argc , char * argv[],kvfs_args & args)
{
    for(int i = 1; i < argc ;++ i)
    {
        if(argv[i][0] == '-')
        {
            args[argv[i] +1] = argv[i+1];
        }
        printf("option : %s value %s \n",argv[i],argv[i+1]);
        ++i;
    }
}

static struct fuse_operations kvfs_operations;

bool FileExists(const std::string& fname) {
    return access(fname.c_str(), F_OK) == 0;
}

//zzy:方法1：在parse参数时解析pool uuid和container uuid，打开池和容器
//方法2：调用dmg函数，不太可行
//模仿dfuse_main，连接到pool和cont
//看dfuse至少使用2个线程，一个线程用于调度
int main(int argc , char * argv[])
{
    kvfs_args args;
    parse_args(argc,argv,args);
    //std::cout <<"meta key size:"<<sizeof(kvfs_inode_meta_key) <<std::endl;
    //std::cout <<"header   size:"<<sizeof(kvfs_inode_header) <<std::endl;
    //zzy:todo 在new metafs时daos_init(),连接到pool和container
    fs = new MetaFS(args);
    
    std::string mountdir = args.at("mount_dir");
    std::string datadir = args.at("data_dir");
    std::string metadir = args.at("meta_dir");

    if( !(FileExists(mountdir) && 
        FileExists(datadir) &&
       FileExists(metadir)))
    {
        fprintf(stderr, "Some input directories cannot be found.\n");
    }

    char * fuse_argv[20];

    int fuse_argc = 0;
    fuse_argv[fuse_argc++] = argv[0];
    char fuse_mount_dir[100];
    strcpy(fuse_mount_dir , mountdir.c_str());
    fuse_argv[fuse_argc++] = fuse_mount_dir;
    // char fuse_opt_s[20] = "-s";  //-s disable multi-threaded operation
    fuse_argv[fuse_argc++] = fuse_opt_s;

    kvfs_operations.init = wrap_init;
    kvfs_operations.getattr = wrap_getattr;
    kvfs_operations.opendir = wrap_opendir;
    kvfs_operations.readdir = wrap_readdir;
    kvfs_operations.releasedir = wrap_releasedir;
    kvfs_operations.mkdir = wrap_mkdir;
    kvfs_operations.rmdir = wrap_rmdir;
    kvfs_operations.rename = wrap_rename;

    kvfs_operations.symlink = wrap_symlink;
    kvfs_operations.readlink = wrap_readlink;

    kvfs_operations.open = wrap_open;
    kvfs_operations.read = wrap_read;
    kvfs_operations.write = wrap_write;

    kvfs_operations.mknod      = wrap_mknod;
    kvfs_operations.unlink     = wrap_unlink;
    kvfs_operations.release    = wrap_release;
    kvfs_operations.chmod      = wrap_chmod;
    kvfs_operations.chown      = wrap_chown;

    kvfs_operations.truncate   = wrap_truncate;
    kvfs_operations.access     = wrap_access;
    kvfs_operations.utimens    = wrap_utimens;
    kvfs_operations.destroy    = wrap_destroy;

    fprintf(stdout,"start to run fuse_main at %s %s \n",argv[0],fuse_mount_dir);

    int fuse_stat = fuse_main(fuse_argc,fuse_argv,&kvfs_operations,NULL);

    return fuse_stat;

}

