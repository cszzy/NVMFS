#include "adaptor.h"
namespace metafs {

const std::string DBAdaptor ::default_DBpath = "/tmp/metafs";

DBAdaptor::DBAdaptor() : inited_(false){
    //default options
}

DBAdaptor::~DBAdaptor(){

}

int DBAdaptor::Init(const std::string & path){

    Option option;
    option.node_allocator_path = path + "/" + "node.pool";
    option.node_allocator_size = 20ULL * 1024 * 1024 * 1024;
    option.file_allocator_path = path + "/" + "file.pool";
    option.file_allocator_size = 20ULL * 1024 * 1024 * 1024;
    

    if( path == "")
    {
        KVFS_LOG("DBAdaptor : use default path\n");
        int s = DB::Open(option,default_DBpath,&db_);
        assert(s==0);
        return 0;
    }
    else 
    {
        KVFS_LOG("DBAdaptor : use path %s\n",path.c_str());
        int s = DB::Open(option,path,&db_);
        assert(s==0);
        return 0;
    }
}

int DBAdaptor::Sync(){
    return 0;
}
void DBAdaptor::Cleanup(){
    delete db_;
    db_ = nullptr;
}

int DBAdaptor::DirPut(const inode_id_t key, const Slice &fname, const inode_id_t value){
    int ret = db_->DirPut(key, fname, value);
    if(ret == 0){
        return 0;
    } else {
        return -1;
    }
}
int DBAdaptor::DirGet(const inode_id_t key, const Slice &fname, inode_id_t &value){
    int ret = db_->DirGet(key, fname, value);
    if(ret == 0){
        return 0;
    } else if (ret == 2){  //未找到
        return 1;
    } else {
        return -1;
    }
}
int DBAdaptor::DirDelete(const inode_id_t key, const Slice &fname){
    int ret = db_->DirDelete(key, fname);
    if(ret == 0 || ret == 1 || ret == 2){
        return 0;
    } else {
        return -1;
    }
}
Iterator* DBAdaptor::DirGetIterator(const inode_id_t target){
    return db_->DirGetIterator(target);
}

int DBAdaptor::InodePut(const inode_id_t key, const Slice &value){
    int ret = db_->InodePut(key, value);
    if(ret == 0 || ret == 2){
        return 0;
    } else {
        return -1;
    }
}
int DBAdaptor::InodeGet(const inode_id_t key, std::string &value){
    int ret = db_->InodeGet(key, value);
    if(ret == 0 || ret == 2){
        return 0;
    } else {
        return -1;
    }

}
int DBAdaptor::InodeDelete(const inode_id_t key){
    int ret = db_->InodeDelete(key);
    if(ret == 0 || ret == 2){
        return 0;
    } else {
        return -1;
    }
}

} // namespace name