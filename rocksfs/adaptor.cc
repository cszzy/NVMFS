#include "adaptor.h"
namespace rocksfs {

const std::string DBAdaptor ::default_DBpath = "/tmp/rocksdb";

DBAdaptor::DBAdaptor() : inited_(false){
    //default options
}

DBAdaptor::~DBAdaptor(){

}

int DBAdaptor::Init(const std::string & path){

    Options option;
    option.create_if_missing = true;
    option.compression = rocksdb::kNoCompression;

    if( path == "")
    {
        KVFS_LOG("DBAdaptor : use default path\n");
        Status s = DB::Open(option,default_DBpath,&db_);
        KVFS_LOG("status : %s",s.ToString().c_str());
        assert(s.ok());
        return 0;
    }
    else 
    {
        KVFS_LOG("DBAdaptor : use path %s\n",path.c_str());
        Status s = DB::Open(option,path,&db_);
        KVFS_LOG(s.ToString().c_str());
        assert(s.ok());
        return 0;
    }
}

int DBAdaptor::Get(const rocksdb::Slice &key, std::string &result){
    ReadOptions options;
    Status s = db_->Get(options, key, &result);
    
    if(s.ok()){
        return 0;
    } else if(s.IsNotFound()) {
        return 1;
    } else {
        return -1;
    }
}

int DBAdaptor::Put(const rocksdb::Slice &key, const rocksdb::Slice &values){
    WriteOptions write_options;
    rocksdb::Status s = db_->Put(write_options, key, values);
    if(s.ok()){
        return 0;
    } else {
        return -1;
    }
}

int DBAdaptor::Delete(const rocksdb::Slice &key){
    WriteOptions write_options;
    rocksdb::Status s = db_->Delete(write_options, key);
    if(s.ok()){
        return 0;
    } else {
        return -1;
    }
}
int DBAdaptor::Sync(){
    WriteOptions write_options;
    write_options.sync = true;
    Status s = db_->Put(write_options,"sync","");
    if(s.ok()){
        return 0;
    } else {
        return -1;
    }
}
void DBAdaptor::Cleanup(){
    delete db_;
    db_ = nullptr;
}

} // namespace name