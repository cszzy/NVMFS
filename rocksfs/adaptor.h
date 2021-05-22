#ifndef _ROCKSFS_ADAPTOR_H_
#define _ROCKSFS_ADAPTOR_H_


#include "rocksdb/db.h"
#include <string>
#include "inode_format.h"

namespace rocksfs {

using namespace rocksdb;

class DBAdaptor {
public:
    DBAdaptor();
    ~DBAdaptor();

    void Cleanup();

    int Get(const rocksdb::Slice &key, std::string &result);
    int Put(const rocksdb::Slice &key, const rocksdb::Slice &values);
    int Delete(const rocksdb::Slice &key);

    int Sync();

    int Init(const std::string & path = "");

    inline Iterator * NewIterator() { return db_->NewIterator(ReadOptions()); }

protected:
    bool inited_;
    DB * db_;
    Options options_;
    static const std::string default_DBpath;

};

}







#endif