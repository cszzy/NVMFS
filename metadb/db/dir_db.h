/**
 * @Author      : Liu Zhiwen
 * @Create Date : 2020-12-09 20:01:22
 * @Contact     : 993096281@qq.com
 * @Description : 
 */
#ifndef _METADB_DIR_DB_H_
#define _METADB_DIR_DB_H_


#include <string>
#include "metadb/option.h"
#include "metadb/slice.h"
#include "metadb/inode.h"
#include "metadb/iterator.h"
#include "dir_hashtable.h"

namespace metadb {

class DirDB {
public:
    DirDB(const Option &option);
    virtual ~DirDB();

    virtual int DirPut(const inode_id_t key, const Slice &fname, const inode_id_t value);
    virtual int DirGet(const inode_id_t key, const Slice &fname, inode_id_t &value);
    virtual int DirDelete(const inode_id_t key, const Slice &fname);
    virtual Iterator* DirGetIterator(const inode_id_t target);

    virtual void PrintDir();
    virtual void PrintStats(std::string &stats);
private:
    const Option option_;
    DirHashTable *hashtable_;
};


}







#endif