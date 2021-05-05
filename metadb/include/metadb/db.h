/**
 * @Author      : Liu Zhiwen
 * @Create Date : 2020-12-09 14:17:09
 * @Contact     : 993096281@qq.com
 * @Description : 
 */
#ifndef _METADB_DB_H_
#define _METADB_DB_H_


#include <string>
#include "metadb/option.h"
#include "metadb/slice.h"
#include "metadb/inode.h"
#include "metadb/iterator.h"

namespace metadb {


class DB {
public:
    static int Open(const Option &option, const std::string &name, DB **dbptr);

    DB() { };
    virtual ~DB() { };

    virtual int DirPut(const inode_id_t key, const Slice &fname, const inode_id_t value) = 0;
    virtual int DirGet(const inode_id_t key, const Slice &fname, inode_id_t &value) = 0;
    virtual int DirDelete(const inode_id_t key, const Slice &fname) = 0;
    virtual Iterator* DirGetIterator(const inode_id_t target) = 0;

    virtual int InodePut(const inode_id_t key, const Slice &value) = 0;
    virtual int InodeGet(const inode_id_t key, std::string &value) = 0;
    virtual int InodeDelete(const inode_id_t key) = 0;


    virtual void WaitForBGJob() = 0;
    ///统计
    virtual void PrintDir() = 0;
    virtual void PrintInode() = 0;
    virtual void PrintDirStats(std::string &stats) = 0;
    virtual void PrintInodeStats(std::string &stats) = 0;

    virtual void PrintNodeAllocStats(std::string &stats) = 0;
    virtual void PrintFileAllocStats(std::string &stats) = 0;

    virtual void PrintAllStats(std::string &stats) = 0;
};


}








#endif