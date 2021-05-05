/**
 * @Author      : Liu Zhiwen
 * @Create Date : 2020-12-09 14:50:26
 * @Contact     : 993096281@qq.com
 * @Description : 
 */

#ifndef _METADB_ITERATOR_H_
#define _METADB_ITERATOR_H_

#include <string>
#include "metadb/slice.h"
#include "metadb/inode.h"

using namespace std;
namespace metadb {

class Iterator {
public:
    Iterator() {};
    virtual ~Iterator() {};

    virtual bool Valid() const = 0;
    //virtual void Seek(const Slice& target) = 0;
    virtual void SeekToFirst() = 0;
    virtual void SeekToLast() = 0;
    virtual void Next() = 0;
    virtual void Prev() = 0;
    virtual string fname() const = 0;
    virtual uint64_t hash_fname() const = 0;
    virtual inode_id_t value() const = 0;
};


}





#endif
