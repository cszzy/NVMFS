/**
 * @Author      : Liu Zhiwen
 * @Create Date : 2020-12-17 15:34:51
 * @Contact     : 993096281@qq.com
 * @Description : 
 */

#include "dir_db.h"


namespace metadb {

DirDB::DirDB(const Option &option) : option_(option) {
    assert(sizeof(LinkNode) == DIR_LINK_NODE_SIZE);
    assert(sizeof(BptreeIndexNode) == DIR_BPTREE_INDEX_NODE_SIZE);
    assert(sizeof(BptreeLeafNode) == DIR_BPTREE_LEAF_NODE_SIZE);
    hashtable_ = new DirHashTable(option, 1, option_.DIR_FIRST_HASH_MAX_CAPACITY);
}

DirDB::~DirDB(){
    delete hashtable_;
}

int DirDB::DirPut(const inode_id_t key, const Slice &fname, const inode_id_t value){
    return hashtable_->Put(key, fname, value);
}

int DirDB::DirGet(const inode_id_t key, const Slice &fname, inode_id_t &value){
    return hashtable_->Get(key, fname, value);
}

int DirDB::DirDelete(const inode_id_t key, const Slice &fname){
    return hashtable_->Delete(key, fname);
}

Iterator* DirDB::DirGetIterator(const inode_id_t target){
    return hashtable_->DirHashTableGetIterator(target);
}

void DirDB::PrintDir(){
    hashtable_->PrintHashTable();
}

void DirDB::PrintStats(std::string &stats){
    hashtable_->PrintHashTableStats(stats);
}

} // namespace name