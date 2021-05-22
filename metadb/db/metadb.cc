/**
 * @Author      : Liu Zhiwen
 * @Create Date : 2020-12-09 20:00:16
 * @Contact     : 993096281@qq.com
 * @Description : 
 */

#include "metadb.h"
#include "nvm_node_allocator.h"
#include "nvm_file_allocator.h"
#include "thread_pool.h"

namespace metadb {

int DB::Open(const Option &option, const std::string &name, DB **dbptr){
    *dbptr = new MetaDB(option, name);
    return 0;
}

MetaDB::MetaDB(const Option &option, const std::string &name) : option_(option), db_name_(name) {
    option_.Print();
    if(!option.node_allocator_path.empty()) InitNVMNodeAllocator(option.node_allocator_path, option.node_allocator_size);
    if(!option.file_allocator_path.empty()) InitNVMFileAllocator(option.file_allocator_path, option.file_allocator_size);
    InitThreadPool(option.thread_pool_count);
    dir_db_ = new DirDB(option);
    inode_db_ = new InodeDB(option, option.INODE_MAX_ZONE_NUM);
}

MetaDB::~MetaDB(){
    if(thread_pool) delete thread_pool;
    delete dir_db_;
    delete inode_db_;
    if(node_allocator) delete node_allocator;
    if(file_allocator) delete file_allocator;
}
int MetaDB::DirPut(const inode_id_t key, const Slice &fname, const inode_id_t value){
    return dir_db_->DirPut(key, fname, value);
}

int MetaDB::DirGet(const inode_id_t key, const Slice &fname, inode_id_t &value){
    return dir_db_->DirGet(key, fname, value);
}

int MetaDB::DirDelete(const inode_id_t key, const Slice &fname){
    return dir_db_->DirDelete(key, fname);
}

Iterator* MetaDB::DirGetIterator(const inode_id_t target){
    return dir_db_->DirGetIterator(target);
}
    
int MetaDB::InodePut(const inode_id_t key, const Slice &value){
    return inode_db_->InodePut(key, value);
}

int MetaDB::InodeUpdate(const inode_id_t key, const Slice &new_value){
    return inode_db_->InodeUpdate(key, new_value);
}

int MetaDB::InodeGet(const inode_id_t key, std::string &value){
    return inode_db_->InodeGet(key, value);
}

int MetaDB::InodeDelete(const inode_id_t key){
    return inode_db_->InodeDelete(key);
}

void MetaDB::WaitForBGJob(){
    if(thread_pool)  thread_pool->WaitForBGJob();
}

void MetaDB::PrintDir(){
    dir_db_->PrintDir();
}

void MetaDB::PrintInode(){
    inode_db_->PrintInode();
}

void MetaDB::PrintDirStats(std::string &stats){
    dir_db_->PrintStats(stats);
}

void MetaDB::PrintInodeStats(std::string &stats){
    inode_db_->PrintInodeStats(stats);
}

void MetaDB::PrintNodeAllocStats(std::string &stats){
    if(node_allocator != nullptr) node_allocator->PrintNodeAllocatorStats(stats);
}
void MetaDB::PrintFileAllocStats(std::string &stats){
    if(file_allocator != nullptr) file_allocator->PrintFileAllocatorStats(stats);
}

void MetaDB::PrintAllStats(std::string &stats){
    dir_db_->PrintStats(stats);
    inode_db_->PrintInodeStats(stats);
    if(node_allocator != nullptr) node_allocator->PrintNodeAllocatorStats(stats);
    if(file_allocator != nullptr) file_allocator->PrintFileAllocatorStats(stats);
}

}