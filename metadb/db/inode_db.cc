/**
 * @Author      : Liu Zhiwen
 * @Create Date : 2021-01-13 15:01:35
 * @Contact     : 993096281@qq.com
 * @Description : 
 */

#include "inode_db.h"
#include "metadb/debug.h"

namespace metadb {

InodeDB::InodeDB(const Option &option, uint64_t capacity) : option_(option), capacity_(capacity){
    zones_ = new InodeZone[capacity_];
    for(uint32_t i = 0; i < capacity; i++){
        zones_[i].InitInodeZone(option, i);
    }
}

InodeDB::~InodeDB(){
    delete[] zones_;
}

int InodeDB::InodePut(const inode_id_t key, const Slice &value){
    return zones_[hash_zone_id(key)].InodePut(key, value);
}

int InodeDB::InodeUpdate(const inode_id_t key, const Slice &new_value){
    return zones_[hash_zone_id(key)].InodeUpdate(key, new_value);
}

int InodeDB::InodeGet(const inode_id_t key, std::string &value){
    return zones_[hash_zone_id(key)].InodeGet(key, value);
}

int InodeDB::InodeDelete(const inode_id_t key){
    return zones_[hash_zone_id(key)].InodeDelete(key);
}

inline uint32_t InodeDB::hash_zone_id(const inode_id_t key){
    return key % capacity_;
}

void InodeDB::PrintInode(){
    DBG_LOG("[inode] Print inode, capacity:%lu", capacity_);
    for(uint64_t i = 0; i < capacity_; i++){
        DBG_LOG("[inode] Print zone:%lu", i);
        zones_[i].PrintZone();
    }
}

void InodeDB::PrintInodeStats(std::string &stats){
    stats.append("--------Inode--------\n");
    char buf[1024];
    snprintf(buf, sizeof(buf), "Inode capacity:%lu\n", capacity_);
    stats.append(buf);

    uint64_t all_file_nums = 0;
    uint64_t all_write_lens = 0;
    uint64_t all_kv_nums = 0;
    uint64_t all_invalid_kv_nums = 0;
    uint64_t all_hashtable_node_nums = 0;
    uint64_t all_hashtable_kv_nums = 0;

    for(uint64_t i = 0; i < capacity_; i++){
        uint64_t file_nums = 0;
        uint64_t write_lens = 0;
        uint64_t kv_nums = 0;
        uint64_t invalid_kv_nums = 0;
        uint64_t hashtable_node_nums = 0;
        uint64_t hashtable_kv_nums = 0;
        snprintf(buf, sizeof(buf), "zone:%lu ", i);
        stats.append(buf);
        stats.append(zones_[i].PrintZoneStats(file_nums, write_lens, kv_nums, invalid_kv_nums, hashtable_node_nums, hashtable_kv_nums));
        
        all_file_nums += file_nums;
        all_write_lens += write_lens;
        all_kv_nums += kv_nums;
        all_invalid_kv_nums += invalid_kv_nums;
        all_hashtable_node_nums += hashtable_node_nums;
        all_hashtable_kv_nums += hashtable_kv_nums;
    }
    snprintf(buf, sizeof(buf), "Inode capacity:%lu all file_nums:%lu write_lens:%lu kv_nums:%lu invalid_kv_nums:%lu hashtable_node_nums:%lu hashtable_kv_nums:%lu\n", capacity_, \
            all_file_nums, all_write_lens, all_kv_nums, all_invalid_kv_nums, all_hashtable_node_nums, all_hashtable_kv_nums);
    stats.append(buf);
    
    stats.append("---------------------\n");
}

} // namespace name