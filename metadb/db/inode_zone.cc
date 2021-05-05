/**
 * @Author      : Liu Zhiwen
 * @Create Date : 2021-01-05 19:49:05
 * @Contact     : 993096281@qq.com
 * @Description : 
 */
#include "inode_zone.h"
#include "metadb/debug.h"

namespace metadb {

InodeZone::InodeZone(const Option &option, uint32_t zone_id) : option_(option), zone_id_(zone_id) {
    write_file_ = nullptr;
    hashtable_ = new InodeHashTable(option, this);
}

void InodeZone::InitInodeZone(const Option &option, uint32_t zone_id){
    zone_id_ = zone_id;
    write_file_ = nullptr;
    option_ = option;
    hashtable_ = new InodeHashTable(option, this);
}

InodeZone::~InodeZone(){
    delete hashtable_;
    for(auto it : files_locks_){
        delete it.second;
    }
}

void InodeZone::FilesMapInsert(NVMInodeFile *file){
    files_mu_.Lock();
    files_.insert(make_pair(GetFileId(FILE_GET_OFFSET(file)), file));
    files_mu_.Unlock();

    Mutex *new_lock = new Mutex();
    locks_mu_.Lock();
    files_locks_.insert(make_pair(GetFileId(FILE_GET_OFFSET(file)), new_lock));
    locks_mu_.Unlock();
}

NVMInodeFile *InodeZone::FilesMapGet(uint64_t id){
    NVMInodeFile *file = nullptr;
    files_mu_.Lock();
    auto it = files_.find(id);
    if(it != files_.end()){
        file = it->second;
    }
    files_mu_.Unlock();
    return file;
}

NVMInodeFile *InodeZone::FilesMapGetAndGetLock(uint64_t id, Mutex **lock){
    NVMInodeFile *file = nullptr;
    files_mu_.Lock();
    auto it = files_.find(id);
    if(it != files_.end()){
        file = it->second;
    }
    files_mu_.Unlock();

    locks_mu_.Lock();
    auto it_lock = files_locks_.find(id);
    if(it_lock != files_locks_.end()){
        *lock = it_lock->second;
    }
    locks_mu_.Unlock();
    return file;
}

void InodeZone::FilesMapDelete(uint64_t id){
    NVMInodeFile *file = nullptr;
    Mutex *lock = nullptr;
    files_mu_.Lock();
    auto it = files_.find(id);
    if(it != files_.end()){
        file = it->second;
        files_.erase(it);
    }
    files_mu_.Unlock();

    locks_mu_.Lock();
    auto it_lock = files_locks_.find(id);
    if(it_lock != files_locks_.end()){
        files_locks_.erase(it_lock);
    }
    locks_mu_.Unlock();
    if(file != nullptr) file_allocator->Free(file, INODE_FILE_SIZE);
    if(lock != nullptr) delete lock;
}

pointer_t InodeZone::WriteFile(const inode_id_t key, const Slice &value){
    write_mu_.Lock();
    if(write_file_ == nullptr){
        write_file_ = AllocNVMInodeFlie();
        FilesMapInsert(write_file_);
    }
    pointer_t key_addr = 0;
    if(write_file_->InsertKV(key, value, key_addr) != 0){
        write_file_ = AllocNVMInodeFlie();
        FilesMapInsert(write_file_);
        write_file_->InsertKV(key, value, key_addr);
    }
    write_mu_.Unlock();
    return key_addr;
}

int InodeZone::InodePut(const inode_id_t key, const Slice &value){
    pointer_t key_offset = WriteFile(key, value);
    pointer_t old_value = INVALID_POINTER;
    int res = hashtable_->Put(key, key_offset, old_value);
    if(res == 2){  //key以存在
        DeleteFlie(old_value);
    }
    return res;
}

int InodeZone::ReadFile(pointer_t addr, std::string &value){
    uint64_t id = GetFileId(addr);
    uint64_t offset = GetFileOffset(addr);
    NVMInodeFile *file = FilesMapGet(id);
    if(file == nullptr) {
        ERROR_PRINT("not find file! addr:%lu id:%lu offset:%lu\n", addr, id, offset);
        return 2;
    }
    return file->GetKV(offset, value);
}

int InodeZone::InodeGet(const inode_id_t key, std::string &value){
    pointer_t addr;
    int res = hashtable_->Get(key, addr);
    if(res == 0){
        res = ReadFile(addr, value);
    }
    return res;
}

int InodeZone::DeleteFlie(pointer_t value_addr){
    uint64_t id = GetFileId(value_addr);
    uint64_t offset = GetFileOffset(value_addr);
    Mutex *file_lock;
    NVMInodeFile *file = FilesMapGetAndGetLock(id, &file_lock);
    if(file == nullptr){
         ERROR_PRINT("not find file! addr:%lu id:%lu offset:%lu\n", value_addr, id, offset);
        return 2;
    }
    file_lock->Lock();
    file->SetInvalidNumPersist(file->invalid_num + 1);
    file_lock->Unlock();
    if(file != write_file_ && file->num == file->invalid_num){  //直接回收
        FilesMapDelete(id);
    }
    return 0;
}

int InodeZone::InodeDelete(const inode_id_t key){
    pointer_t value_addr1 = INVALID_POINTER;
    pointer_t value_addr2 = INVALID_POINTER;
    int res = hashtable_->Delete(key, value_addr1, value_addr2);
    int res1 = 0;
    int res2 = 0;
    if(!IS_INVALID_POINTER(value_addr1)){
        res1 = DeleteFlie(value_addr1);
    }
    if(!IS_INVALID_POINTER(value_addr2)){
        res2 = DeleteFlie(value_addr2);
    }
    return res1 | res2;  //两个都没问题才返回0；
}

int InodeZone::InodeUpdate(const inode_id_t key, const Slice &new_value){
    pointer_t key_offset = WriteFile(key, new_value);
    pointer_t old_value = INVALID_POINTER;
    int res = hashtable_->Update(key, key_offset, old_value);
    if(res == 2){  //key以存在
        DeleteFlie(old_value);
    }
    return res;
}

void InodeZone::PrintZone(){
    hashtable_->PrintHashTable();
    uint64_t nums = 0;
    uint64_t invalid_nums = 0;
    for(auto it : files_){
        DBG_LOG("[inode] file:%lu num:%lu write_pointer:%lu invalid_num:%lu", it.first, it.second->num, \
                it.second->write_offset, it.second->invalid_num);
        nums += it.second->num;
        invalid_nums += it.second->invalid_num;
    }
    DBG_LOG("[inode] zone:%lu all files:%u nums:%lu invalid:%lu valid:%ld", zone_id_, files_.size(), nums, invalid_nums, nums - invalid_nums);
}

string InodeZone::PrintZoneStats(uint64_t &file_nums, uint64_t &write_lens, uint64_t &kv_nums, uint64_t &invalid_kv_nums, uint64_t &hashtable_node_nums, uint64_t &hashtable_kv_nums){
    string res;
    char buf[1024];
    file_nums = files_.size();
    for(auto it : files_){
        write_lens += it.second->write_offset;
        kv_nums += it.second->num;
        invalid_kv_nums += it.second->invalid_num;
    }
    snprintf(buf, sizeof(buf), "file_nums:%lu write_lens:%lu kv_nums:%lu invalid_kv_nums:%lu hashtable ", file_nums, write_lens, \
                kv_nums, invalid_kv_nums);
    res.append(buf);
    res.append(hashtable_->PrintHashTableStats(hashtable_node_nums, hashtable_kv_nums));
    return res;
}


} // namespace name