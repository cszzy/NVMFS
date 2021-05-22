/**
 * @Author      : Liu Zhiwen
 * @Create Date : 2021-01-04 15:12:50
 * @Contact     : 993096281@qq.com
 * @Description : 
 */
#ifndef _METADB_NVM_FILE_ALLOCATOR_H_
#define _METADB_NVM_FILE_ALLOCATOR_H_

#include <stdint.h>
#include <string>
#include <vector>
#include <map>
#include <atomic>

#include "metadb/libnvm.h"
#include "../util/lock.h"
#include "../util/bitmap.h"
#include "format.h"

#define FILE_GET_OFFSET(dst) (reinterpret_cast<char *>(dst) - metadb::file_pool_pointer)     //void *-> offset
#define FILE_GET_POINTER(offset) (static_cast<void *>(metadb::file_pool_pointer + offset))  //offset -> void *

#define MAX_GROUP_BLOCK_TYPE  11

using namespace std;

namespace metadb {

class NVMFileAllocator;

extern char *file_pool_pointer;
extern NVMFileAllocator *file_allocator;

enum class NVMGroupBlockType : uint8_t {
    UNKNOWN_TYPE = 0,
    NVMGROUPBLOCK_1K_TYPE = 1,
    NVMGROUPBLOCK_4K_TYPE,
    NVMGROUPBLOCK_16K_TYPE,
    NVMGROUPBLOCK_64K_TYPE,
    NVMGROUPBLOCK_256K_TYPE,
    NVMGROUPBLOCK_1MB_TYPE,
    NVMGROUPBLOCK_4MB_TYPE,
    NVMGROUPBLOCK_16MB_TYPE,
    NVMGROUPBLOCK_32MB_TYPE,
    NVMGROUPBLOCK_64MB_TYPE
};

static inline uint64_t GetNVMGroupBlockSize(NVMGroupBlockType type){
    switch (type){
        case NVMGroupBlockType::UNKNOWN_TYPE:
            return 0;
        case NVMGroupBlockType::NVMGROUPBLOCK_1K_TYPE:
            return 1ULL * 1024;
        case NVMGroupBlockType::NVMGROUPBLOCK_4K_TYPE:
            return 4ULL * 1024;
        case NVMGroupBlockType::NVMGROUPBLOCK_16K_TYPE:
            return 16ULL * 1024;
        case NVMGroupBlockType::NVMGROUPBLOCK_64K_TYPE:
            return 64ULL * 1024;
        case NVMGroupBlockType::NVMGROUPBLOCK_256K_TYPE:
            return 256ULL * 1024;
        case NVMGroupBlockType::NVMGROUPBLOCK_1MB_TYPE:
            return 1ULL * 1024 * 1024;
        case NVMGroupBlockType::NVMGROUPBLOCK_4MB_TYPE:
            return 4ULL * 1024 * 1024;
        case NVMGroupBlockType::NVMGROUPBLOCK_16MB_TYPE:
            return 16ULL * 1024 * 1024;
        case NVMGroupBlockType::NVMGROUPBLOCK_32MB_TYPE:
            return 32ULL * 1024 * 1024;
        case NVMGroupBlockType::NVMGROUPBLOCK_64MB_TYPE:
            return 64ULL * 1024 * 1024;
        default:
            return 0;
    }
}


class NVMGroupManager {
public:
    NVMGroupManager(uint64_t id, NVMGroupBlockType type) : id_(id), type_(type) {
        uint64_t block_size = GetNVMGroupBlockSize(type);
        free_blocks_ = FILE_BASE_SIZE / block_size;
        bitmap_ = new BitMap(FILE_BASE_SIZE / block_size);
        last_allocate_ = 0;
    }
    ~NVMGroupManager() {
        delete bitmap_;
    }
    uint64_t GetId() { return id_; }

    int Allocate(uint64_t size){
        MutexLock lock(&mu_);
        int i = last_allocate_;
        uint64_t block_size = GetNVMGroupBlockSize(type_);
        uint64_t max = FILE_BASE_SIZE / block_size;
        uint64_t need = (size + block_size - 1) / block_size;
        uint64_t ok = 0;
        for(; i < max;){
            if(!bitmap_->get(i)) {
                ok = 1;
                while(ok < need && (i + ok) < max && !bitmap_->get(i + ok)){
                    ok++;
                }
                if(ok >= need){  //申请成功
                    last_allocate_ = i + ok;
                    for(int j = 0; j < need; j++){
                        bitmap_->set(i + j);
                    }
                    free_blocks_ -= need;
                    return i * block_size;
                }
                else if((i + ok) >= max){  //地址超过
                    break;
                }
                else{  //申请失败
                    i += ok;
                }
            }
            i++;
        }
        last_allocate_ = 0;
        i = last_allocate_;

        for(; i < max;){
            if(!bitmap_->get(i)) {
                ok = 1;
                while(ok < need && (i + ok) < max && !bitmap_->get(i + ok)){
                    ok++;
                }
                if(ok >= need){  //申请成功
                    last_allocate_ = i + ok;
                    for(uint64_t j = 0; j < need; j++){
                        bitmap_->set(i + j);
                    }
                    free_blocks_ -= need;
                    return i * block_size;
                }
                else if((i + ok) >= max){  //地址超过
                    break;
                }
                else{  //申请失败
                    i += ok;
                }
            }
            i++;
        }
        return -1;  //申请失败
    }

    void Free(uint64_t offset, uint64_t size){
        MutexLock lock(&mu_);
        uint64_t block_size = GetNVMGroupBlockSize(type_);
        uint64_t index = offset / block_size;  //offset一定是block_size的倍数
        uint64_t num = (size + block_size - 1) / block_size;
        for(uint64_t i = 0; i < num; i++){
            bitmap_->clr(index + i);
        }
        free_blocks_ += num;
    }

    uint64_t FreeSpace() {
        uint64_t block_size = GetNVMGroupBlockSize(type_);
        return free_blocks_ * block_size;
    }
    
private:
    uint64_t id_;  //在nvm中的index；
    NVMGroupBlockType type_;
    uint64_t free_blocks_;
    //uint64_t capacity_;
    //uint64_t max_block_;   //划分为block后的最大block num
    //uint64_t block_size_;
    Mutex mu_;     //操作的锁
    BitMap *bitmap_;  //group内部的位图
    uint64_t last_allocate_;
};

class NVMFileAllocator {
public:
    NVMFileAllocator(const std::string path, uint64_t size);
    ~NVMFileAllocator();

    void *Allocate(uint64_t size);
    void *AllocateAndInit(uint64_t size, int c);

    void Free(void *addr, uint64_t len);
    void Free(pointer_t addr, uint64_t len);
    char *GetPmemAddr() { return pmemaddr_; }

    //统计
    void PrintFileAllocatorStats(string &stats);

    void Sync(){
        if (is_pmem_)
            pmem_persist(pmemaddr_, capacity_);
        else
            pmem_msync(pmemaddr_, capacity_);
    }

    inline void nvm_persist(void *pmemdest, size_t len){
        if (is_pmem_)
            pmem_persist(pmemdest, len);
        else
            pmem_msync(pmemdest, len);
    }

    inline void nvm_memmove_persist(void *pmemdest, const void *src, size_t len){
        if(is_pmem_){
            pmem_memmove_persist(pmemdest, src, len);
        }
        else{
            memmove(pmemdest, src, len);
            pmem_msync(pmemdest, len);
        }
    }

    inline void nvm_memcpy_persist(void *pmemdest, const void *src, size_t len){
        if(is_pmem_){
            pmem_memcpy_persist(pmemdest, src, len);
        }
        else{
            memcpy(pmemdest, src, len);
            pmem_msync(pmemdest, len);
        }
    }

    inline void nvm_memset_persist(void *pmemdest, int c, size_t len){
        if(is_pmem_){
            pmem_memset_persist(pmemdest, c, len);
        }
        else{
            memset(pmemdest, c, len);
            pmem_msync(pmemdest, len);
        }
    }

    inline void nvm_memmove_nodrain(void *pmemdest, const void *src, size_t len){
        if(is_pmem_){
            pmem_memmove_nodrain(pmemdest, src, len);
        }
        else{
            memmove(pmemdest, src, len);
        }
    }

    inline void nvm_memcpy_nodrain(void *pmemdest, const void *src, size_t len){
        if(is_pmem_){
            pmem_memcpy_nodrain(pmemdest, src, len);
        }
        else{
            memcpy(pmemdest, src, len);
        }
    }

    inline void nvm_memset_nodrain(void *pmemdest, int c, size_t len){
        if(is_pmem_){
            pmem_memset_nodrain(pmemdest, c, len);
        }
        else{
            memset(pmemdest, c, len);
        }
    }    

private:
    char* pmemaddr_;
    uint64_t mapped_len_;
    uint64_t capacity_;
    int is_pmem_;
    Mutex bitmap_mu_;     //bitmap_的锁
    BitMap *bitmap_;  //划分为64MB后的group位图
    uint64_t last_allocate_;

    NVMGroupManager * groups_[MAX_GROUP_BLOCK_TYPE];   //groups_不用vector，正在分配的NVMGroupManager
    Mutex groups_mu_[MAX_GROUP_BLOCK_TYPE];  //对groups_[i]进行操作的锁，

    //暂时不加map，因为删除group时还是要在vector中删除，要么vector变成只需要一个正在分配的值，要么去掉map
    map<uint64_t, NVMGroupManager *> map_groups_;   //防止groups_过长，导致查询效率低，每个groups都有一个id，id = offset / FILE_BASE_SIZE
    Mutex map_mu_; // map_groups_的锁

    //统计
    atomic<uint64_t> allocate_size;
    atomic<uint64_t> free_size;


    uint64_t GetFreeIndex();
    void SetFreeIndex(uint64_t index);
    NVMGroupBlockType SelectGroup(uint64_t size);
    NVMGroupManager *CreateNVMGroupManager(NVMGroupBlockType type);

};

extern int InitNVMFileAllocator(const std::string path, uint64_t size);
static inline uint64_t GetId(pointer_t addr);      //以FILE_BASE_SIZE划分的id
static inline uint64_t GetOffset(pointer_t addr);  //以FILE_BASE_SIZE划分的offset
static inline uint64_t GetId(void *addr);
static inline uint64_t GetOffset(void * addr);


} // namespace name








#endif