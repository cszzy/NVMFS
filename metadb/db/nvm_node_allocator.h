/**
 * @Author      : Liu Zhiwen
 * @Create Date : 2020-12-14 14:10:55
 * @Contact     : 993096281@qq.com
 * @Description : 
 */
#ifndef _METADB_NVM_NODE_ALLOCATOR_H_
#define _METADB_NVM_NODE_ALLOCATOR_H_

#include <stdint.h>
#include <string>
#include <atomic>

#include "metadb/libnvm.h"
#include "../util/lock.h"
#include "../util/bitmap.h"
#include "format.h"

#define NODE_GET_OFFSET(dst) (reinterpret_cast<char *>(dst) - metadb::node_pool_pointer)     //void *-> offset
#define NODE_GET_POINTER(offset) (static_cast<void *>(metadb::node_pool_pointer + offset))  //offset -> void *

using namespace std;
namespace metadb {

class NVMNodeAllocator;

extern char *node_pool_pointer;
extern NVMNodeAllocator *node_allocator;

class NVMNodeAllocator {
public:
    NVMNodeAllocator(const std::string path, uint64_t size);
    ~NVMNodeAllocator();

    void *Allocate(uint64_t size);
    void *AllocateAndInit(uint64_t size, int c);

    void Free(void *addr, uint64_t len);
    void Free(pointer_t addr, uint64_t len);
    char *GetPmemAddr() { return pmemaddr_; }

    //统计
    void PrintNodeAllocatorStats(string &stats);
    void PrintBitmap();

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
    Mutex mu_;
    BitMap *bitmap_;

    uint64_t last_allocate_;

    //统计
    atomic<uint64_t> allocate_size;
    atomic<uint64_t> free_size;

    uint64_t GetFreeIndex(uint64_t size);
    void SetFreeIndex(uint64_t offset, uint64_t len);

};

extern int InitNVMNodeAllocator(const std::string path, uint64_t size);


} // namespace name








#endif