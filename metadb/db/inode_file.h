/**
 * @Author      : Liu Zhiwen
 * @Create Date : 2021-01-05 19:22:14
 * @Contact     : 993096281@qq.com
 * @Description : 
 */
#ifndef _METADB_INODE_FILE_H_
#define _METADB_INODE_FILE_H_

#include <stdint.h>

#include "metadb/inode.h"
#include "metadb/slice.h"
#include "format.h"
#include "nvm_file_allocator.h"

namespace metadb {

static const uint32_t NVM_INODE_FILE_CAPACITY = INODE_FILE_SIZE - 16;
static const uint32_t NVM_INODE_FILE_HEADER_SIZE = 24;  //头部大小

class NVMInodeFile{
public:
    uint64_t num;   //有效kv个数
    uint64_t write_offset;  //写指针，偏移
    uint64_t invalid_num;   //无效kv个数
    char buf[NVM_INODE_FILE_CAPACITY];

    
    NVMInodeFile() {}
    ~NVMInodeFile() {}
    uint64_t GetFreeSpace(){
        return NVM_INODE_FILE_CAPACITY - write_offset;
    }

    int InsertKV(const inode_id_t key, const Slice &value, pointer_t &addr){
        uint64_t len = sizeof(inode_id_t) + 4 + value.size();
        if(GetFreeSpace() < len) return -1;
        uint32_t value_len = value.size();
        char *buffer = new char[len];
        memcpy(buffer, &key, sizeof(inode_id_t));
        memcpy(buffer + sizeof(inode_id_t), &value_len, 4);
        memcpy(buffer + sizeof(inode_id_t) + 4, value.data(), value_len);
        SetBufPersist(write_offset, buffer, len);
        addr = FILE_GET_OFFSET(buf + write_offset);   //addr 是相对file_allocator的相对地址
        SetNumAndWriteOffsetPersist(num + 1, write_offset + len);
        return 0;
    }

    int GetKV(uint64_t offset, std::string &value){  //offset是以该类为起始地址的偏移
        uint64_t buf_offset = offset - NVM_INODE_FILE_HEADER_SIZE;
        uint32_t value_len = *reinterpret_cast<uint32_t *>(buf + buf_offset + sizeof(inode_id_t));
        value.assign(buf + buf_offset + sizeof(inode_id_t) + 4, value_len);
        return 0;
    }

    void SetNumAndWriteOffsetPersist(uint64_t new_num, uint64_t new_write_offset){
        char buffer[16];
        memcpy(buffer, &new_num, 8);
        memcpy(buffer + 8, &new_write_offset, 8);
        file_allocator->nvm_memcpy_persist(&num, buffer, 16);
    }

    void SetInvalidNumPersist(uint64_t new_invalid_num){
        file_allocator->nvm_memcpy_persist(&invalid_num, &new_invalid_num, 8);
    }

    void SetBufPersist(uint32_t offset, const void *ptr, uint32_t len){
        file_allocator->nvm_memmove_persist(buf + offset, ptr, len);
    }
    void SetBufNodrain(uint32_t offset, const void *ptr, uint32_t len){
        file_allocator->nvm_memmove_nodrain(buf + offset, ptr, len);
    }

};

extern NVMInodeFile *AllocNVMInodeFlie();

static inline uint64_t GetFileId(pointer_t addr){   //以INODE_FILE_SIZE划分的id
    return addr / INODE_FILE_SIZE;
}

static inline uint64_t GetFileOffset(pointer_t addr){   //以INODE_FILE_SIZE划分的offset
    return addr % INODE_FILE_SIZE;
}

} // namespace name








#endif