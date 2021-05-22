/**
 * @Author      : Liu Zhiwen
 * @Create Date : 2020-12-17 15:57:49
 * @Contact     : 993096281@qq.com
 * @Description : 
 */
#ifndef _METADB_FORMAT_H_
#define _METADB_FORMAT_H_

#include <stdint.h>

#define NODE_BASE_SIZE 256    //node节点基础大小，以该值为块，必须为2的幂次，2^N
#define FILE_BASE_SIZE (64ULL * 1024 * 1024)   //file以64MB为一个group组，里面再划分不同的块
#define START_ALLOCATOR_INDEX 1  //从1开始分配，0为无效地址。

#define INVALID_POINTER 0

//最高位为1表示是二级hash地址，其余63位为offset
#define SECOND_HASH_POINTER  (1ULL << 63)

#define IS_INVALID_POINTER(offset) (offset == INVALID_POINTER)
#define IS_SECOND_HASH_POINTER(offset) (offset >> 63)

#define DIR_LINK_NODE_SIZE  512
#define DIR_BPTREE_INDEX_NODE_SIZE  256
#define DIR_BPTREE_LEAF_NODE_SIZE  1024

#define MAX_DIR_BPTREE_LEVEL 8

////
#define INODE_HASH_ENTRY_SIZE  256
#define INODE_FILE_SIZE (4ULL * 1024 * 1024)   //MB
////


namespace metadb {

typedef uint64_t pointer_t;   //在NVM存的指针地址都是偏移，避免每次mmap到不同地址无法恢复





} // namespace name








#endif