/**
 * @Author      : Liu Zhiwen
 * @Create Date : 2020-12-08 20:16:16
 * @Contact     : 993096281@qq.com
 * @Description : 
 */
#ifndef _METADB_OPTION_H_
#define _METADB_OPTION_H_

#include <stdio.h>
#include <stdint.h>
#include <string>

using namespace std;
namespace metadb {

class Option {
public:
    uint64_t DIR_FIRST_HASH_MAX_CAPACITY = 4096;   //dir 存在的一级hash的容量
    uint64_t DIR_LINKNODE_TRAN_SECOND_HASH_NUM = 16;   //dir中linknode 个数超过该值，转成二级hash
    uint64_t DIR_SECOND_HASH_INIT_SIZE = 16;   //dir中转成二级hash初始大小
    double DIR_SECOND_HASH_TRIG_REHASH_TIMES = 1.5;  //dir中二级hash节点个数达到多少倍进行扩展

    uint64_t INODE_MAX_ZONE_NUM = 1024;   //inode 存储的一级hash最大数，inode id通过hash到一个一个zone里面
    uint64_t INODE_HASHTABLE_INIT_SIZE = 64;  //inode存储的hashtable的初始大小
    double INODE_HASHTABLE_TRIG_REHASH_TIMES = 1.5;  //inode存储的hashtable的node num 已经是capacity的INODE_HASHTABLE_TRIG_REHASH_TIMES倍，触发rehash
    
    string node_allocator_path = "/pmem0/test/node.pool";
    uint64_t node_allocator_size = 2ULL * 1024 * 1024 * 1024;   //GB
    string file_allocator_path = "/pmem0/test/file.pool";
    uint64_t file_allocator_size = 2ULL * 1024 * 1024 * 1024;   //GB
    uint32_t thread_pool_count = 2;

    Option() {}
    ~Option() {}

    void Print() const {
        fprintf(stdout, "---------------Option---------------\n");

        fprintf(stdout, "DIR_FIRST_HASH_MAX_CAPACITY:%lu DIR_LINKNODE_TRAN_SECOND_HASH_NUM:%lu \n",  \
            DIR_FIRST_HASH_MAX_CAPACITY, DIR_LINKNODE_TRAN_SECOND_HASH_NUM);
        fprintf(stdout, "DIR_SECOND_HASH_INIT_SIZE:%lu DIR_SECOND_HASH_TRIG_REHASH_TIMES:%lf \n",  \
            DIR_SECOND_HASH_INIT_SIZE, DIR_SECOND_HASH_TRIG_REHASH_TIMES);
        fprintf(stdout, "INODE_MAX_ZONE_NUM:%lu INODE_HASHTABLE_INIT_SIZE:%lu INODE_HASHTABLE_TRIG_REHASH_TIMES:%lf\n",  \
            INODE_MAX_ZONE_NUM, INODE_HASHTABLE_INIT_SIZE, INODE_HASHTABLE_TRIG_REHASH_TIMES);
        fprintf(stdout, "node_allocator_path:%s node_allocator_size:%lu MB\n",  \
            node_allocator_path.c_str(), node_allocator_size / (1024 * 1024));
        fprintf(stdout, "file_allocator_path:%s file_allocator_size:%lu MB\n",  \
            file_allocator_path.c_str(), file_allocator_size / (1024 * 1024));
        fprintf(stdout, "thread_pool_count:%u \n", thread_pool_count);

        fprintf(stdout, "------------------------------------\n");
        fflush(stdout);
    }
    
};

}








#endif