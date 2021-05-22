/**
 * @Author      : Liu Zhiwen
 * @Create Date : 2020-12-09 14:39:27
 * @Contact     : 993096281@qq.com
 * @Description : 
 */
#ifndef _METADB_INODE_H_
#define _METADB_INODE_H_

#include <stdint.h>
#define INVALID_INODE_ID_KEY  0
#define MAX_INODE_ID_KEY UINT64_MAX
#define MAX_FNAME_LEN 180

namespace metadb {

typedef uint64_t inode_id_t;

static inline int compare_inode_id(inode_id_t a, inode_id_t b) {
    if(a < b) {
        return -1;
    }
    else if( a > b) {
        return 1;
    }
    else {
        return 0;
    }
}

static inline uint64_t MurmurHash64(const void * key, int len, uint64_t seed = 123)
{
    const uint64_t m = 0xc6a4a7935bd1e995;
    const int r = 47;

    uint64_t h = seed ^ (len * m);

    const uint64_t * data = (const uint64_t *)key;
    const uint64_t * end = data + (len/8);

    while(data != end)
    {
        uint64_t k = *data++;

        k *= m; 
        k ^= k >> r; 
        k *= m; 
        
        h ^= k;
        h *= m; 
    }

    const unsigned char * data2 = (const unsigned char*)data;

    switch(len & 7)
    {
    case 7: h ^= uint64_t(data2[6]) << 48;
    case 6: h ^= uint64_t(data2[5]) << 40;
    case 5: h ^= uint64_t(data2[4]) << 32;
    case 4: h ^= uint64_t(data2[3]) << 24;
    case 3: h ^= uint64_t(data2[2]) << 16;
    case 2: h ^= uint64_t(data2[1]) << 8;
    case 1: h ^= uint64_t(data2[0]);
            h *= m;
    };
    
    h ^= h >> r;
    h *= m;
    h ^= h >> r;

    return h;
} 




}









#endif