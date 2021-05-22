/**
 * @Author      : Liu Zhiwen
 * @Create Date : 2020-12-14 14:46:01
 * @Contact     : 993096281@qq.com
 * @Description : 
 */
#ifndef _METADB_BITMAP_H_
#define _METADB_BITMAP_H_

#include <cstring>

namespace metadb {

class BitMap {

public:
    BitMap(){
        //default 10000
        gsize = (10000 >> 3) + 1;
        bitmap = new char[gsize];
        memset(bitmap, 0, gsize);
    };

    BitMap(int n){
        gsize = (n >> 3) + 1;
        bitmap = new char[gsize];
        memset(bitmap, 0, gsize);
    };

    ~BitMap(){
        delete[] bitmap;
    };

    int get(int x){
        int cur = x >> 3;
        int remainder = x & (7);
        if (cur > gsize) return -1;

        return (bitmap[cur] >> remainder) & 1;
    };

    int set(int x){
        int cur = x >> 3;
        int remainder = x & (7);
        if (cur > gsize) return 0;
        bitmap[cur] |= (1 << remainder);
        return 1;
    };

    int clr(int x){
        int cur = x >> 3;
        int remainder = x & (7);
        if (cur > gsize)return 0;
        bitmap[cur] &= (~(1 << remainder));
        return 1;
    };

    int reset(){
        memset(bitmap, 0, gsize);
        return 1;
    };

    int get_capacity() { return gsize; }
        
private:
    char *bitmap;
    int gsize;
}; 


} // namespace name








#endif