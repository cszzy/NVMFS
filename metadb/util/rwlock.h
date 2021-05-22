/**
 * @Author      : Liu Zhiwen
 * @Create Date : 2020-12-14 14:37:24
 * @Contact     : 993096281@qq.com
 * @Description : 
 */
#ifndef _RWLOCK_H_
#define _RWLOCK_H_

#include <pthread.h>

namespace metadb {

class RWLock{
public:
    RWLock(){
        pthread_rwlock_init(&rw_, NULL);
    };

    ~RWLock(){
        pthread_rwlock_destroy(&rw_);
    };

    void ReadLock(){
        pthread_rwlock_rdlock(&rw_);
    };

    void WriteLock(){
        pthread_rwlock_wrlock(&rw_);
    };

    void Unlock(){
        pthread_rwlock_unlock(&rw_);
    };

private:
    pthread_rwlock_t rw_;

}; 


} // namespace name







#endif