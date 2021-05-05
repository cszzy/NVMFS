/**
 * @Author      : Liu Zhiwen
 * @Create Date : 2021-01-07 14:08:37
 * @Contact     : 993096281@qq.com
 * @Description : 
 */

#include "thread_pool.h"

namespace metadb {

ThreadPool *thread_pool = nullptr;

int InitThreadPool(uint32_t count){
    thread_pool = new ThreadPool(count);
    return 0;
}


} // namespace name