/**
 * @Author      : Liu Zhiwen
 * @Create Date : 2020-12-09 15:06:00
 * @Contact     : 993096281@qq.com
 * @Description : 
 */
#ifndef _METADB_DEBUG_H_
#define _METADB_DEBUG_H_

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <pthread.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>

#define LOG_FILE_PATH "log.log"

namespace metadb {

#ifdef NDEBUG

#define DBG_LOG(format, a...)
#define DBG_PRINT(format, a...)

#else

static inline unsigned int get_tid(){
#ifdef __APPLE__
    return (unsigned int) pthread_self();
#else
    return (unsigned int) syscall(__NR_gettid);
#endif

}

class DebugLogger{
public:
    DebugLogger(const char *log_file_path) {
        fp = fopen(log_file_path,"w+");
        if(fp == nullptr){
            printf("can't create log file!");
            exit(-1);
        }
    }

    static inline DebugLogger* GetInstance(){
        if(log_ == nullptr)
        {
            log_ = new DebugLogger(LOG_FILE_PATH);
        }
        return log_;
    }

    inline FILE* GetFp() { return fp; }

protected:
    static DebugLogger* log_;
    FILE *fp;
    
};

#define DBG_LOG(format, a...) \
do{ \
    fprintf(metadb::DebugLogger::GetInstance()->GetFp(), "[%-18s][%4d][%5d]: " #format"\n", __FUNCTION__, __LINE__, get_tid(), ##a); \
    fflush(metadb::DebugLogger::GetInstance()->GetFp()); \
}while(0)

#define DBG_PRINT(format, a...) \
do{ \
    printf("[%-18s][%4d][%5d]: " #format"\n", __FUNCTION__, __LINE__, get_tid(), ##a); \
}while(0)

#endif

#define ERROR_PRINT(format, a...) \
do{ \
    printf("ERROR:[%s][%s][%d]: " #format"\n", __FILE__, __FUNCTION__, __LINE__, ##a); \
    DBG_LOG(format, ##a); \
}while(0)

}

#endif