#include "inode_format.h"
#include <iostream>


namespace metafs{

//uint64_t kvfs_inode_meta_key ::current_index = 0;


std::unordered_map <inode_id_t ,kvfs_file_handle *> kvfs_file_handle :: handle_map = 
    std::unordered_map <inode_id_t ,kvfs_file_handle *>();

kvfs_file_handle * kvfs_file_handle::GetHandle(const inode_id_t key)
{
    if(handle_map .find(key) != handle_map.end())
        return handle_map.at(key);
    else return nullptr;
}
bool kvfs_file_handle :: InsertHandle(const inode_id_t key , kvfs_file_handle * handle)
{
    if(handle_map.find(key) != handle_map.end())
    {
        return false;
    }
    else 
    {
        handle_map[key] = handle;
        return true;
    }
}
//zzy:在删除handle时释放array object
bool kvfs_file_handle :: DeleteHandle(const inode_id_t key)
{
    auto it = handle_map .find(key) ;
    if(it!= handle_map.end())
    {
        // int rc = daos_array_close(it->second->oh, NULL);
        // assert(rc == 0);
        delete it->second;
        handle_map.erase(it);
        return true;
    }
    else return false;

}

uint64_t murmur64( const void * key, int len, uint64_t seed )
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


#ifdef KVFSDEBUG
KVFSLogger * KVFSLogger :: instance_ = nullptr;

KVFSLogger * KVFSLogger :: GetInstance()
{
    if(instance_ == nullptr)
    {
        instance_ = new KVFSLogger();
    }
    return instance_;
}


void KVFSLogger :: Log(const char * file_name ,int line, const char * data,  ...)
{
    va_list ap;
    va_start(ap,data);
    //std::cout << data << std::endl;
    fprintf(fp,"[%s : %d]- ",file_name ,line);
    vfprintf(fp,data,ap);
    int len = strlen(data);
    if(data[len-1] != '\n')
        fprintf(fp,"\n");
    fflush(fp);
}
#endif // NDEBUG

}

