#ifndef _ROCKSFS_CONFIG_H_
#define _ROCKSFS_CONFIG_H_

#include <stdint.h>
#include <string>
#include <unordered_map>
#include <sys/stat.h>
#include <daos.h>

namespace rocksfs{
#define DFS_OBJ_CLASS OC_SX
#define FS_DEFAULT_CHUNK_SIZE (1<<20) //1MB
using kvfs_args = std::unordered_map <std::string, std::string >;

class KVFSConfig {

public :
    KVFSConfig();
    void Init(const kvfs_args & args);
    inline bool IsEmpty() { 
        return 0 == current_inode_size;  }
    inline std::string GetDataDir()
    {
        return data_dir;
    }
    inline std::string GetMetaDir()
    {
        return meta_dir;
    }
    inline std::string GetMountDir()
    {
        return mount_dir;
    }
    inline uint64_t GetThreshold() 
    {
        return threshold;
    }
    inline uint64_t NewInode(){
        ++current_inode_size;
        return current_inode_size;
    }
    inline void NewInode(uint64_t &key){
        ++current_inode_size;
        key = current_inode_size;
    }

    inline void GetDaosObjId(daos_obj_id_t* oid, uint64_t inode_id) {
        /* generate a unique and not scary long object ID */
        oid->lo = inode_id;
        oid->hi = 0;
        daos_obj_generate_id(oid, DAOS_OF_DKEY_UINT64 | DAOS_OF_KV_FLAT | DAOS_OF_ARRAY, OC_SX, 0);
    }

protected:

    // const path
    static const std::string config_file_name;

    // dirs 
    std::string meta_dir;
    std::string data_dir;
    std::string mount_dir;
    uint64_t current_inode_size;

    uint64_t threshold;

public:
    //daos
    uuid_t pool_uuid;
    uuid_t cont_uuid;
    daos_handle_t poh; //pool oh
    daos_handle_t coh; //container oh
    daos_pool_info_t pool_info;
    daos_cont_info_t cont_info;



};

}

#endif // KVFS_CONFIG_HPP
