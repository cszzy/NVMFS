#include "config.h"
#include <stdlib.h>
#include <assert.h>
#include <iostream>
#include <unistd.h>
#include <fstream>
#include "inode_format.h"

#include "daos.h"
static daos_size_t static_chunk_size = 16;
namespace metafs{
const std::string KVFSConfig :: config_file_name = "config.dat";


KVFSConfig::KVFSConfig():
    threshold(4096),current_inode_size(0)
{
}

//zzy:todo是否销毁pool和cont
KVFSConfig::~KVFSConfig()
{

}

void KVFSConfig::Init(const kvfs_args & args)
{
    KVFS_LOG("KVFSConfig : Init config .. \n ");
    assert(args.find("meta_dir") != args.end());
    assert(args.find("data_dir") != args.end());
    assert(args.find("mount_dir") != args.end());
    assert(args.find("pool") != args.end());
    assert(args.find("container") != args.end());

    // uuid_t pool_uuid, cont_uuid;
    KVFS_LOG("KVFSConfig : Parse pool uuid ..  ");
    int rc = uuid_parse(args.at("pool").c_str(), this->pool_uuid);
    if(rc != 0){
        KVFS_LOG("error");
    }

    KVFS_LOG("KVFSConfig : Parse container uuid ..  ");
    rc = uuid_parse(args.at("container").c_str(), this->cont_uuid);
    if(rc != 0){
        KVFS_LOG("error");
    }

    // KVFS_LOG("KVFSConfig : Connect to pool ..  ");
    // rc = daos_pool_connect(pool_uuid, NULL, DAOS_PC_RW, &this->poh, &this->pool_info, NULL);
    // if(rc != 0){
    //     KVFS_LOG("error");
    // }

    // KVFS_LOG("KVFSConfig : Connect to container ..  ");
    // rc = daos_cont_open(this->poh, cont_uuid, DAOS_COO_RW, &this->coh, &this->cont_info, NULL);
    // if(rc != 0){
    //     KVFS_LOG("error");
    // }

    // daos_obj_id_t oid;
    // daos_handle_t oh;
    // daos_array_iod_t iod;
	// d_sg_list_t	sgl;
	// daos_range_t	rg;
	// d_iov_t		iov;
	// char		buf[80], rbuf[80];
	// daos_size_t	array_size;
    // memset(buf, 'A', 80);
    // /** set array location */
	// iod.arr_nr = 1;
	// rg.rg_len = 80;
	// rg.rg_idx = 0;
	// iod.arr_rgs = &rg;

    // /** set memory location */
	// sgl.sg_nr = 1;
	// d_iov_set(&iov, buf, 80);
	// sgl.sg_iovs = &iov;

    // this->GetDaosObjId(&oid, 200);
    // // rc = daos_obj_open(config_->coh, oid, DAOS_OO_RO, &oh, NULL);
    // // KVFS_LOG("daos_array_open_with_attr");
    // // rc = daos_array_open_with_attr(this->coh, oid, DAOS_TX_NONE, DAOS_OO_RW, 4, static_chunk_size, &oh, NULL);
    // KVFS_LOG("daos_array_create");
    // rc = daos_array_create(this->coh, oid, DAOS_TX_NONE, 1, FS_DEFAULT_CHUNK_SIZE, &oh, NULL);
    // if(rc != 0){
    //     KVFS_LOG("error,rc:%d", rc);
    // }
    // /** Write */
    // KVFS_LOG("daos_array_write");
	// rc = daos_array_write(oh, DAOS_TX_NONE, &iod, &sgl, NULL);
	// if(rc != 0){
    //     KVFS_LOG("error,rc:%d", rc);
    // }

    char ans[4096];
    
    if(nullptr != realpath(( args.at("meta_dir")).c_str(),ans))
    {
        KVFS_LOG("realpath of metadir : %s\n",ans);
        this->meta_dir = ans;
    }
    else 
    {
        KVFS_LOG(strerror(errno));
    }

    if(nullptr != realpath( args.at("data_dir").c_str(),ans))
    {
        KVFS_LOG("realpath of datadir : %s\n",ans);
        this->data_dir = ans;
    }
    else 
    {
        KVFS_LOG(strerror(errno));
    }

    if(nullptr != realpath( args.at("mount_dir").c_str(),ans))
    {
        KVFS_LOG("realpath of mount_dir : %s\n",ans);
        this->mount_dir = ans;
    }
    else 
    {
        KVFS_LOG(strerror(errno));
    }
    if(access(data_dir.c_str(),W_OK) > 0 
        || access(meta_dir.c_str(),W_OK) > 0)
    {
        KVFS_LOG("cant open directory ! ");
        std::cerr << "cannot open directory !" <<std::endl;
        exit(1);
    }
    std::fstream fs;
    fs.open(data_dir +"/" + config_file_name, std::ios::in);
    // cant find config file
    if(!fs.is_open())
    {
        KVFS_LOG("cant find config file ..create a new one..");
        // write open
        fs.open(data_dir +"/" + config_file_name, std::ios::out);
        current_inode_size = 0 ;
        fs << current_inode_size ;
        fs.close();
        // TODO: how big data place ?
    }
    else  //find config file
    {
        KVFS_LOG("find a config file ...");
        fs >> current_inode_size ;
        fs.close();
    }
}




}