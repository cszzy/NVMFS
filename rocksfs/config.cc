#include "config.h"
#include <stdlib.h>
#include <assert.h>
#include <iostream>
#include <unistd.h>
#include <fstream>
#include "inode_format.h"

namespace rocksfs{
const std::string KVFSConfig :: config_file_name = "config.dat";


KVFSConfig::KVFSConfig():
    threshold(4096),current_inode_size(0)
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
