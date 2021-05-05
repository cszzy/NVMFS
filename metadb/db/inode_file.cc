/**
 * @Author      : Liu Zhiwen
 * @Create Date : 2021-01-25 14:34:46
 * @Contact     : 993096281@qq.com
 * @Description : 
 */

#include "inode_file.h"

namespace metadb {

NVMInodeFile *AllocNVMInodeFlie(){
    NVMInodeFile *file = static_cast<NVMInodeFile *>(file_allocator->AllocateAndInit(INODE_FILE_SIZE, 0));
    return file;
}

} // namespace name