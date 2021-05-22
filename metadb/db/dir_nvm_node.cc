/**
 * @Author      : Liu Zhiwen
 * @Create Date : 2020-12-21 20:18:14
 * @Contact     : 993096281@qq.com
 * @Description : 
 */

#include <queue>

#include "metadb/debug.h"
#include "dir_nvm_node.h"
#include "dir_iterator.h"

using namespace std;
namespace metadb {

LinkNode *AllocLinkNode(){
    LinkNode *node = static_cast<LinkNode *>(node_allocator->AllocateAndInit(DIR_LINK_NODE_SIZE, 0));
    node->SetTypeNodrain(DirNodeType::LINKNODE_TYPE);
    return node;
}

struct LinkNodeSearchResult {
    bool key_find;
    uint32_t key_index;
    uint32_t key_offset;
    uint32_t key_num;
    uint32_t key_len;
    bool value_is_bptree;  //value 在bptree中;
    bool fname_find;
    uint32_t fname_index;
    uint32_t fname_offset;
    uint32_t value_len;

    LinkNodeSearchResult() : key_find(false), key_index(0), key_offset(0), key_num(0), key_len(0),
        value_is_bptree(false), fname_find(false), fname_index(0), fname_offset(0), value_len(0) {}
    ~LinkNodeSearchResult() {}
};



int LinkNodeSearch(LinkNodeSearchResult &res, LinkNode *cur, const inode_id_t key, const Slice &fname){
    if(cur->num == 0 || cur->len == 0){
        res.key_index = 0;
        res.key_offset = 0;
        return 1;
    }
    if(compare_inode_id(key, cur->min_key) < 0) {
        res.key_index = 0;
        res.key_offset = 0;
        return 1;
    }
    if(compare_inode_id(key, cur->max_key) > 0) {
        res.key_index = cur->num;
        res.key_offset = cur->len;
        return 1;
    }

    inode_id_t temp_key;
    uint32_t kv_len;
    uint32_t key_num, key_len;
    uint32_t offset = 0;
    uint32_t i = 0;
    for(; i < cur->num; i++){
        cur->DecodeBufGetKeyNumLen(offset, temp_key, key_num, key_len);
        if(compare_inode_id(key, temp_key) == 0) {  //找到key
            res.key_find = true;
            res.key_index = i;
            res.key_offset = offset;
            res.key_num = key_num;
            res.key_len = key_len;
            if(key_num == 0){ //value 是bptree
                res.value_is_bptree = true;
            } else {
                uint64_t hash_fname = MurmurHash64(fname.data(), fname.size());
                uint64_t get_hash_fname;
                uint32_t value_len;
                uint32_t value_offset = offset + sizeof(inode_id_t) + 4 + 4;
                uint32_t j = 0;
                for(; j < key_num; j++) {
                    cur->DecodeBufGetHashfnameAndLen(value_offset, get_hash_fname, value_len);
                    if(get_hash_fname == hash_fname){  //找到fname
                        res.fname_find = true;
                        res.fname_index = j;
                        res.fname_offset = value_offset;
                        res.value_len = value_len;

                        return 0;
                    }
                    if(get_hash_fname > hash_fname) {
                        res.fname_index = j;
                        res.fname_offset = value_offset;
                        res.value_len = value_len;
                        return 0;
                    }
                    value_offset += (8 + 4 + value_len);
                }
                res.fname_index = j;
                res.fname_offset = value_offset;
                
            }

            return 0;
        }

        if(compare_inode_id(key, temp_key) < 0) {  //未找到
            res.key_index = i;
            res.key_offset = offset;
            return 1;
        }
        if(key_num == 0){
            kv_len = sizeof(inode_id_t) + 4 + 8;
        } else {
            kv_len = sizeof(inode_id_t) + 4 + 4 + key_len;
        }

        offset += kv_len;
    }
    res.key_index = i;
    res.key_offset = offset;
    return 1;
}

void MemoryEncodeKey(char *buf, inode_id_t key){
    memcpy(buf, &key, sizeof(inode_id_t));
}

void MemoryEncodeKVnumKVlen(char *buf, uint32_t key_num, uint32_t key_len){
    uint32_t insert_offset = 0;
    memcpy(buf + insert_offset, &key_num, 4);
    insert_offset += 4;
    memcpy(buf + insert_offset, &key_len, 4);
}

void MemoryEncodeFnameKey(char *buf, const Slice &fname, const inode_id_t value){
    uint64_t hash_fname = MurmurHash64(fname.data(), fname.size());
    uint32_t value_len = fname.size() + sizeof(inode_id_t);
    uint32_t insert_offset = 0;
    memcpy(buf + insert_offset, &hash_fname, 8);
    insert_offset += 8;
    memcpy(buf + insert_offset, &value_len, 4);
    insert_offset += 4;
    memcpy(buf + insert_offset, fname.data(), fname.size());
    insert_offset += fname.size();
    memcpy(buf + insert_offset, &value, sizeof(inode_id_t));   
}

void MemoryEncodeKeyBptreePointer(char * buf, inode_id_t key, pointer_t bptree){
    uint32_t num = 0;
    memcpy(buf, &key, sizeof(inode_id_t));
    memcpy(buf + sizeof(inode_id_t), &num, 4);
    memcpy(buf + sizeof(inode_id_t) + 4, &bptree, sizeof(pointer_t));
}

void MemoryEncodeHashkeyLenFnameValue(char *buf, const uint64_t hash_key, const Slice &fname, const inode_id_t value){
    uint32_t offset = 0;
    uint32_t value_len = fname.size() + sizeof(inode_id_t);
    memcpy(buf + offset, &hash_key, 8);
    offset += 8;
    memcpy(buf + offset, &value_len, 4);
    offset += 4;
    memcpy(buf + offset, fname.data(), fname.size());
    offset += fname.size();
    memcpy(buf + offset, &value, sizeof(inode_id_t));
}

void MemoryDecodeGetKeyNumLen(const char *buf, inode_id_t &key, uint32_t &key_num, uint32_t &key_len){
    key = *reinterpret_cast<const inode_id_t *>(buf);
    key_num = *reinterpret_cast<const uint32_t *>(buf + sizeof(inode_id_t));
    key_len = *reinterpret_cast<const uint32_t *>(buf + sizeof(inode_id_t) + 4);
}

inode_id_t MemoryDecodeGetKey(const char *buf){
    return (*reinterpret_cast<const inode_id_t *>(buf));
}

void MemoryDecodeHashkeyValuelen(const char *buf, uint64_t &hash_key, uint32_t &value_len){
    hash_key = *reinterpret_cast<const uint64_t *>(buf);
    value_len = *reinterpret_cast<const uint32_t *>(buf + 8);
}

void LinkNodeUpdateNextPrev(LinkNode *new_node){
    if(!IS_INVALID_POINTER(new_node->next)) {
        LinkNode *next_node = static_cast<LinkNode *>(NODE_GET_POINTER(new_node->next));
        next_node->SetPrevPersist(NODE_GET_OFFSET(new_node));
    }

    if(!IS_INVALID_POINTER(new_node->prev)){
        LinkNode *prev_node = static_cast<LinkNode *>(NODE_GET_POINTER(new_node->prev));
        prev_node->SetNextPersist(NODE_GET_OFFSET(new_node));
    }
}

int LinkNodeSplit(const char *buf, uint32_t len, vector<pointer_t> &res){   //有可能分裂为三个节点
    uint32_t offset = 0;
    uint32_t last_node_offset = 0;
    uint32_t new_node_len = 0;
    uint16_t new_node_key_num = 0;
    inode_id_t key;
    uint32_t key_num, key_len;
    uint32_t kv_len;
    inode_id_t min_key, prev_key;
    pointer_t prev = INVALID_POINTER;
    uint32_t split_size = len / 2; //分为两半的size；
    while(offset < len) {
        MemoryDecodeGetKeyNumLen(buf + offset, key, key_num, key_len);
        if(new_node_len == 0) min_key = key;
        if(key_num == 0){
            kv_len = sizeof(inode_id_t) + 4 + 8;
        } else {
            kv_len = sizeof(inode_id_t) + 8 + key_len;
        }

        if((new_node_len + kv_len)  > LINK_NODE_CAPACITY){  //该节点不可再增加内容，前面的内容生成一个节点
            LinkNode *new_node = AllocLinkNode();
            new_node->SetNumAndLenNodrain(new_node_key_num, new_node_len);
            new_node->SetMinkeyNodrain(min_key);
            new_node->SetMaxkeyNodrain(prev_key);
            new_node->SetPrevNodrain(prev);
            new_node->SetBufNodrain(0, buf + last_node_offset, new_node_len);
            last_node_offset = offset;
            split_size = (len - last_node_offset) > LINK_NODE_CAPACITY ? (len - last_node_offset) / 2 : len - last_node_offset;  //剩下的长度大于一个节点，再次分割，否则直接剩下的内容组成一个节点
            new_node_key_num = 0;
            new_node_len = 0;
            min_key = key;
            prev = NODE_GET_OFFSET(new_node);
            res.push_back(prev);
            
        }
        offset += kv_len;
        prev_key = key;
        new_node_len += kv_len;
        new_node_key_num++;
        if(new_node_len >= split_size || offset == len) { //长度超过剩余内容一半，或者到达末尾，可生成一个节点
            LinkNode *new_node = AllocLinkNode();
            new_node->SetNumAndLenNodrain(new_node_key_num, new_node_len);
            new_node->SetMinkeyNodrain(min_key);
            new_node->SetMaxkeyNodrain(key);
            new_node->SetPrevNodrain(prev);
            new_node->SetBufNodrain(0, buf + last_node_offset, new_node_len);
            last_node_offset = offset;
            split_size = (len - last_node_offset) > LINK_NODE_CAPACITY ? (len - last_node_offset) / 2 : len - last_node_offset;  //剩下的长度大于一个节点，再次分割，否则直接剩下的内容组成一个节点
            new_node_key_num = 0;
            new_node_len = 0;
            prev = NODE_GET_OFFSET(new_node);
            res.push_back(prev);
        }
    }
    assert(offset == len);

    for(uint32_t i = 0; i < res.size() - 1; i++){  //设置next指针
        LinkNode *cur = static_cast<LinkNode *>(NODE_GET_POINTER(res[i]));
        cur->SetNextNodrain(res[i+1]);
    }
    return 0;

}

//将cur节点的key转成B+树bptree后进行操作,buf是剩余内容，len是剩余长度
int LinkNodeTranBptreeDo(LinkListOp &op, LinkNodeSearchResult &res, LinkNode *cur, const char *buf, uint32_t len){
    //len不可能为0
    if(len < LINK_NODE_TRIG_MERGE_SIZE ){ //可能需要合并
        if(!IS_INVALID_POINTER(cur->next)){  
            LinkNode *next_node = static_cast<LinkNode *>(NODE_GET_POINTER(cur->next));
            if(next_node->GetFreeSpace() >= len){ //和后节点合并，空间足够组成一个节点
                LinkNode *new_node = AllocLinkNode();   //超过8B修改都采用COW，copy on write
                new_node->CopyBy(cur);
                new_node->SetBufNodrain(0, buf, len);
                new_node->SetBufNodrain(len, next_node->buf, next_node->len);
                new_node->SetNumAndLenNodrain(cur->num + next_node->num, len + next_node->len);
                new_node->SetMaxkeyNodrain(next_node->max_key);
                new_node->SetNextNodrain(next_node->next);
                new_node->Flush();

                LinkNodeUpdateNextPrev(new_node);
    
                if(IS_INVALID_POINTER(new_node->prev)){
                    op.res = NODE_GET_OFFSET(new_node);  //头结点替换
                }

                op.free_linknode_list.push_back(NODE_GET_OFFSET(cur));
                op.free_linknode_list.push_back(NODE_GET_OFFSET(next_node));
                op.add_linknode_list.push_back(NODE_GET_OFFSET(new_node));
                return 0;
                
            }
        }

        if(!IS_INVALID_POINTER(cur->prev)){
            LinkNode *prev_node = static_cast<LinkNode *>(NODE_GET_POINTER(cur->prev));
            if(prev_node->GetFreeSpace() >= len){ //和前节点合并，空间足够组成一个节点
                LinkNode *new_node = AllocLinkNode();   //超过8B修改都采用COW，copy on write
                new_node->CopyBy(prev_node);
                new_node->SetBufNodrain(prev_node->len, buf, len);

                new_node->SetNumAndLenNodrain(new_node->num + cur->num, new_node->len + len);
                new_node->SetMaxkeyNodrain(cur->max_key);
                new_node->SetNextNodrain(cur->next);
                new_node->Flush();

                LinkNodeUpdateNextPrev(new_node);
    
                if(IS_INVALID_POINTER(new_node->prev)){
                    op.res = NODE_GET_OFFSET(new_node);  //头结点替换
                }

                op.free_linknode_list.push_back(NODE_GET_OFFSET(cur));
                op.free_linknode_list.push_back(NODE_GET_OFFSET(prev_node));
                op.add_linknode_list.push_back(NODE_GET_OFFSET(new_node));
                return 0;
            }
        }

        //前后节点都不适合合并，保持单个节点；
    }

    //剩下内容组成单个节点

    LinkNode *new_node = AllocLinkNode();   //超过8B修改都采用COW，copy on write
    new_node->CopyBy(cur);
    new_node->SetBufNodrain(0, buf, len);
    new_node->SetNumAndLenNodrain(cur->num, len);
    
    new_node->Flush();
    
    LinkNodeUpdateNextPrev(new_node);

    if(IS_INVALID_POINTER(new_node->prev)){
        op.res = NODE_GET_OFFSET(new_node);  //头结点替换
    }

    op.free_linknode_list.push_back(NODE_GET_OFFSET(cur));
    op.add_linknode_list.push_back(NODE_GET_OFFSET(new_node));
    return 0;
}

int LinkNodeInsert(LinkListOp &op, LinkNode *cur, const inode_id_t key, const Slice &fname, const inode_id_t value){
    LinkNodeSearchResult res;
    LinkNodeSearch(res, cur, key, fname);
    if(res.key_find){
        if(res.value_is_bptree) { //bptree 插入
            pointer_t bptree = cur->DecodeBufGetBptree(res.key_offset + sizeof(inode_id_t) + 4);
            //bptree 插入
            BptreeOp bop;
            bop.root = bptree;
            bop.res = bptree;
            BptreeInsert(bop, MurmurHash64(fname.data(), fname.size()), fname, value);
            op.AddBptreeOp(bop);
            if(bop.root != bop.res){  //修改根节点
                DBG_LOG("[dir] key:%lu bptree modify new root:%lu old:%lu", key, bop.res, bop.root);
                cur->SetBufPersist(res.key_offset + 8 + 4, &bop.res, sizeof(pointer_t));
            }
            return 0;
        }
        if(res.fname_find) { //node直接修改
            assert(fname.size() == (res.value_len - sizeof(inode_id_t)));  //以防hash值不一样
            uint32_t offset = res.fname_offset + 8 + 4 + (res.value_len - sizeof(inode_id_t));
            cur->SetBufPersist(offset, &value, sizeof(inode_id_t));
            return 0;
        }
        //pinode存在，fname不存在情况
        uint32_t add_len = 8 + 4 + fname.size() + sizeof(inode_id_t);
        if(cur->GetFreeSpace() >= add_len) { //空间足够，可以插入
            LinkNode *new_node = AllocLinkNode();   //超过8B修改都采用COW，copy on write
            new_node->CopyBy(cur);
            uint32_t insert_offset = res.fname_offset;
            uint32_t need_move = cur->len - insert_offset;
            if(need_move > 0) {
                new_node->SetBufNodrain(insert_offset + add_len, cur->buf + insert_offset, need_move);
            }
            char *buf = new char[add_len];
            MemoryEncodeFnameKey(buf, fname, value);
            new_node->SetBufNodrain(insert_offset, buf, add_len); 
            
            insert_offset = res.key_offset + 8;
            MemoryEncodeKVnumKVlen(buf, res.key_num + 1, res.key_len + add_len);
            new_node->SetBufNodrain(insert_offset, buf, 8);
            
            new_node->SetNumAndLenNodrain(cur->num, cur->len + add_len);
            new_node->Flush();
            
            LinkNodeUpdateNextPrev(new_node);
            if(IS_INVALID_POINTER(new_node->prev)){  //更新根节点
                op.res = NODE_GET_OFFSET(new_node);
            }

            op.add_linknode_list.push_back(NODE_GET_OFFSET(new_node));
            op.free_linknode_list.push_back(NODE_GET_OFFSET(cur));

            delete[] buf;
            return 0;
        } else {
            if((sizeof(inode_id_t) + 4 + 4 + res.key_len + add_len) > LINK_NODE_CAPACITY){  //同pinode_id聚齐的kv大于一个LinkNode Size，转成B+tree;
                uint32_t need_len = res.key_len + add_len;
                assert(need_len <= LEAF_NODE_CAPACITY);
                BptreeLeafNode *root = AllocBptreeLeafNode();
                root->SetBufNodrain(0, cur->buf + res.key_offset + sizeof(inode_id_t) + 4 + 4, res.key_len);
                uint32_t insert_offset = res.fname_offset - res.key_offset - sizeof(inode_id_t) - 4 - 4;
                uint32_t need_move = res.key_len - insert_offset;
                if(need_move > 0) {
                    root->SetBufNodrain(insert_offset + add_len, root->buf + insert_offset, need_move);
                }
                char *buf = new char[add_len];
                MemoryEncodeHashkeyLenFnameValue(buf, MurmurHash64(fname.data(), fname.size()), fname, value);
                root->SetBufNodrain(insert_offset, buf, add_len);
                root->SetNumAndLenNodrain(res.key_num + 1, need_len);
                root->Flush();

                op.add_leafnode_list.push_back(NODE_GET_OFFSET(root));
                DBG_LOG("[dir] key:%lu tran to bptree:%lu num:%u len:%u", key, NODE_GET_OFFSET(root), root->num, root->len);
                //处理转换成b+tree后的链表；

                delete[] buf;
                uint32_t remain_len = cur->len - res.key_len + 4; //加4是因为|----key(8B)---|---num(4B)---|----len(4B)----| 转成 |----key(8B)---|---num(4B) = 0 ---|---b+tree_pointer(8B)---|
                char *remain_buf = new char[remain_len];   //剩余内容一定很少，转换成memory再操作
                uint32_t first_move = res.key_offset;
                if(first_move){
                    memcpy(remain_buf, cur->buf, first_move);
                }
                MemoryEncodeKeyBptreePointer(remain_buf + first_move, key, NODE_GET_OFFSET(root));
                uint32_t second_move = cur->len - (res.key_offset + sizeof(inode_id_t) + 8 + res.key_len);
                if(second_move){
                    memcpy(remain_buf + first_move + sizeof(inode_id_t) + 12, cur->buf + res.key_offset + sizeof(inode_id_t) + 8 + res.key_len, second_move);
                }
                int ret = LinkNodeTranBptreeDo(op, res, cur, remain_buf, remain_len);
                
                delete[] remain_buf;
                return ret;
            }

            //普通分裂,再插入
            uint32_t new_beyond_buf_size = cur->len + add_len;
            char *buf = new char[new_beyond_buf_size];
            memcpy(buf, cur->buf, cur->len);
            uint32_t insert_offset = res.fname_offset;
            uint32_t need_move = cur->len - insert_offset;
            if(need_move > 0) {
                memmove(buf + insert_offset + add_len, buf + insert_offset, need_move);
            } 
            MemoryEncodeFnameKey(buf + insert_offset, fname, value);
            insert_offset = res.key_offset + 8;
            MemoryEncodeKVnumKVlen(buf + insert_offset, res.key_num + 1, res.key_len + add_len);

            vector<pointer_t> res;
            res.reserve(3);
            LinkNodeSplit(buf, new_beyond_buf_size, res);
            uint32_t res_size = res.size();

            if(!IS_INVALID_POINTER(cur->next)){
                LinkNode *new_node = static_cast<LinkNode *>(NODE_GET_POINTER(res[res_size - 1]));
                new_node->SetNextNodrain(cur->next);
            }
            if(!IS_INVALID_POINTER(cur->prev)){
                LinkNode *new_node = static_cast<LinkNode *>(NODE_GET_POINTER(res[0]));
                new_node->SetPrevNodrain(cur->prev);
            }

            for(auto it : res){
                op.add_linknode_list.push_back(it);
                LinkNode *temp = static_cast<LinkNode *>(NODE_GET_POINTER(it));
                temp->Flush();
            }

            if(!IS_INVALID_POINTER(cur->next)){
                LinkNode *next_node = static_cast<LinkNode *>(NODE_GET_POINTER(cur->next));
                next_node->SetPrevPersist(res[res_size - 1]);
            }
            if(!IS_INVALID_POINTER(cur->prev)){
                LinkNode *prev_node = static_cast<LinkNode *>(NODE_GET_POINTER(cur->prev));
                prev_node->SetNextPersist(res[0]);
            } else {
                op.res = res[0];   //根节点替换
            }
            
            op.free_linknode_list.push_back(NODE_GET_OFFSET(cur));

            delete[] buf;
            return 0;
        }
    }
    //未找到，在节点内插入
    uint32_t add_len = sizeof(inode_id_t) + 4 + 4 + 8 + 4 + fname.size() + sizeof(inode_id_t);
    if(cur->num == 0 || cur->len == 0){  //空节点，直接插入
        char *buf = new char[add_len];
        uint32_t insert_offset = 0;
        MemoryEncodeKey(buf + insert_offset, key);
        insert_offset += sizeof(inode_id_t);
        MemoryEncodeKVnumKVlen(buf + insert_offset, 1, 8 + 4 + fname.size() + sizeof(inode_id_t));
        insert_offset += 8;
        MemoryEncodeFnameKey(buf + insert_offset, fname, value);
        cur->SetBufNodrain(res.key_offset, buf, add_len); 
        cur->SetMinkeyNodrain(key);
        cur->SetMaxkeyNodrain(key);
        cur->SetNumAndLenNodrain(1, add_len);
        cur->Flush();

        delete[] buf;
        return 0;
    }

    if(cur->GetFreeSpace() >= add_len) { //空间足够，可以插入
        LinkNode *new_node = AllocLinkNode();   //超过8B修改都采用COW，copy on write
        new_node->CopyBy(cur);
        uint32_t insert_offset = res.key_offset;
        uint32_t need_move = cur->len - insert_offset;
        if(need_move > 0) {
            new_node->SetBufNodrain(insert_offset + add_len, cur->buf + insert_offset, need_move);
        }
        char *buf = new char[add_len];
        insert_offset = 0;
        MemoryEncodeKey(buf + insert_offset, key);
        insert_offset += sizeof(inode_id_t);
        MemoryEncodeKVnumKVlen(buf + insert_offset, 1, 8 + 4 + fname.size() + sizeof(inode_id_t));
        insert_offset += 8;
        MemoryEncodeFnameKey(buf + insert_offset, fname, value);
        new_node->SetBufNodrain(res.key_offset, buf, add_len); 

        if(res.key_index == 0){ //插入的是最小值
            new_node->SetMinkeyNodrain(key);
        }
        if(res.key_index == cur->num){ //插入的是最大值
            new_node->SetMaxkeyNodrain(key);
        }
        
        new_node->SetNumAndLenNodrain(cur->num + 1, cur->len + add_len);
        new_node->Flush();

        LinkNodeUpdateNextPrev(new_node);

        if(IS_INVALID_POINTER(new_node->prev)){  //替换根节点
            op.res = NODE_GET_OFFSET(new_node);
        }

        op.add_linknode_list.push_back(NODE_GET_OFFSET(new_node));
        op.free_linknode_list.push_back(NODE_GET_OFFSET(cur));

        delete[] buf;
        return 0;
    } else {
        if((add_len) > LINK_NODE_CAPACITY){  //单独一个KV大于，LinkNode Size，转成B+tree;
            assert(0); //文件名太长，超过MAX_FNAME_LEN
            return 0;
        }

        //普通分裂,再插入
        uint32_t new_beyond_buf_size = cur->len + add_len;
        char *buf = new char[new_beyond_buf_size];
        memcpy(buf, cur->buf, cur->len);
        uint32_t insert_offset = res.key_offset;
        uint32_t need_move = cur->len - insert_offset;
        if(need_move > 0) {
            memmove(buf + insert_offset + add_len, buf + insert_offset, need_move);
        } 
        MemoryEncodeKey(buf + insert_offset, key);
        insert_offset += sizeof(inode_id_t);
        MemoryEncodeKVnumKVlen(buf + insert_offset, 1, 8 + 4 + fname.size() + sizeof(inode_id_t));
        insert_offset += 8;
        MemoryEncodeFnameKey(buf + insert_offset, fname, value);

        vector<pointer_t> res;
        res.reserve(3);
        LinkNodeSplit(buf, new_beyond_buf_size, res);
        uint32_t res_size = res.size();

        if(!IS_INVALID_POINTER(cur->next)){
            LinkNode *new_node = static_cast<LinkNode *>(NODE_GET_POINTER(res[res_size - 1]));
            new_node->SetNextNodrain(cur->next);
        }
        if(!IS_INVALID_POINTER(cur->prev)){
            LinkNode *new_node = static_cast<LinkNode *>(NODE_GET_POINTER(res[0]));
            new_node->SetPrevNodrain(cur->prev);
        }

        for(auto it : res){
            op.add_linknode_list.push_back(it);
            LinkNode *temp = static_cast<LinkNode *>(NODE_GET_POINTER(it));
            temp->Flush();
        }

        if(!IS_INVALID_POINTER(cur->next)){
            LinkNode *next_node = static_cast<LinkNode *>(NODE_GET_POINTER(cur->next));
            next_node->SetPrevPersist(res[res_size - 1]);
        }
        if(!IS_INVALID_POINTER(cur->prev)){
            LinkNode *prev_node = static_cast<LinkNode *>(NODE_GET_POINTER(cur->prev));
            prev_node->SetNextPersist(res[0]);
        } else {
            op.res = res[0];
        }
        
        op.free_linknode_list.push_back(NODE_GET_OFFSET(cur));

        delete[] buf;
        return 0;
    }
}

int LinkListInsert(LinkListOp &op, const inode_id_t key, const Slice &fname, const inode_id_t value){
    pointer_t cur = op.root;
    pointer_t prev = op.root;

    LinkNode *cur_node = static_cast<LinkNode *>(NODE_GET_POINTER(cur));
    while(!IS_INVALID_POINTER(cur) && compare_inode_id(key, cur_node->min_key) >= 0) {
        prev = cur;
        cur = cur_node->next;
        cur_node = static_cast<LinkNode *>(NODE_GET_POINTER(cur));
    }
    pointer_t insert = prev;  //在该节点插入kv；
    LinkNode *insert_node = static_cast<LinkNode *>(NODE_GET_POINTER(insert));
    LinkNodeInsert(op, insert_node, key, fname, value);

    return 0;
}


int LinkListGet(LinkNode *root, const inode_id_t key, const Slice &fname, inode_id_t &value){
    pointer_t cur = NODE_GET_OFFSET(root);
    pointer_t prev = cur;

    LinkNode *cur_node = root;
    while(!IS_INVALID_POINTER(cur) && compare_inode_id(key, cur_node->min_key) >= 0) {
        prev = cur;
        cur = cur_node->next;
        cur_node = static_cast<LinkNode *>(NODE_GET_POINTER(cur));
    }
    pointer_t search = prev;  //在该节点查找kv；
    LinkNode *search_node = static_cast<LinkNode *>(NODE_GET_POINTER(search));

    LinkNodeSearchResult res;
    LinkNodeSearch(res, search_node, key, fname);
    if(res.key_find){
        if(res.value_is_bptree){  //bptree中查找
            pointer_t bptree = search_node->DecodeBufGetBptree(res.key_offset + sizeof(inode_id_t) + 4);
            return BptreeGet(bptree, MurmurHash64(fname.data(), fname.size()), fname, value);
        } else if (res.fname_find) {
            uint32_t get_offset = res.fname_offset + 8 + 4 + fname.size();
            value = *reinterpret_cast<inode_id_t *>(search_node->buf + get_offset);
            return 0;
        } else {
            return 2;
        }

    }
    return 2;
}

inode_id_t LinkNodeFindMaxKey(LinkNode *cur){
    inode_id_t temp_key = INVALID_INODE_ID_KEY;
    uint32_t key_num, key_len;
    uint32_t offset = 0;
    uint32_t i = 0;
    for(; i < cur->num; i++){
        cur->DecodeBufGetKeyNumLen(offset, temp_key, key_num, key_len);
        offset += (sizeof(inode_id_t) + 4 + 4 + key_len);
    }
    return temp_key;
}

//删除Bptree树时，后续处理
int LinkNodeDeleteBptree(LinkListOp &op, LinkNodeSearchResult &res, LinkNode *cur){
    uint32_t del_len = sizeof(inode_id_t) + 4 + 8;
    uint32_t remain_len = cur->len - del_len;
    if(remain_len != 0 && remain_len < LINK_NODE_TRIG_MERGE_SIZE ){ //可能需要合并
        if(!IS_INVALID_POINTER(cur->next)){  
            LinkNode *next_node = static_cast<LinkNode *>(NODE_GET_POINTER(cur->next));
            if(next_node->GetFreeSpace() >= remain_len){ //和后节点合并，空间足够组成一个节点
                LinkNode *new_node = AllocLinkNode();   //超过8B修改都采用COW，copy on write
                new_node->CopyBy(cur);
                uint32_t del_offset = res.key_offset;
                uint32_t need_move = cur->len - (del_offset + del_len);
                if(need_move > 0) {
                    new_node->SetBufNodrain(del_offset, cur->buf + del_offset + del_len, need_move);
                }
                uint16_t num = cur->num - 1;
                new_node->SetNumAndLenNodrain(num, cur->len - del_len);
                if(res.key_index == 0){
                     //删除的是最小值
                    inode_id_t min_key;
                    new_node->DecodeBufGetKey(0, min_key);
                    new_node->SetMinkeyNodrain(min_key);
                    
                }
                new_node->SetBufNodrain(new_node->len, next_node->buf, next_node->len);
                new_node->SetNumAndLenNodrain(new_node->num + next_node->num, new_node->len + next_node->len);
                new_node->SetMaxkeyNodrain(next_node->max_key);
                new_node->SetNextNodrain(next_node->next);
                new_node->Flush();

                LinkNodeUpdateNextPrev(new_node);
    
                if(IS_INVALID_POINTER(new_node->prev)){
                    op.res = NODE_GET_OFFSET(new_node);  //头结点替换
                }

                op.free_linknode_list.push_back(NODE_GET_OFFSET(cur));
                op.free_linknode_list.push_back(NODE_GET_OFFSET(next_node));
                op.add_linknode_list.push_back(NODE_GET_OFFSET(new_node));
                return 0;
                
            }
        }

        if(!IS_INVALID_POINTER(cur->prev)){
            LinkNode *prev_node = static_cast<LinkNode *>(NODE_GET_POINTER(cur->prev));
            if(prev_node->GetFreeSpace() >= remain_len){ //和前节点合并，空间足够组成一个节点
                LinkNode *new_node = AllocLinkNode();   //超过8B修改都采用COW，copy on write
                new_node->CopyBy(cur);
                uint32_t del_offset = res.key_offset;
                uint32_t need_move = cur->len - (del_offset + del_len);
                if(need_move > 0) {
                    new_node->SetBufNodrain(del_offset, cur->buf + del_offset + del_len, need_move);
                }
                uint16_t num = cur->num - 1;
                new_node->SetNumAndLenNodrain(num, cur->len - del_len);
                if(res.key_index == (cur->num - 1)){  //删除的是最大值
                    inode_id_t max_key = LinkNodeFindMaxKey(new_node);
                    new_node->SetMaxkeyNodrain(max_key);
                }

                new_node->SetBufNodrain(prev_node->len, new_node->buf, new_node->len);
                new_node->SetBufNodrain(0, prev_node->buf, prev_node->len);
                new_node->SetNumAndLenNodrain(new_node->num + prev_node->num, new_node->len + prev_node->len);
                new_node->SetMinkeyNodrain(prev_node->min_key);
                new_node->SetPrevNodrain(prev_node->prev);
                new_node->Flush();

                LinkNodeUpdateNextPrev(new_node);
    
                if(IS_INVALID_POINTER(new_node->prev)){
                    op.res = NODE_GET_OFFSET(new_node);  //头结点替换
                }

                op.free_linknode_list.push_back(NODE_GET_OFFSET(cur));
                op.free_linknode_list.push_back(NODE_GET_OFFSET(prev_node));
                op.add_linknode_list.push_back(NODE_GET_OFFSET(new_node));
                return 0;
            }
        }

        //前后节点都不适合合并，保持单个节点；
    }

    //剩下内容组成单个节点
    if(remain_len == 0){  //直接删除节点；
        if(!IS_INVALID_POINTER(cur->next)){  //后节点存在
            LinkNode *next_node = static_cast<LinkNode *>(NODE_GET_POINTER(cur->next));
            next_node->SetPrevPersist(cur->prev);
        }

        if(!IS_INVALID_POINTER(cur->prev)){  //前结点存在
            LinkNode *prev_node = static_cast<LinkNode *>(NODE_GET_POINTER(cur->prev));
            prev_node->SetNextPersist(cur->next);
        } else {
            op.res = cur->next;  //头结点替换
        }

        op.free_linknode_list.push_back(NODE_GET_OFFSET(cur));
        return 0;
    }
    LinkNode *new_node = AllocLinkNode();   //超过8B修改都采用COW，copy on write
    new_node->CopyBy(cur);
    uint32_t del_offset = res.key_offset;
    uint32_t need_move = cur->len - (del_offset + del_len);
    if(need_move > 0) {
        new_node->SetBufNodrain(del_offset, cur->buf + del_offset + del_len, need_move);
    }
    
    uint16_t num = cur->num - 1;
    new_node->SetNumAndLenNodrain(num, cur->len - del_len);
    
    if(res.key_index == 0){ //删除的是最小值
        inode_id_t min_key;
        new_node->DecodeBufGetKey(0, min_key);
        new_node->SetMinkeyNodrain(min_key);
    }
    if(res.key_index == (cur->num - 1)){ //删除的是最大值
        inode_id_t max_key = LinkNodeFindMaxKey(new_node);
        new_node->SetMaxkeyNodrain(max_key);
    }
    
    new_node->Flush();

    LinkNodeUpdateNextPrev(new_node);
    
    if(IS_INVALID_POINTER(new_node->prev)){
        op.res = NODE_GET_OFFSET(new_node);  //头结点替换
    }

    op.free_linknode_list.push_back(NODE_GET_OFFSET(cur));
    op.add_linknode_list.push_back(NODE_GET_OFFSET(new_node));
    return 0;
}

int LinkNodeDelete(LinkListOp &op, LinkNode *cur, const inode_id_t key, const Slice &fname){
    LinkNodeSearchResult res;
    LinkNodeSearch(res, cur, key, fname);
    if(!res.key_find) return 1;
    if(res.value_is_bptree){  //bptree中删除
        pointer_t bptree = cur->DecodeBufGetBptree(res.key_offset + sizeof(inode_id_t) + 4);
        
        BptreeOp bop;
        bop.root = bptree;
        bop.res = bptree;
        BptreeDelete(bop, MurmurHash64(fname.data(), fname.size()), fname);
        op.AddBptreeOp(bop);
        if(bop.root != bop.res){  //修改根节点
            if(IS_INVALID_POINTER(bop.res)){  //根节点删除了，
                return LinkNodeDeleteBptree(op, res, cur);
            }
            DBG_LOG("[dir] key:%lu bptree modify new root:%lu old:%lu", key, bop.res, bop.root);
            //根节点替换，直接修改根节点地址
            cur->SetBufPersist(res.key_offset + 8 + 4, &bop.res, sizeof(pointer_t));
        }
        return 0;
    }
    if(!res.fname_find) return 2;
    uint32_t del_len = (res.key_num == 1) ? (sizeof(inode_id_t) + 8 + res.key_len) : (8 + 4 + res.value_len);
    uint32_t remain_len = cur->len - del_len;
    if(remain_len != 0 && remain_len < LINK_NODE_TRIG_MERGE_SIZE ){ //可能需要合并
        if(!IS_INVALID_POINTER(cur->next)){  
            LinkNode *next_node = static_cast<LinkNode *>(NODE_GET_POINTER(cur->next));
            if(next_node->GetFreeSpace() >= remain_len){ //和后节点合并，空间足够组成一个节点
                LinkNode *new_node = AllocLinkNode();   //超过8B修改都采用COW，copy on write
                new_node->CopyBy(cur);
                uint32_t del_offset = (res.key_num == 1) ? res.key_offset : res.fname_offset;
                uint32_t need_move = cur->len - (del_offset + del_len);
                if(need_move > 0) {
                    new_node->SetBufNodrain(del_offset, cur->buf + del_offset + del_len, need_move);
                }
                if(res.key_num > 1){  //修改key_num,key_len,
                    char buf[8];
                    MemoryEncodeKVnumKVlen(buf, res.key_num - 1, res.key_len - del_len);
                    new_node->SetBufNodrain(res.key_offset + 8, buf, 8);
                }
                uint16_t num = (res.key_num == 1) ? (cur->num - 1) : cur->num;
                new_node->SetNumAndLenNodrain(num, cur->len - del_len);
                if(res.key_num == 1 && res.key_index == 0){
                     //删除的是最小值
                    inode_id_t min_key;
                    new_node->DecodeBufGetKey(0, min_key);
                    new_node->SetMinkeyNodrain(min_key);
                    
                }
                new_node->SetBufNodrain(new_node->len, next_node->buf, next_node->len);
                new_node->SetNumAndLenNodrain(new_node->num + next_node->num, new_node->len + next_node->len);
                new_node->SetMaxkeyNodrain(next_node->max_key);
                new_node->SetNextNodrain(next_node->next);
                new_node->Flush();

                LinkNodeUpdateNextPrev(new_node);
    
                if(IS_INVALID_POINTER(new_node->prev)){
                    op.res = NODE_GET_OFFSET(new_node);  //头结点替换
                }

                op.free_linknode_list.push_back(NODE_GET_OFFSET(cur));
                op.free_linknode_list.push_back(NODE_GET_OFFSET(next_node));
                op.add_linknode_list.push_back(NODE_GET_OFFSET(new_node));
                return 0;
                
            }
        }

        if(!IS_INVALID_POINTER(cur->prev)){
            LinkNode *prev_node = static_cast<LinkNode *>(NODE_GET_POINTER(cur->prev));
            if(prev_node->GetFreeSpace() >= remain_len){ //和前节点合并，空间足够组成一个节点
                LinkNode *new_node = AllocLinkNode();   //超过8B修改都采用COW，copy on write
                new_node->CopyBy(cur);
                uint32_t del_offset = (res.key_num == 1) ? res.key_offset : res.fname_offset;
                uint32_t need_move = cur->len - (del_offset + del_len);
                if(need_move > 0) {
                    new_node->SetBufNodrain(del_offset, cur->buf + del_offset + del_len, need_move);
                }
                if(res.key_num > 1){  //修改key_num,key_len,
                    char buf[8];
                    MemoryEncodeKVnumKVlen(buf, res.key_num - 1, res.key_len - del_len);
                    new_node->SetBufNodrain(res.key_offset + 8, buf, 8);
                }
                uint16_t num = (res.key_num == 1) ? (cur->num - 1) : cur->num;
                new_node->SetNumAndLenNodrain(num, cur->len - del_len);
                if(res.key_num == 1 && res.key_index == (cur->num - 1)){  //删除的是最大值
                    inode_id_t max_key = LinkNodeFindMaxKey(new_node);
                    new_node->SetMaxkeyNodrain(max_key);
                }

                new_node->SetBufNodrain(prev_node->len, new_node->buf, new_node->len);
                new_node->SetBufNodrain(0, prev_node->buf, prev_node->len);
                new_node->SetNumAndLenNodrain(new_node->num + prev_node->num, new_node->len + prev_node->len);
                new_node->SetMinkeyNodrain(prev_node->min_key);
                new_node->SetPrevNodrain(prev_node->prev);
                new_node->Flush();

                LinkNodeUpdateNextPrev(new_node);
    
                if(IS_INVALID_POINTER(new_node->prev)){
                    op.res = NODE_GET_OFFSET(new_node);  //头结点替换
                }

                op.free_linknode_list.push_back(NODE_GET_OFFSET(cur));
                op.free_linknode_list.push_back(NODE_GET_OFFSET(prev_node));
                op.add_linknode_list.push_back(NODE_GET_OFFSET(new_node));
                return 0;
            }
        }

        //前后节点都不适合合并，保持单个节点；
    }

    //剩下内容组成单个节点
    if(remain_len == 0){  //直接删除节点；
        if(!IS_INVALID_POINTER(cur->next)){  //后节点存在
            LinkNode *next_node = static_cast<LinkNode *>(NODE_GET_POINTER(cur->next));
            next_node->SetPrevPersist(cur->prev);
        }

        if(!IS_INVALID_POINTER(cur->prev)){  //前结点存在
            LinkNode *prev_node = static_cast<LinkNode *>(NODE_GET_POINTER(cur->prev));
            prev_node->SetNextPersist(cur->next);
        } else {
            op.res = cur->next;  //头结点替换
        }

        op.free_linknode_list.push_back(NODE_GET_OFFSET(cur));
        return 0;
    }

    LinkNode *new_node = AllocLinkNode();   //超过8B修改都采用COW，copy on write
    new_node->CopyBy(cur);
    uint32_t del_offset = (res.key_num == 1) ? res.key_offset : res.fname_offset;
    uint32_t need_move = cur->len - (del_offset + del_len);
    if(need_move > 0) {
        new_node->SetBufNodrain(del_offset, cur->buf + del_offset + del_len, need_move);
    }
    if(res.key_num > 1){  //修改key_num,key_len,
        char buf[8];
        MemoryEncodeKVnumKVlen(buf, res.key_num - 1, res.key_len - del_len);
        new_node->SetBufNodrain(res.key_offset + 8, buf, 8);
    }
    uint16_t num = (res.key_num == 1) ? (cur->num - 1) : cur->num;
    new_node->SetNumAndLenNodrain(num, cur->len - del_len);
    if(res.key_num == 1){
        if(res.key_index == 0){ //删除的是最小值
            inode_id_t min_key;
            new_node->DecodeBufGetKey(0, min_key);
            new_node->SetMinkeyNodrain(min_key);
        }
        if(res.key_index == (cur->num - 1)){ //删除的是最大值
            inode_id_t max_key = LinkNodeFindMaxKey(new_node);
            new_node->SetMaxkeyNodrain(max_key);
        }
    }
    new_node->Flush();

    LinkNodeUpdateNextPrev(new_node);
    
    if(IS_INVALID_POINTER(new_node->prev)){
        op.res = NODE_GET_OFFSET(new_node);  //头结点替换
    }

    op.free_linknode_list.push_back(NODE_GET_OFFSET(cur));
    op.add_linknode_list.push_back(NODE_GET_OFFSET(new_node));
    return 0;

}

int LinkListDelete(LinkListOp &op, const inode_id_t key, const Slice &fname){
    pointer_t head = op.root;
    pointer_t cur = op.root;
    pointer_t prev = op.root;

    LinkNode *cur_node = static_cast<LinkNode *>(NODE_GET_POINTER(cur));
    while(!IS_INVALID_POINTER(cur) && compare_inode_id(key, cur_node->min_key) >= 0) {
        prev = cur;
        cur = cur_node->next;
        cur_node = static_cast<LinkNode *>(NODE_GET_POINTER(cur));
    }
    pointer_t del = prev;  //在该节点插入kv；
    LinkNode *del_node = static_cast<LinkNode *>(NODE_GET_POINTER(del));
    return LinkNodeDelete(op, del_node, key, fname);
}

int MemoryTranToNVMLinkNode(vector<string> &kvs, pointer_t &root, uint32_t &node_num){
    if(kvs.empty()) return 0;
    LinkNode *new_node = AllocLinkNode();
    uint16_t new_node_key_num = 0;
    uint32_t new_node_len = 0;

    root = NODE_GET_OFFSET(new_node);
    vector<LinkNode *> add_nodes;

    inode_id_t key, prev_key;
    uint32_t kv_len;
    pointer_t prev = INVALID_POINTER;
    for(uint32_t i = 0; i < kvs.size(); i++){
        key = MemoryDecodeGetKey(kvs[i].data());
        kv_len = kvs[i].size();
        if(new_node_key_num == 0) new_node->SetMinkeyNodrain(key);

        if((new_node_len + kv_len)  > LINK_NODE_CAPACITY){ //单节点存不下了，生成节点
            new_node->SetNumAndLenNodrain(new_node_key_num, new_node_len);
            new_node->SetMaxkeyNodrain(prev_key);
            new_node->SetPrevNodrain(prev);
            
            new_node_key_num = 0;
            new_node_len = 0;
            prev = NODE_GET_OFFSET(new_node);
            add_nodes.push_back(new_node);

            //新建新节点
            new_node = AllocLinkNode();
            new_node->SetMinkeyNodrain(key);
        }
        new_node->SetBufNodrain(new_node_len, kvs[i].data(), kv_len);
        prev_key = key;
        new_node_len += kv_len;
        new_node_key_num++;

        if(i == kvs.size() - 1){  //最后一个kv，生成新节点
            new_node->SetNumAndLenNodrain(new_node_key_num, new_node_len);
            new_node->SetMaxkeyNodrain(prev_key);
            new_node->SetPrevNodrain(prev);
            add_nodes.push_back(new_node);
        }
    }
    //设置后节点,并刷新节点
    node_num = add_nodes.size();
    for(uint32_t i = 0; i < add_nodes.size() - 1; i++){
        add_nodes[i]->SetNextNodrain(NODE_GET_OFFSET(add_nodes[i + 1]));
        add_nodes[i]->Flush();
    }
    add_nodes[node_num - 1]->Flush();  //刷新尾节点
    return 0;
}

//只搜索key的位置
int LinkNodeSearchKey(LinkNodeSearchResult &res, LinkNode *cur, const inode_id_t key){
    if(compare_inode_id(key, cur->min_key) < 0) {
        res.key_index = 0;
        res.key_offset = 0;
        return 1;
    }
    if(compare_inode_id(key, cur->max_key) > 0) {
        res.key_index = cur->num;
        res.key_offset = cur->len;
        return 1;
    }

    inode_id_t temp_key;
    uint32_t kv_len;
    uint32_t key_num, key_len;
    uint32_t offset = 0;
    uint32_t i = 0;
    for(; i < cur->num; i++){
        cur->DecodeBufGetKeyNumLen(offset, temp_key, key_num, key_len);
        if(compare_inode_id(key, temp_key) == 0) {  //找到key
            res.key_find = true;
            res.key_index = i;
            res.key_offset = offset;
            res.key_num = key_num;
            res.key_len = key_len;
            if(key_num == 0){ //value 是bptree
                res.value_is_bptree = true;
            } 
            return 0;
        }

        if(compare_inode_id(key, temp_key) < 0) {  //未找到
            res.key_index = i;
            res.key_offset = offset;
            return 1;
        }
        if(key_num == 0){
            kv_len = sizeof(inode_id_t) + 4 + 8;
        } else {
            kv_len = sizeof(inode_id_t) + 4 + 4 + key_len;
        }

        offset += kv_len;
    }
    res.key_index = i;
    res.key_offset = offset;
    return 1;
}

//将迁移rehash_kvs和now_kvs归并成new_kvs
int LinkKVSMerge(const string &now_kvs, const string &rehash_kvs, string &new_kvs){
    inode_id_t key = MemoryDecodeGetKey(now_kvs.data());
    uint32_t key_num = 0;
    uint32_t now_offset = sizeof(inode_id_t) + 8;
    uint32_t rehash_offset = sizeof(inode_id_t) + 8;
    uint64_t now_hashkey;
    uint32_t now_value_len;
    uint64_t rehash_hashkey;
    uint32_t rehash_value_len;
    while(now_offset < now_kvs.size() && rehash_offset < rehash_kvs.size()){
        MemoryDecodeHashkeyValuelen(now_kvs.data() + now_offset, now_hashkey, now_value_len);
        MemoryDecodeHashkeyValuelen(rehash_kvs.data() + rehash_offset, rehash_hashkey, rehash_value_len);
        if(now_hashkey == rehash_hashkey){ //相等情况，取now_hashkey的值
            new_kvs.append(now_kvs.data() + now_offset, 8 + 4 + now_value_len);
            now_offset += (8 + 4 + now_value_len);
            rehash_offset += (8 + 4 + rehash_value_len);
            key_num++;
        } else if (now_hashkey < rehash_hashkey) {
            new_kvs.append(now_kvs.data() + now_offset, 8 + 4 + now_value_len);
            now_offset += (8 + 4 + now_value_len);
            key_num++;
        } else {
            new_kvs.append(rehash_kvs.data() + rehash_offset, 8 + 4 + rehash_value_len);
            rehash_offset += (8 + 4 + rehash_value_len);
            key_num++;
        }
    }
    while(now_offset < now_kvs.size()){
        MemoryDecodeHashkeyValuelen(now_kvs.data() + now_offset, now_hashkey, now_value_len);
        new_kvs.append(now_kvs.data() + now_offset, 8 + 4 + now_value_len);
        now_offset += (8 + 4 + now_value_len);
        key_num++;
    }
    while(rehash_offset < rehash_kvs.size()){
        MemoryDecodeHashkeyValuelen(rehash_kvs.data() + rehash_offset, rehash_hashkey, rehash_value_len);
        new_kvs.append(rehash_kvs.data() + rehash_offset, 8 + 4 + rehash_value_len);
        rehash_offset += (8 + 4 + rehash_value_len);
        key_num++;
    }
    uint32_t key_len = new_kvs.size();
    new_kvs.insert(0, reinterpret_cast<const char *>(&key_len), 4);
    new_kvs.insert(0, reinterpret_cast<const char *>(&key_num), 4);
    new_kvs.insert(0, reinterpret_cast<const char *>(&key), sizeof(inode_id_t));
    return 0;
}

//将整棵Bptree的节点加入bop的free链中，层序遍历
void AddBptreeNodeToBop(pointer_t root, BptreeOp &op){
    pointer_t cur = root;
    queue<pointer_t> queues;
    queues.push(cur);
    while(!queues.empty() && !IS_INVALID_POINTER(queues.front())){
        cur = queues.front();
        if(IsIndexNode(cur)){  //中间节点查找
            BptreeIndexNode *cur_node = static_cast<BptreeIndexNode *>(NODE_GET_POINTER(cur));
            for(uint32_t i = 0; i < cur_node->num; i++){
                queues.push(cur_node->entry[i].pointer);
            }
            op.free_indexnode_list.push_back(cur);
        }
        else{    //叶子节点查找
            op.free_leafnode_list.push_back(cur);
        }
        queues.pop();
    }
}

int RehashLinkNodeInsert(LinkListOp &op, LinkNode *cur, const inode_id_t key, string &kvs){
    LinkNodeSearchResult res;
    LinkNodeSearchKey(res, cur, key);
    if(res.key_find){  //key存在，需要和kvs合并
        uint32_t kvs_key_num, kvs_key_len;
        inode_id_t temp_key;
        MemoryDecodeGetKeyNumLen(kvs.data(), temp_key, kvs_key_num, kvs_key_len);
        bool kvs_is_bptree = kvs_key_num == 0;
        if(res.value_is_bptree) { //新插入的kv是bptree，和kvs合并
            if(!kvs_is_bptree){  // kvs不是Bptree，现有的res是bptree
                pointer_t bptree = cur->DecodeBufGetBptree(res.key_offset + sizeof(inode_id_t) + 4);
                BptreeOp bop;
                bop.root = bptree;
                bop.res = bptree;

                DBG_LOG("[dir] do rehash, key:%lu new is bptree:%lu, kvs is not bptree", key, bptree);

                uint64_t hash_fname;
                uint32_t value_len;
                uint32_t offset = sizeof(inode_id_t) + 4 + 4;
                Slice fname;
                inode_id_t value;
                for(uint32_t i = 0; i < res.key_num; i++){
                    MemoryDecodeHashkeyValuelen(kvs.data() + offset, hash_fname, value_len);
                    fname = Slice(kvs.data() + offset + 8, value_len - sizeof(inode_id_t));
                    value = MemoryDecodeGetKey(kvs.data() + offset + 8 + value_len - sizeof(inode_id_t));
                    
                    BptreeOnlyInsert(bop, hash_fname, fname, value);
                    if(bop.res != bop.root){
                        bop.root = bop.res;
                        bop.res = bop.root;
                    }
                }
                op.AddBptreeOp(bop);
                if(bop.res != bptree){  //直接更新根节点
                    cur->SetBufPersist(res.key_offset + sizeof(inode_id_t) + 4, &bop.res, 8);
                }
                return 0;
                
            } else { // kvs是Bptree，现有的res是bptree
                pointer_t res_bptree = cur->DecodeBufGetBptree(res.key_offset + sizeof(inode_id_t) + 4);
                pointer_t kvs_bptree = *reinterpret_cast<const pointer_t *>(kvs.data() + sizeof(inode_id_t) + 4);
                BptreeOp bop;
                bop.root = kvs_bptree;
                bop.res = kvs_bptree;

                DBG_LOG("[dir] do rehash, key:%lu new is bptree:%lu, kvs is bptree:%lu", key, res_bptree, kvs_bptree);

                pointer_t head = INVALID_POINTER;
                BptreeGetLinkHeadNode(res_bptree, head);
                BptreeLeafNode *head_node = static_cast<BptreeLeafNode *>(NODE_GET_POINTER(head));
                Iterator *it = new BptreeIterator(head_node);
                uint64_t hash_fname;
                Slice fname;
                inode_id_t value;
                for(it->SeekToFirst(); it->Valid(); it->Next()){
                    hash_fname = it->hash_fname();
                    fname = it->fname();
                    value = it->value();
                    BptreeInsert(bop, hash_fname, fname, value);
                    if(bop.res != bop.root){
                        bop.root = bop.res;
                        bop.res = bop.root;
                    }
                }
                delete it;
                cur->SetBufPersist(res.key_offset + sizeof(inode_id_t) + 4, &bop.res, 8);   //更新根节点
                AddBptreeNodeToBop(res_bptree, bop);
                op.AddBptreeOp(bop);
                return 0;
            }
            return 0;
        }
        if(kvs_is_bptree){ //kvs是Bptree，现有的res不是bptree
            pointer_t bptree = *reinterpret_cast<const pointer_t *>(kvs.data() + sizeof(inode_id_t) + 4);
            BptreeOp bop;
            bop.root = bptree;
            bop.res = bptree;

            DBG_LOG("[dir] do rehash, key:%lu new is not bptree, kvs is bptree:%lu", key, bptree);

            uint64_t hash_fname;
            uint32_t value_len;
            uint32_t offset = res.key_offset + sizeof(inode_id_t) + 4 + 4;
            Slice fname;
            inode_id_t value;
            for(uint32_t i = 0; i < res.key_num; i++){
                cur->DecodeBufGetHashfnameAndLen(offset, hash_fname, value_len);
                fname = Slice(cur->buf + offset + 8, value_len - sizeof(inode_id_t));
                cur->DecodeBufGetKey(offset + 8 + value_len - sizeof(inode_id_t), value);
                BptreeInsert(bop, hash_fname, fname, value);
                if(bop.res != bop.root){
                    bop.root = bop.res;
                    bop.res = bop.root;
                }
            }
            op.AddBptreeOp(bop);

            uint32_t remain_len = cur->len - res.key_len + 4; //加4是因为|----key(8B)---|---num(4B)---|----len(4B)----| 转成 |----key(8B)---|---num(4B) = 0 ---|---b+tree_pointer(8B)---|
            char *remain_buf = new char[remain_len];   //剩余内容一定很少，转换成memory再操作
            uint32_t first_move = res.key_offset;
            if(first_move){
                memcpy(remain_buf, cur->buf, first_move);
            }
            MemoryEncodeKeyBptreePointer(remain_buf + first_move, key, bop.res);
            uint32_t second_move = cur->len - (res.key_offset + sizeof(inode_id_t) + 8 + res.key_len);
            if(second_move){
                memcpy(remain_buf + first_move + sizeof(inode_id_t) + 12, cur->buf + res.key_offset + sizeof(inode_id_t) + 8 + res.key_len, second_move);
            }
            int ret = LinkNodeTranBptreeDo(op, res, cur, remain_buf, remain_len);

            delete[] remain_buf;
            return ret;
        }
        //kvs和现有的res都不是bptree；
        uint32_t now_kv_len = sizeof(inode_id_t) + 4 + 4 + res.key_len;
        string now_kvs(cur->buf + res.key_offset, now_kv_len);
        string new_kvs;
        LinkKVSMerge(now_kvs, kvs, new_kvs);
        uint32_t add_len = new_kvs.size() - now_kv_len;
        if(cur->GetFreeSpace() >= add_len) { //空间足够，可以插入
            LinkNode *new_node = AllocLinkNode();   //超过8B修改都采用COW，copy on write
            new_node->CopyBy(cur);
            uint32_t insert_offset = res.key_offset;
            uint32_t need_move = cur->len - insert_offset - now_kv_len;
            if(need_move > 0) {
                new_node->SetBufNodrain(insert_offset + new_kvs.size(), cur->buf + insert_offset + now_kv_len, need_move);
            }
            new_node->SetBufNodrain(insert_offset, new_kvs.data(), new_kvs.size()); 
            
            new_node->SetNumAndLenNodrain(cur->num, cur->len + add_len);
            new_node->Flush();
            
            LinkNodeUpdateNextPrev(new_node);
            if(IS_INVALID_POINTER(new_node->prev)){  //更新根节点
                op.res = NODE_GET_OFFSET(new_node);
            }

            op.add_linknode_list.push_back(NODE_GET_OFFSET(new_node));
            op.free_linknode_list.push_back(NODE_GET_OFFSET(cur));

            return 0;
        } else {  //空间不足
            if((new_kvs.size()) > LINK_NODE_CAPACITY){  //同pinode_id聚齐的kv大于一个LinkNode Size，转成B+tree;
                inode_id_t temp_key;
                uint32_t new_kvs_key_num, new_kvs_key_len;
                MemoryDecodeGetKeyNumLen(new_kvs.data(), temp_key, new_kvs_key_num, new_kvs_key_len);
                uint32_t need_len = new_kvs_key_len;
                assert(need_len <= LEAF_NODE_CAPACITY);
                BptreeLeafNode *root = AllocBptreeLeafNode();

                root->SetBufNodrain(0, new_kvs.data() + sizeof(inode_id_t) + 8, new_kvs_key_len);
                root->SetNumAndLenNodrain(new_kvs_key_num, new_kvs_key_len);
                root->Flush();

                op.add_leafnode_list.push_back(NODE_GET_OFFSET(root));

                //处理转换成b+tree后的链表；
                uint32_t remain_len = cur->len - res.key_len + 4; //加4是因为|----key(8B)---|---num(4B)---|----len(4B)----| 转成 |----key(8B)---|---num(4B) = 0 ---|---b+tree_pointer(8B)---|
                char *remain_buf = new char[remain_len];   //剩余内容一定很少，转换成memory再操作
                uint32_t first_move = res.key_offset;
                if(first_move){
                    memcpy(remain_buf, cur->buf, first_move);
                }
                MemoryEncodeKeyBptreePointer(remain_buf + first_move, key, NODE_GET_OFFSET(root));
                uint32_t second_move = cur->len - (res.key_offset + sizeof(inode_id_t) + 8 + res.key_len);
                if(second_move){
                    memcpy(remain_buf + first_move + sizeof(inode_id_t) + 12, cur->buf + res.key_offset + sizeof(inode_id_t) + 8 + res.key_len, second_move);
                }
                int ret = LinkNodeTranBptreeDo(op, res, cur, remain_buf, remain_len);

                delete[] remain_buf;
                return ret;
            }

            //普通分裂,再插入
            uint32_t new_beyond_buf_size = cur->len + add_len;
            char *buf = new char[new_beyond_buf_size];
            memcpy(buf, cur->buf, cur->len);
            uint32_t insert_offset = res.key_offset;
            uint32_t need_move = cur->len - insert_offset - now_kv_len;
            if(need_move > 0) {
                memmove(buf + insert_offset + new_kvs.size(), buf + insert_offset + now_kv_len, need_move);
            } 
            memcpy(buf + insert_offset, new_kvs.data(), new_kvs.size());

            vector<pointer_t> res;
            res.reserve(3);
            LinkNodeSplit(buf, new_beyond_buf_size, res);
            uint32_t res_size = res.size();

            if(!IS_INVALID_POINTER(cur->next)){
                LinkNode *new_node = static_cast<LinkNode *>(NODE_GET_POINTER(res[res_size - 1]));
                new_node->SetNextNodrain(cur->next);
            }
            if(!IS_INVALID_POINTER(cur->prev)){
                LinkNode *new_node = static_cast<LinkNode *>(NODE_GET_POINTER(res[0]));
                new_node->SetPrevNodrain(cur->prev);
            }

            for(auto it : res){
                op.add_linknode_list.push_back(it);
                LinkNode *temp = static_cast<LinkNode *>(NODE_GET_POINTER(it));
                temp->Flush();
            }

            if(!IS_INVALID_POINTER(cur->next)){
                LinkNode *next_node = static_cast<LinkNode *>(NODE_GET_POINTER(cur->next));
                next_node->SetPrevPersist(res[res_size - 1]);
            }
            if(!IS_INVALID_POINTER(cur->prev)){
                LinkNode *prev_node = static_cast<LinkNode *>(NODE_GET_POINTER(cur->prev));
                prev_node->SetNextPersist(res[0]);
            } else {
                op.res = res[0];   //根节点替换
            }
            
            op.free_linknode_list.push_back(NODE_GET_OFFSET(cur));

            delete[] buf;
            return 0;
        }
    }
    //key不存在，将kvs插入
    uint32_t add_len = kvs.size();
    if(cur->num == 0 || cur->len == 0){  //空节点，直接插入
        cur->SetBufNodrain(0, kvs.data(), add_len); 
        cur->SetMinkeyNodrain(key);
        cur->SetMaxkeyNodrain(key);
        cur->SetNumAndLenNodrain(1, add_len);
        cur->Flush();
        return 0;
    }

    if(cur->GetFreeSpace() >= add_len) { //空间足够，可以插入
        LinkNode *new_node = AllocLinkNode();   //超过8B修改都采用COW，copy on write
        new_node->CopyBy(cur);
        uint32_t insert_offset = res.key_offset;
        uint32_t need_move = cur->len - insert_offset;
        if(need_move > 0) {
            new_node->SetBufNodrain(insert_offset + add_len, cur->buf + insert_offset, need_move);
        }
        new_node->SetBufNodrain(res.key_offset, kvs.data(), add_len); 

        if(res.key_index == 0){ //插入的是最小值
            new_node->SetMinkeyNodrain(key);
        }
        if(res.key_index == cur->num){ //插入的是最大值
            new_node->SetMaxkeyNodrain(key);
        }
        
        new_node->SetNumAndLenNodrain(cur->num + 1, cur->len + add_len);
        new_node->Flush();

        LinkNodeUpdateNextPrev(new_node);

        if(IS_INVALID_POINTER(new_node->prev)){  //替换根节点
            op.res = NODE_GET_OFFSET(new_node);
        }

        op.add_linknode_list.push_back(NODE_GET_OFFSET(new_node));
        op.free_linknode_list.push_back(NODE_GET_OFFSET(cur));

        return 0;
    } else {
        if((add_len) > LINK_NODE_CAPACITY){  //单独一个KVS大于，LinkNode Size，转成B+tree;
            assert(0); //不可能存在这种情况
            return 0;
        }

        //普通分裂,再插入
        uint32_t new_beyond_buf_size = cur->len + add_len;
        char *buf = new char[new_beyond_buf_size];
        memcpy(buf, cur->buf, cur->len);
        uint32_t insert_offset = res.key_offset;
        uint32_t need_move = cur->len - insert_offset;
        if(need_move > 0) {
            memmove(buf + insert_offset + add_len, buf + insert_offset, need_move);
        } 
        memcpy(buf + insert_offset, kvs.data(), add_len);

        vector<pointer_t> res;
        res.reserve(3);
        LinkNodeSplit(buf, new_beyond_buf_size, res);
        uint32_t res_size = res.size();

        if(!IS_INVALID_POINTER(cur->next)){
            LinkNode *new_node = static_cast<LinkNode *>(NODE_GET_POINTER(res[res_size - 1]));
            new_node->SetNextNodrain(cur->next);
        }
        if(!IS_INVALID_POINTER(cur->prev)){
            LinkNode *new_node = static_cast<LinkNode *>(NODE_GET_POINTER(res[0]));
            new_node->SetPrevNodrain(cur->prev);
        }

        for(auto it : res){
            op.add_linknode_list.push_back(it);
            LinkNode *temp = static_cast<LinkNode *>(NODE_GET_POINTER(it));
            temp->Flush();
        }

        if(!IS_INVALID_POINTER(cur->next)){
            LinkNode *next_node = static_cast<LinkNode *>(NODE_GET_POINTER(cur->next));
            next_node->SetPrevPersist(res[res_size - 1]);
        }
        if(!IS_INVALID_POINTER(cur->prev)){
            LinkNode *prev_node = static_cast<LinkNode *>(NODE_GET_POINTER(cur->prev));
            prev_node->SetNextPersist(res[0]);
        } else {
            op.res = res[0];
        }
        
        op.free_linknode_list.push_back(NODE_GET_OFFSET(cur));

        delete[] buf;
        return 0;
    }
}

int RehashLinkListInsert(LinkListOp &op, const inode_id_t key, string &kvs){
    pointer_t cur = op.root;
    pointer_t prev = op.root;

    LinkNode *cur_node = static_cast<LinkNode *>(NODE_GET_POINTER(cur));
    while(!IS_INVALID_POINTER(cur) && compare_inode_id(key, cur_node->min_key) >= 0) {
        prev = cur;
        cur = cur_node->next;
        cur_node = static_cast<LinkNode *>(NODE_GET_POINTER(cur));
    }
    pointer_t insert = prev;  //在该节点插入kv；
    LinkNode *insert_node = static_cast<LinkNode *>(NODE_GET_POINTER(insert));
    RehashLinkNodeInsert(op, insert_node, key, kvs);

    return 0;
}

////// bptree
BptreeIndexNode *AllocBptreeIndexNode(){
    BptreeIndexNode *node = static_cast<BptreeIndexNode *>(node_allocator->AllocateAndInit(DIR_BPTREE_INDEX_NODE_SIZE, 0));
    node->SetTypeNodrain(DirNodeType::BPTREEINDEXNODE_TYPE);
    return node;
}

BptreeLeafNode *AllocBptreeLeafNode(){
    BptreeLeafNode *node = static_cast<BptreeLeafNode *>(node_allocator->AllocateAndInit(DIR_BPTREE_LEAF_NODE_SIZE, 0));
    node->SetTypeNodrain(DirNodeType::BPTREELEAFNODE_TYPE);
    return node;
}

struct BptreeIndexNodeSearch {  //中间节点查找的路径
    pointer_t node;
    uint32_t index;   //中间节点为index， 

    BptreeIndexNodeSearch() : node(INVALID_POINTER), index(0) {}
    BptreeIndexNodeSearch(pointer_t n, uint32_t i) : node(n), index(i) {}
    ~BptreeIndexNodeSearch() {}
};

struct BptreeSearchResult {
    bool key_find;
    uint32_t index_level; //Bptree index搜索层数,0代表没有中间节点
    BptreeIndexNodeSearch path[MAX_DIR_BPTREE_LEVEL];  //中间层搜索路径
    pointer_t leaf_node;       //叶子节点查找结果
    uint32_t leaf_key_offset;  //叶子节点查找结果
    uint32_t leaf_value_len;   //叶子节点查找结果
    

    BptreeSearchResult() : key_find(false), index_level(0), leaf_node(INVALID_POINTER), leaf_key_offset(0), leaf_value_len(0) {}
    ~BptreeSearchResult() {}
};

bool IsIndexNode(pointer_t ptr){
    DirNodeType type = *static_cast<DirNodeType *>(NODE_GET_POINTER(ptr));
    return type == DirNodeType::BPTREEINDEXNODE_TYPE;
}

uint64_t BptreeNodeGetMinkey(pointer_t ptr){
    if(IsIndexNode(ptr)){
        BptreeIndexNode *index_node = static_cast<BptreeIndexNode *>(NODE_GET_POINTER(ptr));
        return index_node->entry[0].key;
    } else {
        BptreeLeafNode *leaf_node = static_cast<BptreeLeafNode *>(NODE_GET_POINTER(ptr));
        return leaf_node->GetMinKey();
    }
}

//二分查找大于等于hash_key的最小值，index一定会返回一个值，尽管hash_key小于所有值的时候，这是index = 0；
bool BanirySearchIndex(BptreeIndexNode *cur, const uint64_t hash_key, uint32_t &index){
    uint32_t max = cur->num - 1;
    uint32_t left = 0;
    uint32_t right = max;
    uint32_t mid;
    while(left < right){
        mid = (left + right + 1) / 2;
        if(hash_key >= cur->entry[mid].key){
            left = mid;
        }
        else {
            right = mid - 1;
        }
    }

    index = left;
    if(left == 0 && hash_key < cur->entry[left].key) return false; 
    return true;
}

bool LinearSearchLeaf(BptreeLeafNode *cur, const uint64_t hash_key, uint32_t &key_offset, uint32_t &value_len){
    uint32_t num = cur->num;
    uint64_t temp_key;
    uint32_t len;
    uint32_t offset = 0;
    for(uint32_t i = 0; i < num; i++){
        cur->DecodeBufGetKeyValuelen(offset, temp_key, len);
        if(temp_key == hash_key){
            key_offset = offset;
            value_len = len;
            return true;
        }
        if(temp_key > hash_key){  //未找到
            key_offset = offset;
            value_len = len;
            return false;
        }
        offset += (8 + 4 + len);
    }
    key_offset = offset;
    return false;
}


int BptreeSearch(BptreeSearchResult &res, pointer_t root, const uint64_t hash_key) {
    pointer_t cur = root;
    uint32_t index = 0;
    while(!IS_INVALID_POINTER(cur)){
        if(IsIndexNode(cur)){  //中间节点查找
            BptreeIndexNode *cur_node = static_cast<BptreeIndexNode *>(NODE_GET_POINTER(cur));
            BanirySearchIndex(cur_node, hash_key, index);
            res.path[res.index_level].node = cur;
            res.path[res.index_level].index = index;
            res.index_level++;
            cur = cur_node->entry[index].pointer;
        }
        else{    //叶子节点查找
            BptreeLeafNode *cur_node = static_cast<BptreeLeafNode *>(NODE_GET_POINTER(cur));
            res.key_find = LinearSearchLeaf(cur_node, hash_key, res.leaf_key_offset, res.leaf_value_len);
            res.leaf_node = cur;
            break;
        }
    }
    return 0;
}

int BptreeLeafNodeSplit(const char *buf, uint32_t all_key_num, uint32_t len, vector<pointer_t> &res){  //只会分裂成两个节点
    uint32_t offset = 0;
    uint64_t hash_key;
    uint32_t value_len;
    uint32_t split_size = len / 2; //分为两半的size；
    uint32_t key_num = 0;
    while(offset < len) {
        MemoryDecodeHashkeyValuelen(buf, hash_key, value_len);
        offset += (8 + 4 + value_len);
        key_num++;
        if(offset >= split_size){
            assert(offset <= LEAF_NODE_CAPACITY);
            assert((len - offset) <= LEAF_NODE_CAPACITY);
            BptreeLeafNode *left_node = AllocBptreeLeafNode();
            left_node->SetBufNodrain(0, buf, offset);
            left_node->SetNumAndLenNodrain(key_num, offset);

            BptreeLeafNode *right_node = AllocBptreeLeafNode();
            right_node->SetBufNodrain(0, buf + offset, len - offset);
            right_node->SetNumAndLenNodrain(all_key_num - key_num, len - offset);

            left_node->SetNextNodrain(NODE_GET_OFFSET(right_node));
            right_node->SetPrevNodrain(NODE_GET_OFFSET(left_node));

            res.push_back(NODE_GET_OFFSET(left_node));
            res.push_back(NODE_GET_OFFSET(right_node));

            return 0;
        }
    }
    return 0;
}

void IndexNodeUpdateEntry(BptreeSearchResult &res, uint32_t father_level, pointer_t child_node){
    if(father_level == 0) return;
    BptreeIndexNode *father = static_cast<BptreeIndexNode *>(NODE_GET_POINTER(res.path[father_level - 1].node));
    uint32_t child_index = res.path[father_level - 1].index;
    uint64_t child_min_key = BptreeNodeGetMinkey(child_node);
    bool child_update_min_key = father->entry[child_index].key != child_min_key;
    
    if(child_update_min_key) {  //child改了最小值，可能整个路径都要修改最小值
        father->SetEntryPersistByIndex(child_index, BptreeNodeGetMinkey(child_node), child_node);   //因为范围是最小值，key和pointer_t都是8B，保证原子性，
        
        uint32_t level = father_level - 1;
        pointer_t child = res.path[father_level - 1].node;
        child_update_min_key = child_index == 0;  //父节点的最小值有没有变
        while(level > 0 && child_update_min_key){
            father = static_cast<BptreeIndexNode *>(NODE_GET_POINTER(res.path[level - 1].node));
            child_index = res.path[level - 1].index;
            child_min_key = BptreeNodeGetMinkey(child);
            father->SetEntryPersist(child_index * sizeof(IndexNodeEntry), &child_min_key, 8);
            child = res.path[level - 1].node;
            child_update_min_key = child_index == 0;
            level--;
        }
    } else {  //只需要改指针
        father->SetEntryPersist(child_index * sizeof(IndexNodeEntry) + 8, &child_node, sizeof(pointer_t));
    }

}

//更新new_node的后节点和前节点
void LeafNodeUpdateNextPrev(BptreeLeafNode *new_node){
    if(!IS_INVALID_POINTER(new_node->next)) {
        BptreeLeafNode *next_node = static_cast<BptreeLeafNode *>(NODE_GET_POINTER(new_node->next));
        next_node->SetPrevPersist(NODE_GET_OFFSET(new_node));
    }

    if(!IS_INVALID_POINTER(new_node->prev)){
        BptreeLeafNode *prev_node = static_cast<BptreeLeafNode *>(NODE_GET_POINTER(new_node->prev));
        prev_node->SetNextPersist(NODE_GET_OFFSET(new_node));
    }
}

//ret是新创的节点
int BptreeLeafNodeInsert(BptreeOp &op, BptreeSearchResult &res, BptreeLeafNode *cur, const uint64_t hash_key, const Slice &fname, const inode_id_t value, vector<pointer_t> &ret){
    uint32_t add_len = 8 + 4 + fname.size() + sizeof(inode_id_t);
    if(cur->GetFreeSpace() >= add_len && res.leaf_key_offset == cur->len){  //空间足够，并且是追加到叶节点后，可直接在原节点操作
        char *buf = new char[add_len];
        MemoryEncodeHashkeyLenFnameValue(buf, hash_key, fname, value);
        cur->SetBufPersist(res.leaf_key_offset, buf, add_len);
        cur->SetNumAndLenPersist(cur->num + 1, cur->len + add_len);

        delete[] buf;
        return 0;
    }
    if(cur->GetFreeSpace() >= add_len){  //一个节点能够存下
        BptreeLeafNode *new_node = AllocBptreeLeafNode();
        new_node->CopyBy(cur);
        uint32_t need_move = cur->len - res.leaf_key_offset;
        if(need_move){
            new_node->SetBufNodrain(res.leaf_key_offset + add_len, new_node->buf + res.leaf_key_offset, need_move);
        }
        char *buf = new char[add_len];
        MemoryEncodeHashkeyLenFnameValue(buf, hash_key, fname, value);
        new_node->SetBufNodrain(res.leaf_key_offset, buf, add_len);
        new_node->SetNumAndLenNodrain(new_node->num + 1, new_node->len + add_len);
        new_node->Flush();
        
        LeafNodeUpdateNextPrev(new_node);

        op.free_leafnode_list.push_back(NODE_GET_OFFSET(cur));
        op.add_leafnode_list.push_back(NODE_GET_OFFSET(new_node));
        ret.push_back(NODE_GET_OFFSET(new_node));
        delete[] buf;
        return 0;
    }

    //需要分裂
    char *buf = new char[cur->len + add_len];
    memcpy(buf, cur->buf, cur->len);
    uint32_t need_move = cur->len - res.leaf_key_offset;
    if(need_move){
        memmove(buf + res.leaf_key_offset + add_len, buf + res.leaf_key_offset, need_move);
    }
    MemoryEncodeHashkeyLenFnameValue(buf + res.leaf_key_offset, hash_key, fname, value);
    vector<pointer_t> split_node;
    BptreeLeafNodeSplit(buf, cur->num + 1, cur->len + add_len, split_node);
    BptreeLeafNode *left_node = static_cast<BptreeLeafNode *>(NODE_GET_POINTER(split_node[0]));
    BptreeLeafNode *right_node = static_cast<BptreeLeafNode *>(NODE_GET_POINTER(split_node[1]));
    if(!IS_INVALID_POINTER(cur->next)){
        right_node->SetNextNodrain(cur->next);
    }
    if(!IS_INVALID_POINTER(cur->prev)){
        left_node->SetPrevNodrain(cur->prev);
    }
    left_node->Flush();
    right_node->Flush();

    if(!IS_INVALID_POINTER(cur->next)){
        BptreeLeafNode *next_node = static_cast<BptreeLeafNode *>(NODE_GET_POINTER(cur->next));
        next_node->SetPrevPersist(split_node[1]);
    }
    if(!IS_INVALID_POINTER(cur->prev)){
        BptreeLeafNode *prev_node = static_cast<BptreeLeafNode *>(NODE_GET_POINTER(cur->prev));
        prev_node->SetNextPersist(split_node[0]);
    }

    op.free_leafnode_list.push_back(NODE_GET_OFFSET(cur));
    op.add_leafnode_list.push_back(split_node[0]);
    op.add_leafnode_list.push_back(split_node[1]);
    ret.push_back(split_node[0]);
    ret.push_back(split_node[1]);

    delete[] buf;
    return 0;
}

//特定函数，在中间节点的index处插入两个分裂的子节点，操作过程产生的新节点存在add中
int BptreeIndexNodeInsert(BptreeOp &op, pointer_t cur, uint32_t index, bool need_update_min_key, vector<pointer_t> &ret, vector<pointer_t> &add){
    assert(ret.size() == 2);
    BptreeIndexNode *cur_node = static_cast<BptreeIndexNode *>(NODE_GET_POINTER(cur));
    if((cur_node->num + ret.size() - 1) <= INDEX_NODE_CAPACITY){  //一个节点能存下
        if(cur_node->num - 1 == index){  //在中间节点最后面加入，可在原节点修改
            cur_node->SetEntryPersist(index * sizeof(IndexNodeEntry) + 8, &(ret[0]), sizeof(pointer_t));
            if(need_update_min_key){
                uint64_t min_key = BptreeNodeGetMinkey(ret[0]);
                cur_node->SetEntryPersist(index * sizeof(IndexNodeEntry), &min_key, 8);
            }
            uint64_t min_key = BptreeNodeGetMinkey(ret[1]);
            cur_node->SetEntryPersistByIndex(index + 1, min_key, ret[1]);
            cur_node->SetNumPersist(cur_node->num + 1);
            return 0;
        } else {
            BptreeIndexNode *new_node = AllocBptreeIndexNode();
            new_node->CopyBy(cur_node);
            uint32_t need_move = new_node->num - 1 - index;
            if(need_move > 0){
                new_node->SetEntryNodrain((index + 2) * sizeof(IndexNodeEntry), &(new_node->entry[index + 1]), need_move * sizeof(IndexNodeEntry));
            }
            new_node->SetEntryNodrain(index * sizeof(IndexNodeEntry) + 8, &(ret[0]), sizeof(pointer_t));
            if(need_update_min_key){
                uint64_t min_key = BptreeNodeGetMinkey(ret[0]);
                new_node->SetEntryNodrain(index * sizeof(IndexNodeEntry), &min_key, 8);
            }
            uint64_t min_key = BptreeNodeGetMinkey(ret[1]);
            new_node->SetEntryNodrainByIndex(index + 1, min_key, ret[1]);
            new_node->SetNumNodrain(new_node->num + 1);
            new_node->Flush();

            op.free_indexnode_list.push_back(cur);
            op.add_indexnode_list.push_back(NODE_GET_OFFSET(new_node));
            add.push_back(NODE_GET_OFFSET(new_node));
            return 0;
        }
    } else {
        //分裂,
        uint32_t entry_num = cur_node->num + ret.size() - 1;
        IndexNodeEntry *entrys = new IndexNodeEntry[entry_num];
        memcpy(entrys, cur_node->entry, cur_node->num * sizeof(IndexNodeEntry));
        uint32_t need_move = cur_node->num - 1 - index;
        if(need_move > 0){
            memmove(&(entrys[index + 2]), &(entrys[index + 1]), need_move * sizeof(IndexNodeEntry));
        }
        entrys[index].pointer = ret[0];
        if(need_update_min_key){
            uint64_t min_key = BptreeNodeGetMinkey(ret[0]);
            entrys[index].key = min_key;
        }
        uint64_t min_key = BptreeNodeGetMinkey(ret[1]);
        entrys[index + 1].key = min_key;
        entrys[index + 1].pointer = ret[1];
        //进行分裂
        uint32_t split_num = entry_num / 2;
        BptreeIndexNode *left_node = AllocBptreeIndexNode();
        left_node->SetEntryNodrain(0, entrys, split_num * sizeof(IndexNodeEntry));
        left_node->SetNumNodrain(split_num);
        left_node->Flush();
        BptreeIndexNode *right_node = AllocBptreeIndexNode();
        right_node->SetEntryNodrain(0, &(entrys[split_num]), (entry_num - split_num) * sizeof(IndexNodeEntry));
        right_node->SetNumNodrain(entry_num - split_num);
        right_node->Flush();

        op.free_indexnode_list.push_back(cur);
        op.add_indexnode_list.push_back(NODE_GET_OFFSET(left_node));
        op.add_indexnode_list.push_back(NODE_GET_OFFSET(right_node));
        add.push_back(NODE_GET_OFFSET(left_node));
        add.push_back(NODE_GET_OFFSET(right_node));

        delete[] entrys;
        return 0;
    }
}

int BptreeInsert(BptreeOp &op, const uint64_t hash_key, const Slice &fname, const inode_id_t value){
    BptreeSearchResult res;
    BptreeSearch(res, op.root, hash_key);
    if(res.key_find){
        //key存在，直接修改value
        BptreeLeafNode *modify = static_cast<BptreeLeafNode *>(NODE_GET_POINTER(res.leaf_node));
        uint32_t offset = res.leaf_key_offset + 8 + 4 + res.leaf_value_len - sizeof(inode_id_t);
        modify->SetBufPersist(offset, &value, sizeof(inode_id_t));
        return 0;
    }
    //插入叶节点
    BptreeLeafNode *leaf_node = static_cast<BptreeLeafNode *>(NODE_GET_POINTER(res.leaf_node));
    vector<pointer_t> ret;
    ret.reserve(3);
    BptreeLeafNodeInsert(op, res, leaf_node, hash_key, fname, value, ret);
    if(ret.size() == 0){ //没有新增节点，直接返回
        return 0;
    } else if(ret.size() == 1){  //新增一个节点，要更新索引值
        if(res.index_level > 0){
            IndexNodeUpdateEntry(res, res.index_level, ret[0]);
            return 0;
        }
        if(res.index_level == 0 ){ //根节点是叶节点，并且叶节点是根节点，则更新根节点
            op.res = ret[0];
        }
        return 0;
    } else if(ret.size() == 2){  //叶节点分裂了，需要在index中插入
        if(res.index_level == 0){ //没有中间节点，新创root中间节点，
            BptreeIndexNode *root = AllocBptreeIndexNode();
            root->SetNumNodrain(2);
            BptreeLeafNode *left_node = static_cast<BptreeLeafNode *>(NODE_GET_POINTER(ret[0]));
            uint64_t left_node_min_key = left_node->GetMinKey();
            root->SetEntryNodrainByIndex(0, left_node_min_key, ret[0]);
            BptreeLeafNode *right_node = static_cast<BptreeLeafNode *>(NODE_GET_POINTER(ret[1]));
            uint64_t right_node_min_key = right_node->GetMinKey();
            root->SetEntryNodrainByIndex(1, right_node_min_key, ret[1]);
            root->Flush();

            op.add_indexnode_list.push_back(NODE_GET_OFFSET(root));
            op.res = NODE_GET_OFFSET(root);
            return 0;
        } else {
            uint32_t i = res.index_level;
            vector<pointer_t> last_level_add_node;
            for(auto it : ret){
                last_level_add_node.push_back(it);
            }

            vector<pointer_t> add_node;
            add_node.reserve(3);
            bool need_update_min_key = res.leaf_key_offset == 0;
            while(i > 0){
                add_node.clear();
                
                BptreeIndexNodeInsert(op, res.path[i - 1].node, res.path[i - 1].index, need_update_min_key, last_level_add_node, add_node);
                need_update_min_key = need_update_min_key && (res.path[i - 1].index == 0);  //下层节点的最小值修改的，并且下层节点在该层节点index == 0处,说明该层的最小值也修改了
                if(add_node.size() == 0){ //没有新增节点，直接返回
                    return 0;
                } else if(add_node.size() == 1){  //一个节点修改了，直接在上层节点修改，
                    if(i == 1){ //没有上层了，root节点替换
                        op.res = add_node[0];

                        return 0;
                    }
                    //在father节点修改索引
                    IndexNodeUpdateEntry(res, i - 1, add_node[0]);
                    
                    return 0;
                } else if(add_node.size() == 2){  //循环继续往上层插入两个值
                    if(i == 1){ //根节点分裂了，新建根节点
                        BptreeIndexNode *root = AllocBptreeIndexNode();
                        root->SetNumNodrain(2);
                        uint64_t left_node_min_key = BptreeNodeGetMinkey(add_node[0]);
                        root->SetEntryNodrainByIndex(0, left_node_min_key, add_node[0]);
                        uint64_t right_node_min_key = BptreeNodeGetMinkey(add_node[1]);;
                        root->SetEntryNodrainByIndex(1, right_node_min_key, add_node[1]);
                        root->Flush();

                        op.add_indexnode_list.push_back(NODE_GET_OFFSET(root));
                        op.res = NODE_GET_OFFSET(root);
                        return 0;
                    }
                    //普通层继续循环插入两个节点
                    last_level_add_node.clear();
                    last_level_add_node.push_back(add_node[0]);
                    last_level_add_node.push_back(add_node[1]);
                }
                i--;
            }
        }
    } else {
        return -1;
    }
    return 0;
}

int BptreeGet(pointer_t root, const uint64_t hash_key, const Slice &fname, inode_id_t &value){
    pointer_t cur = root;
    uint32_t index = 0;
    BptreeSearchResult res;
    bool find = false;
    while(!IS_INVALID_POINTER(cur)){
        if(IsIndexNode(cur)){  //中间节点查找
            BptreeIndexNode *cur_node = static_cast<BptreeIndexNode *>(NODE_GET_POINTER(cur));
            find = BanirySearchIndex(cur_node, hash_key, index);
            if(!find) return 2; //未找到
            res.path[res.index_level].node = cur;
            res.path[res.index_level].index = index;
            res.index_level++;
            cur = cur_node->entry[index].pointer;
        }
        else{    //叶子节点查找
            BptreeLeafNode *cur_node = static_cast<BptreeLeafNode *>(NODE_GET_POINTER(cur));
            res.key_find = LinearSearchLeaf(cur_node, hash_key, res.leaf_key_offset, res.leaf_value_len);
            res.leaf_node = cur;
            break;
        }
    }
    if(res.key_find){
        BptreeLeafNode *leaf = static_cast<BptreeLeafNode *>(NODE_GET_POINTER(res.leaf_node));
        value = *reinterpret_cast<inode_id_t *>(leaf->buf + res.leaf_key_offset + 8 + 4 + fname.size());
        return 0;
    }
    return 2; //未找到
}

//对中间节点res.path[level - 1] 的left_index处更改两个节点，left和right
int BptreeIndexNodeUpdateTwoEntry(BptreeOp &op, BptreeSearchResult &res, uint32_t level, pointer_t left, pointer_t right, uint32_t left_index){
    BptreeIndexNode *cur = static_cast<BptreeIndexNode *>(NODE_GET_POINTER(res.path[level - 1].node));
    BptreeIndexNode *new_node = AllocBptreeIndexNode();
    new_node->CopyBy(cur);
    new_node->SetEntryNodrainByIndex(left_index, BptreeNodeGetMinkey(left), left);
    new_node->SetEntryNodrainByIndex(left_index + 1, BptreeNodeGetMinkey(right), right);
    new_node->Flush();

    if(level > 1){  //需要改父节点的索引
        IndexNodeUpdateEntry(res, level - 1, NODE_GET_OFFSET(new_node));
    }
    else {  //根节点替换了
        op.res = NODE_GET_OFFSET(new_node);
    }

    op.free_indexnode_list.push_back(res.path[level - 1].node);
    op.add_indexnode_list.push_back(NODE_GET_OFFSET(new_node));

    return 0;
}

//针对合并对上层影响的情况：对搜索的路径res.path的level层中间节点进行在update_index处，更改为update,并删除update_index + 1
int BptreeIndexNodeUpdateAndDelete(BptreeOp &op, BptreeSearchResult &res, uint32_t level, pointer_t update, uint32_t update_index){
    BptreeIndexNode *cur = static_cast<BptreeIndexNode *>(NODE_GET_POINTER(res.path[level - 1].node));
    uint32_t remain_num = cur->num - 1;
    if(remain_num == 1){  //说明该节点只有一项的，删除层，
        if(level == 1){  //当前节点是根节点，删除层
            op.free_indexnode_list.push_back(res.path[level - 1].node);
            op.res = update;
            return 0;
        } else {
            assert(0);  //由于除了根节点，index节点不可能存在这种情况，因为entry过少会进行平衡 INDEX_NODE_TRIG_BALANCE_SIZE > 2;
            return 0;
        }
    }
    if(remain_num < INDEX_NODE_TRIG_MERGE_SIZE) {  //可能需要合并
        if(level > 1){  //存在父节点才可合并
            BptreeIndexNode *father = static_cast<BptreeIndexNode *>(NODE_GET_POINTER(res.path[level - 2].node));
            uint32_t father_index = res.path[level - 2].index;
            if(father_index != father->num - 1){  //优先和后节点合并
                BptreeIndexNode *next_node = static_cast<BptreeIndexNode *>(NODE_GET_POINTER(father->entry[father_index + 1].pointer));
                if(next_node->GetFreeSpace() >= remain_num){
                    BptreeIndexNode *new_node = AllocBptreeIndexNode();
                    new_node->CopyBy(cur);
                    uint32_t need_move = remain_num - 1 - update_index;
                    if(need_move) {
                        new_node->SetEntryNodrain((update_index + 1) * sizeof(IndexNodeEntry), new_node->entry + update_index + 2, need_move * sizeof(IndexNodeEntry));
                    }
                    new_node->SetEntryNodrainByIndex(update_index, BptreeNodeGetMinkey(update), update);
                    new_node->SetEntryNodrain(remain_num * sizeof(IndexNodeEntry), next_node->entry, next_node->num * sizeof(IndexNodeEntry));
                    new_node->SetNumNodrain(remain_num + next_node->num);            
                    new_node->Flush();
        
                    op.free_indexnode_list.push_back(res.path[level - 1].node);
                    op.free_indexnode_list.push_back(NODE_GET_OFFSET(next_node));
                    op.add_indexnode_list.push_back(NODE_GET_OFFSET(new_node));

                    return BptreeIndexNodeUpdateAndDelete(op, res, level - 1, NODE_GET_OFFSET(new_node), father_index);
                }
            }
            if(father_index > 0){  //和前节点合并
                BptreeIndexNode *prev_node = static_cast<BptreeIndexNode *>(NODE_GET_POINTER(father->entry[father_index - 1].pointer));
                if(prev_node->GetFreeSpace() >= remain_num){
                    BptreeIndexNode *new_node = AllocBptreeIndexNode();
                    new_node->CopyBy(prev_node);
                    uint32_t first_move = update_index;
                    if(first_move){
                        new_node->SetEntryNodrain(new_node->num * sizeof(IndexNodeEntry), cur->entry, first_move * sizeof(IndexNodeEntry));
                    }
                    new_node->SetEntryNodrainByIndex(new_node->num + update_index, BptreeNodeGetMinkey(update), update);
                    uint32_t second_move = remain_num - 1 - update_index;
                    if(second_move){
                        new_node->SetEntryNodrain((new_node->num + update_index + 1) * sizeof(IndexNodeEntry), cur->entry + update_index + 2, second_move * sizeof(IndexNodeEntry));
                    }
                    new_node->SetNumNodrain(new_node->num + remain_num);
                    new_node->Flush();

                    op.free_indexnode_list.push_back(res.path[level - 1].node);
                    op.free_indexnode_list.push_back(NODE_GET_OFFSET(prev_node));
                    op.add_indexnode_list.push_back(NODE_GET_OFFSET(new_node));

                    return BptreeIndexNodeUpdateAndDelete(op, res, level - 1, NODE_GET_OFFSET(new_node), father_index - 1);
                }
            }

            if(remain_num < INDEX_NODE_TRIG_BALANCE_SIZE){  //该节点无法和前后节点合并成单个节点，但是本身节点又太小，所以和左右节点合并成两个节点，平衡节点
                if(father_index != father->num - 1){  //优先和后面节点合并成两个节点
                    BptreeIndexNode *next_node = static_cast<BptreeIndexNode *>(NODE_GET_POINTER(father->entry[father_index + 1].pointer));
                    uint32_t entry_num = remain_num + next_node->num;
                    IndexNodeEntry *entrys = new IndexNodeEntry[entry_num];
                    memcpy(entrys, cur->entry, remain_num * sizeof(IndexNodeEntry));
                    uint32_t need_move = remain_num - 1 - update_index;
                    if(need_move > 0){
                        memmove(&(entrys[update_index + 1]), &(entrys[update_index + 2]), need_move * sizeof(IndexNodeEntry));
                    }
                    entrys[update_index].key = BptreeNodeGetMinkey(update);
                    entrys[update_index].pointer = update;
                    memcpy(&(entrys[remain_num]), next_node->entry, next_node->num * sizeof(IndexNodeEntry));
                    //进行分裂
                    uint32_t split_num = entry_num / 2;
                    BptreeIndexNode *left_node = AllocBptreeIndexNode();
                    left_node->SetEntryNodrain(0, entrys, split_num * sizeof(IndexNodeEntry));
                    left_node->SetNumNodrain(split_num);
                    left_node->Flush();
                    BptreeIndexNode *right_node = AllocBptreeIndexNode();
                    right_node->SetEntryNodrain(0, &(entrys[split_num]), (entry_num - split_num) * sizeof(IndexNodeEntry));
                    right_node->SetNumNodrain(entry_num - split_num);
                    right_node->Flush();

                    op.free_indexnode_list.push_back(res.path[level - 1].node);
                    op.add_indexnode_list.push_back(NODE_GET_OFFSET(left_node));
                    op.add_indexnode_list.push_back(NODE_GET_OFFSET(right_node));

                    //更新index
                    delete[] entrys;
                    return BptreeIndexNodeUpdateTwoEntry(op, res, level - 1, NODE_GET_OFFSET(left_node), NODE_GET_OFFSET(right_node), father_index);
                }

                if(father_index > 0){  //和前面节点合并成两个节点
                    BptreeIndexNode *prev_node = static_cast<BptreeIndexNode *>(NODE_GET_POINTER(father->entry[father_index - 1].pointer));
                    uint32_t entry_num = remain_num + prev_node->num;
                    IndexNodeEntry *entrys = new IndexNodeEntry[entry_num];
                    memcpy(entrys, prev_node->entry, prev_node->num * sizeof(IndexNodeEntry));
                    uint32_t first_move = update_index;
                    if(first_move){
                        memcpy(&(entrys[prev_node->num]), cur->entry, update_index * sizeof(IndexNodeEntry));
                    }
                    entrys[prev_node->num + update_index].key = BptreeNodeGetMinkey(update);
                    entrys[prev_node->num + update_index].pointer = update;
                    uint32_t second_move = remain_num - 1 - update_index;
                    if(second_move > 0){
                        memmove(&(entrys[prev_node->num + update_index + 1]), &(cur->entry[update_index + 2]), second_move * sizeof(IndexNodeEntry));
                    }
                    
                    //进行分裂
                    uint32_t split_num = entry_num / 2;
                    BptreeIndexNode *left_node = AllocBptreeIndexNode();
                    left_node->SetEntryNodrain(0, entrys, split_num * sizeof(IndexNodeEntry));
                    left_node->SetNumNodrain(split_num);
                    left_node->Flush();
                    BptreeIndexNode *right_node = AllocBptreeIndexNode();
                    right_node->SetEntryNodrain(0, &(entrys[split_num]), (entry_num - split_num) * sizeof(IndexNodeEntry));
                    right_node->SetNumNodrain(entry_num - split_num);
                    right_node->Flush();

                    op.free_indexnode_list.push_back(res.path[level - 1].node);
                    op.add_indexnode_list.push_back(NODE_GET_OFFSET(left_node));
                    op.add_indexnode_list.push_back(NODE_GET_OFFSET(right_node));

                    //更新index
                    delete[] entrys;
                    return BptreeIndexNodeUpdateTwoEntry(op, res, level - 1, NODE_GET_OFFSET(left_node), NODE_GET_OFFSET(right_node), father_index - 1);
                }
            }
            //无需合并，直接操作
        }
        //无父节点，直接操作
    }
    //直接操作节点
    BptreeIndexNode *new_node = AllocBptreeIndexNode();
    new_node->CopyBy(cur);
    uint32_t need_move = remain_num - 1 - update_index;
    if(need_move) {
        new_node->SetEntryNodrain((update_index + 1) * sizeof(IndexNodeEntry), new_node->entry + update_index + 2, need_move * sizeof(IndexNodeEntry));
    }
    new_node->SetEntryNodrainByIndex(update_index, BptreeNodeGetMinkey(update), update);
    new_node->SetNumNodrain(remain_num);            
    new_node->Flush();

    op.free_indexnode_list.push_back(res.path[level - 1].node);
    op.add_indexnode_list.push_back(NODE_GET_OFFSET(new_node));

    //更新索引
    if(level > 1){  //存在父节点
        IndexNodeUpdateEntry(res, level - 1, NODE_GET_OFFSET(new_node));
    } else {  //当前节点是根节点
        op.res = NODE_GET_OFFSET(new_node);
    }
    return 0;
}

//Index节点只删除某一个Entry,
int BptreeIndexNodeDeleteEntry(BptreeOp &op, BptreeSearchResult &res, uint32_t level, uint32_t delete_index){
    BptreeIndexNode *cur = static_cast<BptreeIndexNode *>(NODE_GET_POINTER(res.path[level - 1].node));
    uint32_t remain_num = cur->num - 1;
    if(remain_num == 1){  //说明该节点只有一项的，删除层，
        if(level == 1){  //当前节点是根节点，删除层
            op.free_indexnode_list.push_back(res.path[level - 1].node);
            op.res = delete_index ? cur->entry[0].pointer : cur->entry[1].pointer;
            return 0;
        } else {
            assert(0);  //由于除了根节点，index节点不可能存在这种情况，因为entry过少会进行平衡 INDEX_NODE_TRIG_BALANCE_SIZE > 2;
            return 0;
        }
    }
    if(remain_num < INDEX_NODE_TRIG_MERGE_SIZE) {  //可能需要合并
        if(level > 1){  //存在父节点才可合并
            BptreeIndexNode *father = static_cast<BptreeIndexNode *>(NODE_GET_POINTER(res.path[level - 2].node));
            uint32_t father_index = res.path[level - 2].index;
            if(father_index != father->num - 1){  //优先和后节点合并
                BptreeIndexNode *next_node = static_cast<BptreeIndexNode *>(NODE_GET_POINTER(father->entry[father_index + 1].pointer));
                if(next_node->GetFreeSpace() >= remain_num){
                    BptreeIndexNode *new_node = AllocBptreeIndexNode();
                    new_node->CopyBy(cur);
                    uint32_t need_move = remain_num - 1 - delete_index;
                    if(need_move) {
                        new_node->SetEntryNodrain(delete_index * sizeof(IndexNodeEntry), new_node->entry + delete_index + 1, need_move * sizeof(IndexNodeEntry));
                    }
                    new_node->SetEntryNodrain(remain_num * sizeof(IndexNodeEntry), next_node->entry, next_node->num * sizeof(IndexNodeEntry));
                    new_node->SetNumNodrain(remain_num + next_node->num);            
                    new_node->Flush();
        
                    op.free_indexnode_list.push_back(res.path[level - 1].node);
                    op.free_indexnode_list.push_back(NODE_GET_OFFSET(next_node));
                    op.add_indexnode_list.push_back(NODE_GET_OFFSET(new_node));

                    return BptreeIndexNodeUpdateAndDelete(op, res, level - 1, NODE_GET_OFFSET(new_node), father_index);
                }
            }
            if(father_index > 0){  //和前节点合并
                BptreeIndexNode *prev_node = static_cast<BptreeIndexNode *>(NODE_GET_POINTER(father->entry[father_index - 1].pointer));
                if(prev_node->GetFreeSpace() >= remain_num){
                    BptreeIndexNode *new_node = AllocBptreeIndexNode();
                    new_node->CopyBy(prev_node);
                    uint32_t first_move = delete_index;
                    if(first_move){
                        new_node->SetEntryNodrain(new_node->num * sizeof(IndexNodeEntry), cur->entry, first_move * sizeof(IndexNodeEntry));
                    }
                    uint32_t second_move = remain_num - 1 - delete_index;
                    if(second_move){
                        new_node->SetEntryNodrain((new_node->num + delete_index) * sizeof(IndexNodeEntry), cur->entry + delete_index + 1, second_move * sizeof(IndexNodeEntry));
                    }
                    new_node->SetNumNodrain(new_node->num + remain_num);
                    new_node->Flush();

                    op.free_indexnode_list.push_back(res.path[level - 1].node);
                    op.free_indexnode_list.push_back(NODE_GET_OFFSET(prev_node));
                    op.add_indexnode_list.push_back(NODE_GET_OFFSET(new_node));

                    return BptreeIndexNodeUpdateAndDelete(op, res, level - 1, NODE_GET_OFFSET(new_node), father_index - 1);
                }
            }

            if(remain_num < INDEX_NODE_TRIG_BALANCE_SIZE){  //该节点无法和前后节点合并成单个节点，但是本身节点又太小，所以和左右节点合并成两个节点，平衡节点
                if(father_index != father->num - 1){  //优先和后面节点合并成两个节点
                    BptreeIndexNode *next_node = static_cast<BptreeIndexNode *>(NODE_GET_POINTER(father->entry[father_index + 1].pointer));
                    uint32_t entry_num = remain_num + next_node->num;
                    IndexNodeEntry *entrys = new IndexNodeEntry[entry_num];
                    memcpy(entrys, cur->entry, remain_num * sizeof(IndexNodeEntry));
                    uint32_t need_move = remain_num - 1 - delete_index;
                    if(need_move > 0){
                        memmove(&(entrys[delete_index]), &(entrys[delete_index + 1]), need_move * sizeof(IndexNodeEntry));
                    }
                    memcpy(&(entrys[remain_num]), next_node->entry, next_node->num * sizeof(IndexNodeEntry));
                    //进行分裂
                    uint32_t split_num = entry_num / 2;
                    BptreeIndexNode *left_node = AllocBptreeIndexNode();
                    left_node->SetEntryNodrain(0, entrys, split_num * sizeof(IndexNodeEntry));
                    left_node->SetNumNodrain(split_num);
                    left_node->Flush();
                    BptreeIndexNode *right_node = AllocBptreeIndexNode();
                    right_node->SetEntryNodrain(0, &(entrys[split_num]), (entry_num - split_num) * sizeof(IndexNodeEntry));
                    right_node->SetNumNodrain(entry_num - split_num);
                    right_node->Flush();

                    op.free_indexnode_list.push_back(res.path[level - 1].node);
                    op.add_indexnode_list.push_back(NODE_GET_OFFSET(left_node));
                    op.add_indexnode_list.push_back(NODE_GET_OFFSET(right_node));

                    //更新index
                    delete[] entrys;
                    return BptreeIndexNodeUpdateTwoEntry(op, res, level - 1, NODE_GET_OFFSET(left_node), NODE_GET_OFFSET(right_node), father_index);
                }

                if(father_index > 0){  //和前面节点合并成两个节点
                    BptreeIndexNode *prev_node = static_cast<BptreeIndexNode *>(NODE_GET_POINTER(father->entry[father_index - 1].pointer));
                    uint32_t entry_num = remain_num + prev_node->num;
                    IndexNodeEntry *entrys = new IndexNodeEntry[entry_num];
                    memcpy(entrys, prev_node->entry, prev_node->num * sizeof(IndexNodeEntry));
                    uint32_t first_move = delete_index;
                    if(first_move){
                        memcpy(&(entrys[prev_node->num]), cur->entry, delete_index * sizeof(IndexNodeEntry));
                    }
                    uint32_t second_move = remain_num - 1 - delete_index;
                    if(second_move > 0){
                        memmove(&(entrys[prev_node->num + delete_index]), &(cur->entry[delete_index + 1]), second_move * sizeof(IndexNodeEntry));
                    }
                    
                    //进行分裂
                    uint32_t split_num = entry_num / 2;
                    BptreeIndexNode *left_node = AllocBptreeIndexNode();
                    left_node->SetEntryNodrain(0, entrys, split_num * sizeof(IndexNodeEntry));
                    left_node->SetNumNodrain(split_num);
                    left_node->Flush();
                    BptreeIndexNode *right_node = AllocBptreeIndexNode();
                    right_node->SetEntryNodrain(0, &(entrys[split_num]), (entry_num - split_num) * sizeof(IndexNodeEntry));
                    right_node->SetNumNodrain(entry_num - split_num);
                    right_node->Flush();

                    op.free_indexnode_list.push_back(res.path[level - 1].node);
                    op.add_indexnode_list.push_back(NODE_GET_OFFSET(left_node));
                    op.add_indexnode_list.push_back(NODE_GET_OFFSET(right_node));

                    //更新index
                    delete[] entrys;
                    return BptreeIndexNodeUpdateTwoEntry(op, res, level - 1, NODE_GET_OFFSET(left_node), NODE_GET_OFFSET(right_node), father_index - 1);
                }
            }
            //无需合并，直接操作
        }
        //无父节点，直接操作
    }
    //直接操作节点
    BptreeIndexNode *new_node = AllocBptreeIndexNode();
    new_node->CopyBy(cur);
    uint32_t need_move = remain_num - 1 - delete_index;
    if(need_move) {
        new_node->SetEntryNodrain((delete_index) * sizeof(IndexNodeEntry), new_node->entry + delete_index + 1, need_move * sizeof(IndexNodeEntry));
    }
    new_node->SetNumNodrain(remain_num);            
    new_node->Flush();

    op.free_indexnode_list.push_back(res.path[level - 1].node);
    op.add_indexnode_list.push_back(NODE_GET_OFFSET(new_node));

    //更新索引
    if(level > 1){  //存在父节点
        IndexNodeUpdateEntry(res, level - 1, NODE_GET_OFFSET(new_node));
    } else {  //当前节点是根节点
        op.res = NODE_GET_OFFSET(new_node);
    }
    return 0;
}

int BptreeDelete(BptreeOp &op, const uint64_t hash_key, const Slice &fname){
    BptreeSearchResult res;
    BptreeSearch(res, op.root, hash_key);
    if(!res.key_find) {  //未找到，返回
        return 1;
    }
    BptreeLeafNode *leaf_node = static_cast<BptreeLeafNode *>(NODE_GET_POINTER(res.leaf_node));
    uint32_t del_len = 8 + 4 + res.leaf_value_len;
    uint32_t remain_len = leaf_node->len - del_len;
    if(remain_len != 0 && remain_len < LEAF_NODE_TRIG_MERGE_SIZE){   //需要合并
        if(res.index_level > 0){  //存在父节点才能合并
            BptreeIndexNode *father = static_cast<BptreeIndexNode *>(NODE_GET_POINTER(res.path[res.index_level - 1].node));
            uint32_t father_index = res.path[res.index_level - 1].index;
            if(father_index != father->num - 1){  //优先和后节点合并
                BptreeLeafNode *next_node = static_cast<BptreeLeafNode *>(NODE_GET_POINTER(father->entry[father_index + 1].pointer));
                if(next_node->GetFreeSpace() >= remain_len){  //空间足够，进行合并
                    BptreeLeafNode *new_node = AllocBptreeLeafNode();
                    new_node->CopyBy(leaf_node);
                    uint32_t need_move = new_node->len - res.leaf_key_offset - del_len;
                    if(need_move) {
                        new_node->SetBufNodrain(res.leaf_key_offset, new_node->buf + res.leaf_key_offset + del_len, need_move);
                    }
                    new_node->SetBufNodrain(remain_len, next_node->buf, next_node->len);
                    new_node->SetNumAndLenNodrain(leaf_node->num - 1 + next_node->num, remain_len + next_node->len);
                    new_node->SetNextNodrain(next_node->next);
                    new_node->Flush();
                    LeafNodeUpdateNextPrev(new_node);

                    op.free_leafnode_list.push_back(res.leaf_node);
                    op.free_leafnode_list.push_back(NODE_GET_OFFSET(next_node));
                    op.add_leafnode_list.push_back(NODE_GET_OFFSET(new_node));

                    return BptreeIndexNodeUpdateAndDelete(op, res, res.index_level, NODE_GET_OFFSET(new_node), father_index);
                }
            }
            if(father_index > 0){  //和前节点合并
                BptreeLeafNode *prev_node = static_cast<BptreeLeafNode *>(NODE_GET_POINTER(father->entry[father_index - 1].pointer));
                if(prev_node->GetFreeSpace() >= remain_len){  //空间足够，进行合并
                    BptreeLeafNode *new_node = AllocBptreeLeafNode();
                    new_node->CopyBy(prev_node);
                    uint32_t first_move = res.leaf_key_offset;  //叶节点删除，的前段迁移
                    if(first_move){
                        new_node->SetBufNodrain(new_node->len, leaf_node->buf, first_move);
                    }
                    uint32_t second_move = leaf_node->len - res.leaf_key_offset - del_len;
                    if(second_move) {
                        new_node->SetBufNodrain(new_node->len + first_move, leaf_node->buf + res.leaf_key_offset + del_len, second_move);
                    }
                    new_node->SetNumAndLenNodrain(new_node->num + leaf_node->num - 1, remain_len + new_node->len);
                    new_node->SetNextNodrain(leaf_node->next);
                    new_node->Flush();
                    LeafNodeUpdateNextPrev(new_node);

                    op.free_leafnode_list.push_back(res.leaf_node);
                    op.free_leafnode_list.push_back(NODE_GET_OFFSET(prev_node));
                    op.add_leafnode_list.push_back(NODE_GET_OFFSET(new_node));

                    return BptreeIndexNodeUpdateAndDelete(op, res, res.index_level, NODE_GET_OFFSET(new_node), father_index - 1);
                }
            }
            if(remain_len < LEAF_NODE_TRIG_BALANCE_SIZE) {  //该节点无法和前后节点合并成单个节点，但是本身节点又太小，所以和左右节点合并成两个节点，平衡节点
                if(father_index != father->num - 1){  //优先和后面节点合并成两个节点
                    BptreeLeafNode *next_node = static_cast<BptreeLeafNode *>(NODE_GET_POINTER(father->entry[father_index + 1].pointer));
                    char *buf = new char[remain_len + next_node->len];
                    memcpy(buf, leaf_node->buf, leaf_node->len);
                    uint32_t need_move = leaf_node->len - res.leaf_key_offset - del_len;
                    if(need_move){
                        memmove(buf + res.leaf_key_offset, buf + res.leaf_key_offset + del_len, need_move);
                    }
                    memcpy(buf + remain_len, next_node->buf, next_node->len);
                    vector<pointer_t> split_node;
                    BptreeLeafNodeSplit(buf, leaf_node->num - 1 + next_node->num, remain_len + next_node->len, split_node);
                    BptreeLeafNode *left_node = static_cast<BptreeLeafNode *>(NODE_GET_POINTER(split_node[0]));
                    BptreeLeafNode *right_node = static_cast<BptreeLeafNode *>(NODE_GET_POINTER(split_node[1]));
                    if(!IS_INVALID_POINTER(next_node->next)){
                        right_node->SetNextNodrain(next_node->next);
                    }
                    if(!IS_INVALID_POINTER(leaf_node->prev)){
                        left_node->SetPrevNodrain(leaf_node->prev);
                    }
                    left_node->Flush();
                    right_node->Flush();

                    if(!IS_INVALID_POINTER(next_node->next)){
                        BptreeLeafNode *next_next_node = static_cast<BptreeLeafNode *>(NODE_GET_POINTER(next_node->next));
                        next_next_node->SetPrevPersist(split_node[1]);
                    }
                    if(!IS_INVALID_POINTER(leaf_node->prev)){
                        BptreeLeafNode *prev_node = static_cast<BptreeLeafNode *>(NODE_GET_POINTER(leaf_node->prev));
                        prev_node->SetNextPersist(split_node[0]);
                    }

                    op.free_leafnode_list.push_back(res.leaf_node);
                    op.free_leafnode_list.push_back(NODE_GET_OFFSET(next_node));
                    op.add_leafnode_list.push_back(split_node[0]);
                    op.add_leafnode_list.push_back(split_node[1]);

                    //更新index
                    delete[] buf;
                    return BptreeIndexNodeUpdateTwoEntry(op, res, res.index_level, split_node[0], split_node[1], father_index);
                }

                if(father_index > 0){  //和前面节点合并成两个节点
                    BptreeLeafNode *prev_node = static_cast<BptreeLeafNode *>(NODE_GET_POINTER(father->entry[father_index - 1].pointer));
                    char *buf = new char[remain_len + prev_node->len];
                    memcpy(buf, prev_node->buf, prev_node->len);
                    uint32_t first_move = res.leaf_key_offset;
                    if(first_move){
                        memcpy(buf + prev_node->len, leaf_node->buf, first_move);
                    }
                    uint32_t second_move = leaf_node->len - res.leaf_key_offset - del_len;
                    if(second_move) {
                        memcpy(buf + prev_node->len + first_move, leaf_node->buf + res.leaf_key_offset + del_len, second_move);
                    }
                    vector<pointer_t> split_node;
                    BptreeLeafNodeSplit(buf, leaf_node->num - 1 + prev_node->num, remain_len + prev_node->len, split_node);
                    BptreeLeafNode *left_node = static_cast<BptreeLeafNode *>(NODE_GET_POINTER(split_node[0]));
                    BptreeLeafNode *right_node = static_cast<BptreeLeafNode *>(NODE_GET_POINTER(split_node[1]));
                    if(!IS_INVALID_POINTER(leaf_node->next)){
                        right_node->SetNextNodrain(leaf_node->next);
                    }
                    if(!IS_INVALID_POINTER(prev_node->prev)){
                        left_node->SetPrevNodrain(prev_node->prev);
                    }
                    left_node->Flush();
                    right_node->Flush();

                    if(!IS_INVALID_POINTER(leaf_node->next)){
                        BptreeLeafNode *next_next_node = static_cast<BptreeLeafNode *>(NODE_GET_POINTER(leaf_node->next));
                        next_next_node->SetPrevPersist(split_node[1]);
                    }
                    if(!IS_INVALID_POINTER(prev_node->prev)){
                        BptreeLeafNode *prev_prev_node = static_cast<BptreeLeafNode *>(NODE_GET_POINTER(prev_node->prev));
                        prev_prev_node->SetNextPersist(split_node[0]);
                    }

                    op.free_leafnode_list.push_back(res.leaf_node);
                    op.free_leafnode_list.push_back(NODE_GET_OFFSET(prev_node));
                    op.add_leafnode_list.push_back(split_node[0]);
                    op.add_leafnode_list.push_back(split_node[1]);

                    //更新index
                    delete[] buf;
                    return BptreeIndexNodeUpdateTwoEntry(op, res, res.index_level, split_node[0], split_node[1], father_index - 1);
                }

            }

            //无法合并，直接操作
        } 

        //不存在父节点，直接操作
    }
    if(remain_len == 0){  //删除节点
        if(res.index_level == 0){  //根节点也删除
            op.free_leafnode_list.push_back(res.leaf_node);
            op.res = INVALID_POINTER;
            return 0;
        } else {  //存在父节点
            op.free_leafnode_list.push_back(res.leaf_node);
            return BptreeIndexNodeDeleteEntry(op, res, res.index_level, res.path[res.index_level - 1].index);
        }
    }
    
    //直接操作
    if((res.leaf_key_offset + 8 + 4 + res.leaf_value_len) == leaf_node->len){  //删除的key是最后一个key，直接删除
        leaf_node->SetNumAndLenPersist(leaf_node->num - 1, remain_len);
        return 0;
    }
    //需要新创节点，删除一个key
    BptreeLeafNode *new_node = AllocBptreeLeafNode();
    new_node->CopyBy(leaf_node);
    uint32_t need_move = new_node->len - res.leaf_key_offset - del_len;
    new_node->SetBufNodrain(res.leaf_key_offset, new_node->buf + res.leaf_key_offset + del_len, need_move);
    new_node->SetNumAndLenNodrain(new_node->num - 1, remain_len);
    new_node->Flush();
    LeafNodeUpdateNextPrev(new_node);

    op.free_leafnode_list.push_back(res.leaf_node);
    op.add_leafnode_list.push_back(NODE_GET_OFFSET(new_node));

    //更新索引
    if(res.index_level > 0){
        IndexNodeUpdateEntry(res, res.index_level, NODE_GET_OFFSET(new_node));
    } else { //该节点时根节点，则更新根节点
        op.res = NODE_GET_OFFSET(new_node);
    }

    return 0;
}

int BptreeOnlyInsert(BptreeOp &op, const uint64_t hash_key, const Slice &fname, const inode_id_t value){
    BptreeSearchResult res;
    BptreeSearch(res, op.root, hash_key);
    if(res.key_find){ //key存在
        return 2;
    }
    return BptreeInsert(op, hash_key, fname, value);
}

int BptreeGetLinkHeadNode(pointer_t root, pointer_t &head){
    pointer_t cur = root;
    while(!IS_INVALID_POINTER(cur)){
        if(IsIndexNode(cur)){  //中间节点查找
            BptreeIndexNode *cur_node = static_cast<BptreeIndexNode *>(NODE_GET_POINTER(cur));
            cur = cur_node->entry[0].pointer;
        }
        else{    //叶子节点查找
            head = cur;
            return 0;
        }
    }
    return 0;
}

Iterator* LinkListGetIterator(LinkNode *root_node, const inode_id_t target){
    pointer_t cur = NODE_GET_OFFSET(root_node);
    pointer_t prev = cur;

    LinkNode *cur_node = root_node;
    while(!IS_INVALID_POINTER(cur) && compare_inode_id(target, cur_node->min_key) >= 0) {
        prev = cur;
        cur = cur_node->next;
        cur_node = static_cast<LinkNode *>(NODE_GET_POINTER(cur));
    }
    pointer_t search = prev;  //在该节点查找kv；
    LinkNode *search_node = static_cast<LinkNode *>(NODE_GET_POINTER(search));

    LinkNodeSearchResult res;
    LinkNodeSearchKey(res, search_node, target);
    if(!res.key_find) return nullptr;

    if(res.value_is_bptree){  //bptree
        pointer_t bptree = search_node->DecodeBufGetBptree(res.key_offset + sizeof(inode_id_t) + 4);
        pointer_t head = INVALID_POINTER;
        BptreeGetLinkHeadNode(bptree, head);
        if(IS_INVALID_POINTER(head)) return nullptr;
        BptreeLeafNode *head_node = static_cast<BptreeLeafNode *>(NODE_GET_POINTER(head));

        return new BptreeIterator(head_node);
    }
    //linklist
    return new LinkNodeIterator(search, res.key_offset, res.key_num, res.key_len);
}

//////
char toHex(unsigned char v) {
    if (v <= 9) {
        return '0' + v;
    }
    return 'A' + v - 10;
}
string BufTranToHex(const char *buf, uint32_t len){  //将buf转成数字16进制，方便打印查看
    string res;
    for(uint32_t i = 0; i < len; i++){
        unsigned char c = buf[i];
        res.push_back(toHex(c & 0xf));
        res.push_back(toHex(c >> 4));
    }
    return res;
}
void printBptree(pointer_t root){
    if(IS_INVALID_POINTER(root)) return;
    DBG_LOG("[dir] Bptree: root:%lu", root);
    pointer_t cur = root;
    uint32_t level = 0;
    uint32_t size = 0;
    queue<pointer_t> queues;
    queues.push(cur);
    while(!queues.empty() && !IS_INVALID_POINTER(queues.front())){
        size = queues.size();
        DBG_LOG("[dir] level:%u size:%u", level, size);
        for(uint32_t i = 0; i < size; i++){
            cur = queues.front();
            if(IsIndexNode(cur)){  //中间节点查找
                BptreeIndexNode *cur_node = static_cast<BptreeIndexNode *>(NODE_GET_POINTER(cur));
                string str;
                char buf[100];
                for(uint32_t j = 0; j < cur_node->num; j++){
                    snprintf(buf, 100, "[%u %lx %lu]", j, cur_node->entry[j].key, cur_node->entry[j].pointer);
                    str.append(buf, strlen(buf));
                    queues.push(cur_node->entry[j].pointer);
                }
                DBG_LOG("[dir] level:%u %3u index node:%6lu num:%u value:%s", level, i, cur, cur_node->num, str.c_str());
            }
            else{    //叶子节点查找
                BptreeLeafNode *cur_node = static_cast<BptreeLeafNode *>(NODE_GET_POINTER(cur));
                DBG_LOG("[dir] level:%u %3u leaf node:%6lu num:%u len:%u prev:%lu next:%lu", level, i, cur, cur_node->num, \
                    cur_node->len, cur_node->prev, cur_node->next);
                uint64_t temp_key;
                uint32_t len;
                uint32_t offset = 0;
                for(uint32_t j = 0; j < cur_node->num; j++){
                    cur_node->DecodeBufGetKeyValuelen(offset, temp_key, len);
                    DBG_LOG("[dir] level:%u %3u leaf node:%6lu %02u key:%016lx len:%u value:%.*s", level, i, cur, j, temp_key, len, len - sizeof(inode_id_t), cur_node->buf + offset + 8 + 4);
                    offset += (8 + 4 + len);
                }
            }
            queues.pop();
        }
        level++;
    }

}

void PrintLinkList(pointer_t root){
    if(IS_INVALID_POINTER(root)) return;
    pointer_t cur = root;

    while(!IS_INVALID_POINTER(cur)) {
        LinkNode *cur_node = static_cast<LinkNode *>(NODE_GET_POINTER(cur));
        DBG_LOG("[dir] linknode:%lu num:%u len:%u min:%lu max:%lu prev:%lu next:%lu", cur, cur_node->num, \
            cur_node->len, cur_node->min_key, cur_node->max_key, cur_node->prev, cur_node->next);
        inode_id_t key;
        uint32_t key_num, key_len;
        uint32_t offset = 0;
        for(uint32_t i = 0; i < cur_node->num; i++){
            cur_node->DecodeBufGetKeyNumLen(offset, key, key_num, key_len);
            if(key_num == 0){
                DBG_LOG("[dir] i:%u key:%lu btree:%lu", i, key, cur_node->DecodeBufGetBptree(offset + sizeof(inode_id_t) + 4));
                pointer_t bptree = cur_node->DecodeBufGetBptree(offset + sizeof(inode_id_t) + 4);
                printBptree(bptree);
                offset += sizeof(inode_id_t) + 4 + 8;
            } 
            else {
                string kvs = BufTranToHex(cur_node->buf + offset + sizeof(inode_id_t) + 8, key_len);
                DBG_LOG("[dir] i:%u key:%lu key_num:%u key_len:%u kvs:%.*s", i, key, key_num, key_len, kvs.size(), kvs.c_str());
                offset += sizeof(inode_id_t) + 8 + key_len;
            }
        }
        if (offset != cur_node->len) throw -10;
        cur = cur_node->next;
    }
}

void GetBptreeStats(pointer_t root, uint64_t &index_node_nums, uint64_t &leaf_node_nums){
    if(IS_INVALID_POINTER(root)) return;
    pointer_t cur = root;
    queue<pointer_t> queues;
    queues.push(cur);
    while(!queues.empty() && !IS_INVALID_POINTER(queues.front())){
        cur = queues.front();
        if(IsIndexNode(cur)){  //中间节点
            BptreeIndexNode *cur_node = static_cast<BptreeIndexNode *>(NODE_GET_POINTER(cur));
            for(uint32_t i = 0; i < cur_node->num; i++){
                queues.push(cur_node->entry[i].pointer);
            }
            index_node_nums++;
        }
        else{    //叶子节点
            leaf_node_nums++;
        }
        queues.pop();
    }
}

void GetLinkListStats(pointer_t root, uint64_t &link_node_nums, uint64_t &index_node_nums, uint64_t &leaf_node_nums){
    if(IS_INVALID_POINTER(root)) return;
    pointer_t cur = root;

    while(!IS_INVALID_POINTER(cur)) {
        LinkNode *cur_node = static_cast<LinkNode *>(NODE_GET_POINTER(cur));
        
        inode_id_t key;
        uint32_t key_num, key_len;
        uint32_t offset = 0;
        for(uint32_t i = 0; i < cur_node->num; i++){
            cur_node->DecodeBufGetKeyNumLen(offset, key, key_num, key_len);
            if(key_num == 0){
                pointer_t bptree = cur_node->DecodeBufGetBptree(offset + sizeof(inode_id_t) + 4);
                GetBptreeStats(bptree, index_node_nums, leaf_node_nums);
                offset += sizeof(inode_id_t) + 4 + 8;
            } 
        }
        link_node_nums++;
        cur = cur_node->next;
    }
}

} // namespace name