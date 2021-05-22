/**
 * @Author      : Liu Zhiwen
 * @Create Date : 2020-12-17 16:18:09
 * @Contact     : 993096281@qq.com
 * @Description : 
 */
#ifndef _METADB_NVM_NODE_H_
#define _METADB_NVM_NODE_H_

#include <vector>
#include "format.h"
#include "metadb/slice.h"
#include "metadb/inode.h"
#include "metadb/iterator.h"
#include "nvm_node_allocator.h"

using namespace std;

namespace metadb {

//Bptree 划分范围：key1，pointer1；key2，pointer2；  pointer1 -> key1 <= x < key2;   pointer2 -> key2 <= x <= key3
struct IndexNodeEntry {
    uint64_t key;
    pointer_t pointer;

    IndexNodeEntry() : key(0), pointer(INVALID_POINTER) {}
    ~IndexNodeEntry() {}
};

static const uint32_t LINK_NODE_CAPACITY = DIR_LINK_NODE_SIZE - 40;
static const uint32_t INDEX_NODE_CAPACITY = (DIR_BPTREE_INDEX_NODE_SIZE - 16) / sizeof(IndexNodeEntry);
static const uint32_t LEAF_NODE_CAPACITY = DIR_BPTREE_LEAF_NODE_SIZE - 24;

static const uint32_t LINK_NODE_TRIG_MERGE_SIZE = (LINK_NODE_CAPACITY / 2) - 44;  //LinkNode 触发merge的大小; 减去的是一个KV的长度（假设fname为8）
static const uint32_t LEAF_NODE_TRIG_MERGE_SIZE = (LEAF_NODE_CAPACITY / 2) - 28; //leaf_node 触发merge的大小；减去的28是一个kv长度（假设fname为8）
static const uint32_t LEAF_NODE_TRIG_BALANCE_SIZE = LEAF_NODE_CAPACITY / 4; //叶节点无法和左右节点合并， 但是小于该值时应该和左右节点平衡，即和左右节点合并成两个节点
static const uint32_t INDEX_NODE_TRIG_MERGE_SIZE = (INDEX_NODE_CAPACITY / 2) - 1;  //中间节点触发合并
static const uint32_t INDEX_NODE_TRIG_BALANCE_SIZE = (INDEX_NODE_CAPACITY + 3) / 4; //中间节点无法和左右节点合并， 但是小于该值时应该和左右节点平衡，即和左右节点合并成两个节点, +3单纯因为向上取整

enum class DirNodeType : uint8_t {
    UNKNOWN_TYPE = 0,
    LINKNODE_TYPE = 1,
    BPTREEINDEXNODE_TYPE,  //bptree 中间节点
    BPTREELEAFNODE_TYPE,   //bptree 叶节点
    //BPTREEROOTINDEXNODE_TYPE,  //bptree 即是root节点， 也是中间节点
   // BPTREEROOTLEAFNODE_TYPE,   //bptree 即是root节点， 也是叶节点
};

struct LinkNode {
    uint8_t type;
    uint8_t padding;
    uint16_t num;
    uint32_t len; //开头一共8B
    inode_id_t min_key;
    inode_id_t max_key;
    pointer_t prev;    //头结点，prev为空，
    pointer_t next;    //尾结点，next为空
    char buf[LINK_NODE_CAPACITY];       //  key:fname1,fname2...key2:fname1,fname2: |----key(8B)---|---num(4B)---|----len(4B)----|---hash(fname1)(8B)---|--value_len(4B)--|--value()--|
                                        //                        |---hash(fname1)(8B)---|--value_len(4B)--|--value---|...
                                        //  key:Bptree:    |----key(8B)---|---num(4B) = 0 ---|---b+tree_pointer(8B)---|
                                        // num == 0 不可能的情况，所以代表指向一棵Bptree
    LinkNode() {}
    ~LinkNode() {}

    void CopyBy(LinkNode * dst){
        node_allocator->nvm_memcpy_nodrain(this, dst, sizeof(LinkNode));
    }

    uint32_t GetFreeSpace() {
        return LINK_NODE_CAPACITY - len;
    }

    void DecodeBufGetKey(uint32_t offset, inode_id_t &key){
        key = *reinterpret_cast<inode_id_t *>(buf + offset);
    }

    void DecodeBufGetKeyNumLen(uint32_t offset, inode_id_t &key, uint32_t &key_num, uint32_t &key_len){
        key = *reinterpret_cast<inode_id_t *>(buf + offset);
        key_num = *reinterpret_cast<uint32_t *>(buf + offset + sizeof(inode_id_t));
        key_len = *reinterpret_cast<uint32_t *>(buf + offset + sizeof(inode_id_t) + 4);
    }

    void DecodeBufGetHashfnameAndLen(uint32_t offset, uint64_t &hash_fname, uint32_t &value_len){
        hash_fname = *reinterpret_cast<uint64_t *>(buf + offset);
        value_len = *reinterpret_cast<uint32_t *>(buf + offset + 8);
    }

    pointer_t DecodeBufGetBptree(uint32_t offset) {
        return *reinterpret_cast<pointer_t *>(buf + offset);
    }

    void Flush(){
        node_allocator->nvm_persist(this, sizeof(LinkNode));
    }

    void SetTypeNodrain(DirNodeType t){
        node_allocator->nvm_memcpy_nodrain(&type, &t, 1);
    }
    void SetTypePersist(DirNodeType t){
        node_allocator->nvm_memcpy_persist(&type, &t, 1);
    }

    void SetNumAndLenPersist(uint16_t a, uint32_t b){
        char buff[6];
        memcpy(buff, &a, 2);
        memcpy(buff + 2, &b, 4);
        node_allocator->nvm_memmove_persist(&num, buff, 6);
    }

    void SetNumAndLenNodrain(uint16_t a, uint32_t b){
        char buff[6];
        memcpy(buff, &a, 2);
        memcpy(buff + 2, &b, 4);
        node_allocator->nvm_memcpy_nodrain(&num, buff, 6);
    }

    void SetMinkeyPersist(inode_id_t key){
        node_allocator->nvm_memcpy_persist(&min_key, &key, sizeof(inode_id_t));
    }
    void SetMinkeyNodrain(inode_id_t key){
        node_allocator->nvm_memcpy_nodrain(&min_key, &key, sizeof(inode_id_t));
    }

    void SetMaxkeyPersist(inode_id_t key){
        node_allocator->nvm_memcpy_persist(&max_key, &key, sizeof(inode_id_t));
    }
    void SetMaxkeyNodrain(inode_id_t key){
        node_allocator->nvm_memcpy_nodrain(&max_key, &key, sizeof(inode_id_t));
    }

    void SetPrevPersist(pointer_t ptr){
        node_allocator->nvm_memcpy_persist(&prev, &ptr, sizeof(pointer_t));
    }
    void SetPrevNodrain(pointer_t ptr){
        node_allocator->nvm_memcpy_nodrain(&prev, &ptr, sizeof(pointer_t));
    }

    void SetNextPersist(pointer_t ptr){
        node_allocator->nvm_memcpy_persist(&next, &ptr, sizeof(pointer_t));
    }
    void SetNextNodrain(pointer_t ptr){
        node_allocator->nvm_memcpy_nodrain(&next, &ptr, sizeof(pointer_t));
    }

    void SetBufPersist(uint32_t offset, const void *ptr, uint32_t len){
        node_allocator->nvm_memmove_persist(buf + offset, ptr, len);
    }
    void SetBufNodrain(uint32_t offset, const void *ptr, uint32_t len){
        node_allocator->nvm_memmove_nodrain(buf + offset, ptr, len);
    }


};

struct BptreeIndexNode {
    uint8_t type;
    uint8_t padding;
    uint16_t num;
    uint32_t len;    //len没用
    uint64_t magic_number; //暂时没用，充当padding；
    //pointer_t prev;
    //pointer_t next;
    IndexNodeEntry entry[INDEX_NODE_CAPACITY];

    BptreeIndexNode() {}
    ~BptreeIndexNode() {}

    void CopyBy(BptreeIndexNode * dst){
        node_allocator->nvm_memcpy_nodrain(this, dst, sizeof(BptreeIndexNode));
    }

    uint32_t GetFreeSpace() {
        return INDEX_NODE_CAPACITY - num;
    }

    void Flush(){
        node_allocator->nvm_persist(this, sizeof(BptreeIndexNode));
    }

    void SetTypeNodrain(DirNodeType t){
        node_allocator->nvm_memcpy_nodrain(&type, &t, 1);
    }
    void SetTypePersist(DirNodeType t){
        node_allocator->nvm_memcpy_persist(&type, &t, 1);
    }

    void SetNumPersist(uint16_t a){
        node_allocator->nvm_memmove_persist(&num, &a, 2);
    }

    void SetNumNodrain(uint16_t a){
        node_allocator->nvm_memcpy_nodrain(&num, &a, 2);
    }

    void SetEntryPersist(uint32_t offset, const void *ptr, uint32_t len){
        void *buf = reinterpret_cast<char *>(entry) + offset;
        node_allocator->nvm_memmove_persist(buf, ptr, len);
    }
    void SetEntryNodrain(uint32_t offset, const void *ptr, uint32_t len){
        void *buf = reinterpret_cast<char *>(entry) + offset;
        node_allocator->nvm_memmove_nodrain(buf, ptr, len);
    }

    void SetEntryPersistByIndex(uint32_t index, uint64_t key, pointer_t ptr){
        void *buf = reinterpret_cast<char *>(entry) + index * sizeof(IndexNodeEntry);
        char insert_buf[sizeof(IndexNodeEntry)];
        memcpy(insert_buf, &key, 8);
        memcpy(insert_buf + 8, &ptr, sizeof(pointer_t));
        node_allocator->nvm_memcpy_persist(buf, insert_buf, sizeof(IndexNodeEntry));
    }
    void SetEntryNodrainByIndex(uint32_t index, uint64_t key, pointer_t ptr){
        void *buf = reinterpret_cast<char *>(entry) + index * sizeof(IndexNodeEntry);
        char insert_buf[sizeof(IndexNodeEntry)];
        memcpy(insert_buf, &key, 8);
        memcpy(insert_buf + 8, &ptr, sizeof(pointer_t));
        node_allocator->nvm_memcpy_nodrain(buf, insert_buf, sizeof(IndexNodeEntry));
    }

};

struct BptreeLeafNode {
    uint8_t type;
    uint8_t padding;
    uint16_t num;
    uint32_t len;
    pointer_t prev;
    pointer_t next;
    char buf[LEAF_NODE_CAPACITY];      //key1,value1,key2,value2:   |--key1(8B)--|--value_len(4B)--|--value()--|--key2(8B)--|--value_len(4B)--|--value()--|

    BptreeLeafNode() {}
    ~BptreeLeafNode() {}

    void CopyBy(BptreeLeafNode * dst){
        node_allocator->nvm_memcpy_nodrain(this, dst, sizeof(BptreeLeafNode));
    }

    uint32_t GetFreeSpace() {
        return LEAF_NODE_CAPACITY - len;
    }

    void DecodeBufGetKeyValuelen(uint32_t offset, uint64_t &key, uint32_t &value_len){
        key = *reinterpret_cast<uint64_t *>(buf + offset);
        value_len = *reinterpret_cast<uint32_t *>(buf + offset + 8);
    }

    uint64_t GetMinKey(){
        uint64_t min_key = *reinterpret_cast<uint64_t *>(buf);
        return min_key;
    }


    void Flush(){
        node_allocator->nvm_persist(this, sizeof(BptreeLeafNode));
    }

    void SetTypeNodrain(DirNodeType t){
        node_allocator->nvm_memcpy_nodrain(&type, &t, 1);
    }
    void SetTypePersist(DirNodeType t){
        node_allocator->nvm_memcpy_persist(&type, &t, 1);
    }

    void SetNumAndLenPersist(uint16_t a, uint32_t b){
        char buff[6];
        memcpy(buff, &a, 2);
        memcpy(buff + 2, &b, 4);
        node_allocator->nvm_memmove_persist(&num, buff, 6);
    }

    void SetNumAndLenNodrain(uint16_t a, uint32_t b){
        char buff[6];
        memcpy(buff, &a, 2);
        memcpy(buff + 2, &b, 4);
        node_allocator->nvm_memcpy_nodrain(&num, buff, 6);
    }

    void SetPrevPersist(pointer_t ptr){
        node_allocator->nvm_memcpy_persist(&prev, &ptr, sizeof(pointer_t));
    }
    void SetPrevNodrain(pointer_t ptr){
        node_allocator->nvm_memcpy_nodrain(&prev, &ptr, sizeof(pointer_t));
    }

    void SetNextPersist(pointer_t ptr){
        node_allocator->nvm_memcpy_persist(&next, &ptr, sizeof(pointer_t));
    }
    void SetNextNodrain(pointer_t ptr){
        node_allocator->nvm_memcpy_nodrain(&next, &ptr, sizeof(pointer_t));
    }

    void SetBufPersist(uint32_t offset, const void *ptr, uint32_t len){
        node_allocator->nvm_memmove_persist(buf + offset, ptr, len);
    }
    void SetBufNodrain(uint32_t offset, const void *ptr, uint32_t len){
        node_allocator->nvm_memmove_nodrain(buf + offset, ptr, len);
    }
};

class BptreeOp {
public:
    pointer_t root;
    pointer_t res;

    vector<pointer_t> add_indexnode_list;
    vector<pointer_t> free_indexnode_list;
    vector<pointer_t> add_leafnode_list;
    vector<pointer_t> free_leafnode_list;

    BptreeOp() : root(INVALID_POINTER) , res(INVALID_POINTER) {
        add_indexnode_list.reserve(6);
        free_indexnode_list.reserve(6);
        add_leafnode_list.reserve(3);
        free_leafnode_list.reserve(3);
    }   
    ~BptreeOp() {}
};

////// LinkList 操作
struct LinkListOp {
    pointer_t root;   //输入的root节点
    vector<pointer_t> add_linknode_list;
    vector<pointer_t> free_linknode_list;   //待删除的节点

    vector<pointer_t> add_indexnode_list;
    vector<pointer_t> free_indexnode_list;
    vector<pointer_t> add_leafnode_list;
    vector<pointer_t> free_leafnode_list;
    
    pointer_t res;  //返回的节点

    LinkListOp() : root(INVALID_POINTER), res(INVALID_POINTER) {
        add_linknode_list.reserve(3); //猜测大部分不超过3，
        free_linknode_list.reserve(3);   //猜测大部分不超过3，

        add_indexnode_list.reserve(6);
        free_indexnode_list.reserve(6);
        add_leafnode_list.reserve(3);
        free_leafnode_list.reserve(3);
    }
    void AddBptreeOp(BptreeOp &op){
        add_indexnode_list.insert(add_indexnode_list.end(), op.add_indexnode_list.begin(), op.add_indexnode_list.end());
        free_indexnode_list.insert(free_indexnode_list.end(), op.free_indexnode_list.begin(), op.free_indexnode_list.end());
        add_leafnode_list.insert(add_leafnode_list.end(), op.add_leafnode_list.begin(), op.add_leafnode_list.end());
        free_leafnode_list.insert(free_leafnode_list.end(), op.free_leafnode_list.begin(), op.free_leafnode_list.end());
    }
    ~LinkListOp() {}
};

LinkNode *AllocLinkNode();
int LinkListInsert(LinkListOp &op, const inode_id_t key, const Slice &fname, const inode_id_t value);
int LinkListGet(LinkNode *root, const inode_id_t key, const Slice &fname, inode_id_t &value);
int LinkListDelete(LinkListOp &op, const inode_id_t key, const Slice &fname);

//将kvs转成节点，并返回根节点，节点个数
int MemoryTranToNVMLinkNode(vector<string> &kvs, pointer_t &root, uint32_t &node_num);  //link 转成bptree时调用
int RehashLinkListInsert(LinkListOp &op, const inode_id_t key, string &kvs);   //rehash时迁移kvs，kvs可能是一个key，但是多个fname
//////

////// bptree 操作
BptreeIndexNode *AllocBptreeIndexNode();
BptreeLeafNode *AllocBptreeLeafNode();
int BptreeInsert(BptreeOp &op, const uint64_t hash_key, const Slice &fname, const inode_id_t value);
int BptreeGet(pointer_t root, const uint64_t hash_key, const Slice &fname, inode_id_t &value);
int BptreeDelete(BptreeOp &op, const uint64_t hash_key, const Slice &fname);
int BptreeGetLinkHeadNode(pointer_t root, pointer_t &head);
int BptreeOnlyInsert(BptreeOp &op, const uint64_t hash_key, const Slice &fname, const inode_id_t value);  //只插入，若已存在，则不修改

bool IsIndexNode(pointer_t ptr);
//////


Iterator* LinkListGetIterator(LinkNode *root_node, const inode_id_t target);


//////
void PrintLinkList(pointer_t root);
void GetLinkListStats(pointer_t root, uint64_t &link_node_nums, uint64_t &index_node_nums, uint64_t &leaf_node_num);

} // namespace name







#endif