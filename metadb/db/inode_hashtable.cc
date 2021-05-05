/**
 * @Author      : Liu Zhiwen
 * @Create Date : 2021-01-05 14:52:25
 * @Contact     : 993096281@qq.com
 * @Description : 
 */
#include "inode_hashtable.h"
#include "metadb/debug.h"
#include "inode_zone.h"

namespace metadb {

///// link 操作
struct InodeHashEntrySearchResult {
    bool key_find;
    uint16_t index;
    pointer_t node;

    InodeHashEntrySearchResult() : key_find(false), index(0), node(INVALID_POINTER) {}
    ~InodeHashEntrySearchResult() {}
};

NvmInodeHashEntryNode *AllocHashEntryNode(){
    NvmInodeHashEntryNode *node = static_cast<NvmInodeHashEntryNode *>(node_allocator->AllocateAndInit(INODE_HASH_ENTRY_SIZE, 0));
    return node;
}

//将slot的index位置设为1
static inline uint16_t slot_set_index(uint16_t slot, uint16_t index){
    return slot | (1 << index);
}

//将slot的index位置设为0
static inline uint16_t slot_clx_index(uint16_t slot, uint16_t index){
    return slot & (~(1 << index));
}

//获取slot的index位置的值
static inline bool slot_get_index(uint16_t slot, uint16_t index){
    return (slot >> index) & 1;
}

void HashEntryLinkSearchKey(InodeHashEntrySearchResult &res, pointer_t root, const inode_id_t key){
    pointer_t cur = root;
    NvmInodeHashEntryNode *cur_node;
    while(!IS_INVALID_POINTER(cur)) {
        cur_node = static_cast<NvmInodeHashEntryNode *>(NODE_GET_POINTER(cur));
        for(uint16_t i = 0; i < INODE_HASH_ENTRY_NODE_CAPACITY; i++){
            if(!slot_get_index(cur_node->slot, i)) continue;
            if(compare_inode_id(cur_node->entry[i].key, key) == 0){
                res.key_find = true;
                res.index = i;
                res.node = cur;
                return ;
            }
        }
        cur = cur_node->next;
    }
}

//找到一个空位，或者新创一个节点，生成空位，返回ret，index
void FindFreeSpaceOrCreatEntry(InodeHashEntryLinkOp &op, pointer_t &ret, uint16_t &index){
    //root不能为空
    pointer_t cur = op.root;
    NvmInodeHashEntryNode *cur_node;
    pointer_t prev = cur;
    while(!IS_INVALID_POINTER(cur)) {
        cur_node = static_cast<NvmInodeHashEntryNode *>(NODE_GET_POINTER(cur));
        if(cur_node->GetFreeSpace() > 0){
            for(uint16_t i = 0; i < INODE_HASH_ENTRY_NODE_CAPACITY; i++){
                if(!slot_get_index(cur_node->slot, i)) {
                    ret = cur;
                    index = i;
                    return ;
                }
            }
        }
        prev = cur;
        cur = cur_node->next;
    }
    //新建节点
    NvmInodeHashEntryNode *new_node = AllocHashEntryNode();
    ret = NODE_GET_OFFSET(new_node);
    index = 0;
    new_node->SetPrevNodrain(prev);
    new_node->Flush();
    if(!IS_INVALID_POINTER(prev)){
        NvmInodeHashEntryNode *prev_node = static_cast<NvmInodeHashEntryNode *>(NODE_GET_POINTER(prev));
        prev_node->SetNextPersist(NODE_GET_OFFSET(new_node));
    } else {  //这是新根节点
        op.res = NODE_GET_OFFSET(new_node);
    }
    op.add_list.push_back(NODE_GET_OFFSET(new_node));
}

int InodeHashEntryLinkInsert(InodeHashEntryLinkOp &op, const inode_id_t key, const pointer_t value, pointer_t &old_value){
    InodeHashEntrySearchResult res;
    HashEntryLinkSearchKey(res, op.root, key);
    if(res.key_find){  //已存在
        NvmInodeHashEntryNode *node = static_cast<NvmInodeHashEntryNode *>(NODE_GET_POINTER(res.node));
        old_value = node->entry[res.index].pointer;
        node->SetEntryPointerPersist(res.index, value);
        return 2;
    }
    pointer_t insert;
    uint16_t index;
    FindFreeSpaceOrCreatEntry(op, insert, index);
    NvmInodeHashEntryNode *insert_node = static_cast<NvmInodeHashEntryNode *>(NODE_GET_POINTER(insert));
    insert_node->SetEntryPersistByIndex(index, key,value);
    uint16_t slot = slot_set_index(insert_node->slot, index);
    insert_node->SetNumAndSlotPersist(insert_node->num + 1, slot);
    return 0;
}

int InodeHashEntryLinkOnlyInsert(InodeHashEntryLinkOp &op, const inode_id_t key, const pointer_t value){
    InodeHashEntrySearchResult res;
    HashEntryLinkSearchKey(res, op.root, key);
    if(res.key_find){  //已存在
        return 2;
    }
    pointer_t insert;
    uint16_t index;
    FindFreeSpaceOrCreatEntry(op, insert, index);
    NvmInodeHashEntryNode *insert_node = static_cast<NvmInodeHashEntryNode *>(NODE_GET_POINTER(insert));
    insert_node->SetEntryPersistByIndex(index, key, value);
    uint16_t slot = slot_set_index(insert_node->slot, index);
    insert_node->SetNumAndSlotPersist(insert_node->num + 1, slot);
    return 0;
}

int InodeHashEntryLinkUpdate(InodeHashEntryLinkOp &op, const inode_id_t key, const pointer_t new_value, pointer_t &old_value){
    InodeHashEntrySearchResult res;
    HashEntryLinkSearchKey(res, op.root, key);
    if(res.key_find){  //已存在
        NvmInodeHashEntryNode *node = static_cast<NvmInodeHashEntryNode *>(NODE_GET_POINTER(res.node));
        old_value = node->entry[res.index].pointer;
        node->SetEntryPointerPersist(res.index, new_value);
        return 0;
    }
    return 2; //不存在，无法修改
}

int InodeHashEntryLinkGet(pointer_t root, const inode_id_t key, pointer_t &value){
    InodeHashEntrySearchResult res;
    HashEntryLinkSearchKey(res, root, key);
    if(res.key_find){  //已存在
        NvmInodeHashEntryNode *node = static_cast<NvmInodeHashEntryNode *>(NODE_GET_POINTER(res.node));
        value = node->entry[res.index].pointer;
        return 0;
    }
    return 2;  //key不存在
}

int InodeHashEntryLinkDelete(InodeHashEntryLinkOp &op, const inode_id_t key, pointer_t &value){
    InodeHashEntrySearchResult res;
    HashEntryLinkSearchKey(res, op.root, key);
    if(res.key_find){  //已存在
        NvmInodeHashEntryNode *node = static_cast<NvmInodeHashEntryNode *>(NODE_GET_POINTER(res.node));
        value = node->entry[res.index].pointer;
        //合并暂不考虑
        if(node->num == 1){  //需要删除节点
            if(!IS_INVALID_POINTER(node->next)){
                NvmInodeHashEntryNode *next_node = static_cast<NvmInodeHashEntryNode *>(NODE_GET_POINTER(node->next));
                next_node->SetPrevPersist(node->prev);
            }
            if(!IS_INVALID_POINTER(node->prev)){
                NvmInodeHashEntryNode *prev_node = static_cast<NvmInodeHashEntryNode *>(NODE_GET_POINTER(node->prev));
                prev_node->SetNextPersist(node->next);
            } else {
                op.res = node->next;
            }
            op.del_list.push_back(res.node);
        } else {  //直接删除一条entry
            uint16_t slot = slot_clx_index(node->slot, res.index);
            node->SetNumAndSlotPersist(node->num - 1, slot);
        }
        
        return 0;
    }
    return 2; //不存在，无法删除
}


////

InodeHashTable::InodeHashTable(const Option &option, InodeZone *inode_zone) : option_(option), inode_zone_(inode_zone) {
    is_rehash_ = false;
    version_ = nullptr;
    rehash_version_ = nullptr;

    //init
    version_ = new InodeHashVersion(option_.INODE_HASHTABLE_INIT_SIZE);
    version_->Ref();
    DBG_LOG("inode:%u create hashtable:%p version:%p capacity:%lu", inode_zone_->get_zone_id(), this, version_, option_.INODE_HASHTABLE_INIT_SIZE);
}

InodeHashTable::~InodeHashTable(){
    if(version_) version_->Unref();
    if(rehash_version_) rehash_version_->Unref();
}

inline uint32_t InodeHashTable::hash_id(const inode_id_t key, const uint64_t capacity){
    return (key / option_.INODE_MAX_ZONE_NUM) % capacity;
}

inline bool InodeHashTable::NeedRehash(InodeHashVersion *version){
    uint64_t node_num = version->node_num_.load();
    if(!is_rehash_ && node_num >= (version->capacity_ * option_.INODE_HASHTABLE_TRIG_REHASH_TIMES)){
        return true;
    }
    return false;
}

void InodeHashTable::HashEntryDealWithOp(InodeHashVersion *version, uint32_t index, InodeHashEntryLinkOp &op){
    NvmInodeHashEntry *entry = &(version->buckets_[index]);
    if(op.res != entry->root) {
        entry->SetRootPersist(op.res);
    }
    uint32_t add_num = op.add_list.size();
    uint32_t del_num = op.del_list.size();
    
    if(add_num > del_num){
        version->node_num_.fetch_add(add_num - del_num);
    }
    if(add_num < del_num) {
        version->node_num_.fetch_sub(del_num - add_num);
    }

    if(!op.del_list.empty()) {  //后续最好原子记录一次操作的所有申请和删除的节点，作为空间管理日志
        for(auto it : op.del_list) {
            node_allocator->Free(it, INODE_HASH_ENTRY_SIZE);
        }
    }

    if(NeedRehash(version)){
        //rehash
        DBG_LOG("[inode] add rehash job, version:%p node_num:%lu", version, version->node_num_.load());
        thread_pool->Schedule(&InodeHashTable::BackgroundRehashWrapper, this);
    }
    
}

void InodeHashTable::GetVersionAndRefByWrite(bool &is_rehash, InodeHashVersion **version){
    MutexLock lock(&version_lock_);
    if(is_rehash_){
        is_rehash = true;
        *version = rehash_version_;
        rehash_version_->Ref();
    }
    else {
        is_rehash = false;
        *version = version_;
        version_->Ref();
    }
}

void InodeHashTable::GetVersionAndRef(bool &is_rehash, InodeHashVersion **version, InodeHashVersion **rehash_version){
    MutexLock lock(&version_lock_);
    if(is_rehash_){
        is_rehash = true;
        *version = version_;
        version_->Ref();
        *rehash_version = rehash_version_;
        rehash_version_->Ref();
    }
    else {
        is_rehash = false;
        *version = version_;
        version_->Ref();
    }
}

int InodeHashTable::HashEntryInsertKV(InodeHashVersion *version, uint32_t index, const inode_id_t key, const pointer_t value, pointer_t &old_value){
    version->rwlock_[index].WriteLock();
    NvmInodeHashEntry *entry = &(version->buckets_[index]);
    InodeHashEntryLinkOp op;
    op.root = entry->root;
    op.res = entry->root;
    int res = InodeHashEntryLinkInsert(op, key, value, old_value);
    HashEntryDealWithOp(version, index, op);
    version->rwlock_[index].Unlock();
    return res;

}

int InodeHashTable::HashEntryOnlyInsertKV(InodeHashVersion *version, uint32_t index, const inode_id_t key, const pointer_t value){
    version->rwlock_[index].WriteLock();
    NvmInodeHashEntry *entry = &(version->buckets_[index]);
    InodeHashEntryLinkOp op;
    op.root = entry->root;
    op.res = entry->root;
    int res = InodeHashEntryLinkOnlyInsert(op, key, value);
    if(res == 0) HashEntryDealWithOp(version, index, op);
    version->rwlock_[index].Unlock();
    return res;
}

int InodeHashTable::HashEntryUpdateKV(InodeHashVersion *version, uint32_t index, const inode_id_t key, const pointer_t new_value, pointer_t &old_value){
    version->rwlock_[index].WriteLock();
    NvmInodeHashEntry *entry = &(version->buckets_[index]);
    InodeHashEntryLinkOp op;
    op.root = entry->root;
    op.res = entry->root;
    int res = InodeHashEntryLinkUpdate(op, key, new_value, old_value);
    if(res == 0) HashEntryDealWithOp(version, index, op);
    version->rwlock_[index].Unlock();
    return res;
}

int InodeHashTable::HashEntryGetKV(InodeHashVersion *version, uint32_t index, const inode_id_t key, pointer_t &value){
    //version->rwlock_[index].ReadLock();  //可以不加读锁，因为都是MVCC控制，但是不加锁，什么时候删除垃圾节点是一个问题，延时删除可行或引用计数，
                                                //暂时简单处理，由于空间分配是往后分配，提前删除没有影响；
    NvmInodeHashEntry *entry = &(version->buckets_[index]);
    int res =  InodeHashEntryLinkGet(entry->root, key, value);
    //version->rwlock_[index].Unlock();
    return res;
}

int InodeHashTable::HashEntryDeleteKV(InodeHashVersion *version, uint32_t index, const inode_id_t key, pointer_t &value){
    version->rwlock_[index].WriteLock();
    NvmInodeHashEntry *entry = &(version->buckets_[index]);
    InodeHashEntryLinkOp op;
    op.root = entry->root;
    op.res = entry->root;
    int res = InodeHashEntryLinkDelete(op, key, value);
    if(res == 0) HashEntryDealWithOp(version, index, op);
    version->rwlock_[index].Unlock();
    return res;
}

int InodeHashTable::Put(const inode_id_t key, const pointer_t value, pointer_t &old_value){
    bool is_rehash = false;
    InodeHashVersion *version;
    GetVersionAndRefByWrite(is_rehash, &version);
    uint32_t index = hash_id(key, version->capacity_);

    int res = HashEntryInsertKV(version, index, key, value, old_value);
    
    version->Unref();
    return res;
}

int InodeHashTable::Get(const inode_id_t key, pointer_t &value){
    bool is_rehash = false;
    InodeHashVersion *version;
    InodeHashVersion *rehash_version;
    GetVersionAndRef(is_rehash, &version, &rehash_version);

    pointer_t value1;
    uint32_t index1 = hash_id(key, version->capacity_);
    int res1 = HashEntryGetKV(version, index1, key, value1);
    version->Unref();
    
    //两个版本都查找，如果rehash版本找到，则优先返回rehash版本；
    if(is_rehash){  //正在rehash，在rehash_version也查找
        pointer_t value2;
        uint32_t index2 = hash_id(key, rehash_version->capacity_);
        int res2 = HashEntryGetKV(rehash_version, index2, key, value2);
        rehash_version->Unref();
        if(res2 == 0) {  //res == 0,意味着找到
            value = value2;
            return res2;
        }
    }
    if(res1 == 0){
        value = value1;
    }
    return res1;   
}

int InodeHashTable::Update(const inode_id_t key, const pointer_t new_value, pointer_t &old_value){
    bool is_rehash = false;
    InodeHashVersion *version;
    GetVersionAndRefByWrite(is_rehash, &version);
    uint32_t index = hash_id(key, version->capacity_);
    
    int res = HashEntryInsertKV(version, index, key, new_value, old_value);
    
    version->Unref();
    return res;
}

int InodeHashTable::Delete(const inode_id_t key, pointer_t &value1, pointer_t &value2){
    bool is_rehash = false;
    InodeHashVersion *version;
    InodeHashVersion *rehash_version;
    GetVersionAndRef(is_rehash, &version, &rehash_version);  //只是为了获取两个版本

    uint32_t index = hash_id(key, version->capacity_);
    int res1 = HashEntryDeleteKV(version, index, key, value1);
    version->Unref();
    int res2 = 2;
    if(is_rehash) { //正在rehash，先在version删除，再在rehash_version中删除
        uint32_t index = hash_id(key, rehash_version->capacity_);
        res2 = HashEntryDeleteKV(rehash_version, index, key, value2);
        rehash_version->Unref();
    }
    return res1 & res2;  //有一个为0则为0
}

void InodeHashTable::BackgroundRehashWrapper(void *arg){
    reinterpret_cast<InodeHashTable *>(arg)->BackgroundRehash();
}

void InodeHashTable::BackgroundRehash(){
    version_lock_.Lock();
    if(!NeedRehash(version_)){
        version_lock_.Unlock();
        return ;
    }
    rehash_version_ = new InodeHashVersion(version_->capacity_ * 2);
    version_->Ref();
    rehash_version_->Ref();
    is_rehash_ = true;
    version_lock_.Unlock();

    DBG_LOG("[inode] second hash rehash start, version:%p rehash_version:%p", version_, rehash_version_);

    //开始逐步迁移数据到rehash_version_中
    for(uint32_t index = 0; index < version_->capacity_; index++){
        DBG_LOG("[inode] second hash rehash doing, version:%p rehash_version:%p index:%u", version_, rehash_version_, index);
        MoveEntryToRehash(version_, index, rehash_version_);
    }
    //迁移完
    DBG_LOG("[inode] second hash rehash end, version:%p rehash_version:%p", version_, rehash_version_);
    version_lock_.Lock();
    InodeHashVersion *old_version = version_;
    version_ = rehash_version_;
    rehash_version_ = nullptr;
    is_rehash_ = false;
    version_lock_.Unlock();

    old_version->Unref();   //解除本函数的调用
    old_version->Unref();   //删除旧version
    
}

void InodeHashTable::MoveEntryToRehash(InodeHashVersion *version, uint32_t index, InodeHashVersion *rehash_version){
    version->rwlock_[index].WriteLock();   //
    NvmInodeHashEntry *entry = &(version->buckets_[index]);
    vector<pointer_t> free_list;
    pointer_t cur = entry->root;
    NvmInodeHashEntryNode *cur_node;
    inode_id_t key;
    uint32_t key_index;
    pointer_t value;
    int res = 0;
    while(!IS_INVALID_POINTER(cur)) {
        cur_node = static_cast<NvmInodeHashEntryNode *>(NODE_GET_POINTER(cur));
        for(uint16_t i = 0; i < INODE_HASH_ENTRY_NODE_CAPACITY; i++){
            if(!slot_get_index(cur_node->slot, i)) continue;
            key = cur_node->entry[i].key;
            value = cur_node->entry[i].pointer;
            key_index = hash_id(key, rehash_version->capacity_);
            res = HashEntryOnlyInsertKV(rehash_version, key_index, key, value);
            if(res == 2){  //说明新插入了相同key，旧value废弃
                inode_zone_->DeleteFlie(value);
            }

        }
        free_list.push_back(cur);
        cur = cur_node->next;
    }
    entry->SetRootPersist(INVALID_POINTER);  //旧version的entry变为空
    version->rwlock_[index].Unlock();

    for(auto it : free_list) {
        node_allocator->Free(it, INODE_HASH_ENTRY_SIZE);
    }
}

string PrintSlot(uint16_t slot, uint16_t num){
    string res;
    for(uint16_t i = 0; i < num; i++){
        res.push_back('0' + ((slot >> i) & 1));
    }
    return res;
}

int InodeHashTable::PrintEntry(pointer_t root){  //返回kv条数
    pointer_t cur = root;
    NvmInodeHashEntryNode *cur_node;
    int nums = 0;
    while(!IS_INVALID_POINTER(cur)) {
        cur_node = static_cast<NvmInodeHashEntryNode *>(NODE_GET_POINTER(cur));
        DBG_LOG("[inode] node:%lu num:%u slot:%.*s prev:%lu next:%lu", cur, cur_node->num, INODE_HASH_ENTRY_NODE_CAPACITY,
            PrintSlot(cur_node->slot, INODE_HASH_ENTRY_NODE_CAPACITY).c_str(), cur_node->prev, cur_node->next);
        for(uint16_t i = 0; i < INODE_HASH_ENTRY_NODE_CAPACITY; i++){
            if(!slot_get_index(cur_node->slot, i)) continue;
            string value;
            inode_zone_->GetValueByAddr(cur_node->entry[i].pointer, value);
            DBG_LOG("[inode] node:%lu %u key:%lu pointer:%lu value:%s", cur, i, cur_node->entry[i].key, cur_node->entry[i].pointer, value.c_str());
        }
        nums += cur_node->num;
        cur = cur_node->next;
    }
    return nums;
}

void InodeHashTable::PrintVersion(InodeHashVersion *version){
    if(version == nullptr) return ;
    DBG_LOG("[inode] hashtable verion:%p capacity:%lu node_num:%u", version, version->capacity_, version->node_num_.load());
    int nums = 0;
    for(uint32_t i = 0; i < version->capacity_; i++){
        NvmInodeHashEntry *entry = &(version->buckets_[i]);
        DBG_LOG("[inode] hashtable hash version:%p entry:%u root:%u ", version, i, entry->root);
        nums += PrintEntry(entry->root);
    }
    DBG_LOG("[inode] hashtable verion:%p kv nums:%d", version, nums);
}

void InodeHashTable::PrintHashTable(){
    DBG_LOG("[inode] hashtable print! hashtable:%p version:%p re_version:%p", this, version_, rehash_version_);
    PrintVersion(version_);
    PrintVersion(rehash_version_);
}

void GetEntryStats(pointer_t root, uint64_t &node_nums, uint64_t &kv_nums){
    pointer_t cur = root;
    NvmInodeHashEntryNode *cur_node;
    while(!IS_INVALID_POINTER(cur)) {
        cur_node = static_cast<NvmInodeHashEntryNode *>(NODE_GET_POINTER(cur));
        kv_nums += cur_node->num;
        node_nums++;
        cur = cur_node->next;
    }
}

string InodeHashTable::PrintVersionStats(InodeHashVersion *version, uint64_t &hashtable_node_nums, uint64_t &hashtable_kv_nums){
    if(version == nullptr) return string();
    string res;
    char buf[1024];
    uint64_t node_nums = 0;
    uint64_t kv_nums = 0;

    for(uint32_t i = 0; i < version->capacity_; i++){
        NvmInodeHashEntry *entry = &(version->buckets_[i]);
        GetEntryStats(entry->root, node_nums, kv_nums);
    }
    hashtable_node_nums += node_nums;
    hashtable_kv_nums += kv_nums;
    snprintf(buf, sizeof(buf), "capacity:%lu nodes:%lu node_nums:%lu kv_nums:%lu ", version->capacity_, version->node_num_.load(), node_nums, kv_nums);
    res.append(buf);
    return res;
}

string InodeHashTable::PrintHashTableStats(uint64_t &hashtable_node_nums, uint64_t &hashtable_kv_nums){
    string res;
    if(version_ != nullptr){
        res.append("version ");
        res.append(PrintVersionStats(version_, hashtable_node_nums, hashtable_kv_nums));
    }
    if(rehash_version_ != nullptr){
        res.append("rehash version ");
        res.append(PrintVersionStats(rehash_version_, hashtable_node_nums, hashtable_kv_nums));
    }
    res.push_back('\n');
    return res;
}

} // namespace name