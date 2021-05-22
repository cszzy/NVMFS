/**
 * @Author      : Liu Zhiwen
 * @Create Date : 2021-01-25 14:17:20
 * @Contact     : 993096281@qq.com
 * @Description : 
 */

#include "dir_iterator.h"

namespace metadb {

LinkNodeIterator::LinkNodeIterator(pointer_t node, uint32_t key_offset, uint32_t key_num, uint32_t key_len) 
                : key_offset_(key_offset), key_num_(key_num), key_len_(key_len) {
    cur_node_ = nullptr;
    if(!IS_INVALID_POINTER(node)){
        cur_node_ = static_cast<LinkNode *>(NODE_GET_POINTER(node));
    }
    cur_index_ = 0;
    cur_offset_ = key_offset_;
}

bool LinkNodeIterator::Valid() const {
    return cur_index_ < key_num_;
}
void LinkNodeIterator::SeekToFirst() {
    cur_index_ = 0;
    cur_offset_ = key_offset_ + sizeof(inode_id_t) + 4 + 4;
}

void LinkNodeIterator::SeekToLast(){
    assert(0);
}

void LinkNodeIterator::Next(){
    cur_index_++;
    if(Valid()){
        uint64_t hash_fname;
        uint32_t value_len;
        cur_node_->DecodeBufGetHashfnameAndLen(cur_offset_, hash_fname, value_len);
        cur_offset_ += (8 + 4 + value_len);
    }
}

void LinkNodeIterator::Prev(){
    assert(0);
}

string LinkNodeIterator::fname() const {
    uint64_t hash_fname;
    uint32_t value_len;
    cur_node_->DecodeBufGetHashfnameAndLen(cur_offset_, hash_fname, value_len);
    return string(cur_node_->buf + cur_offset_ + 8 + 4, value_len - sizeof(inode_id_t));
}

uint64_t LinkNodeIterator::hash_fname() const {
    return *reinterpret_cast<const uint64_t *>(cur_node_->buf + cur_offset_);
}

inode_id_t LinkNodeIterator::value() const {
    uint64_t hash_fname;
    uint32_t value_len;
    cur_node_->DecodeBufGetHashfnameAndLen(cur_offset_, hash_fname, value_len);
    uint32_t value_offset = cur_offset_ + 8 + 4 + (value_len - sizeof(inode_id_t));
    inode_id_t value;
    cur_node_->DecodeBufGetKey(value_offset, value);
    return value;
}

BptreeIterator::BptreeIterator(BptreeLeafNode *head) : leaf_head_(head) {
    cur_node_ = leaf_head_;
    cur_index_ = 0;
    cur_offset_ = 0;
}

bool BptreeIterator::Valid() const {
    return cur_node_ != nullptr && cur_index_ < cur_node_->num;
}
void BptreeIterator::SeekToFirst() {
    cur_node_ = leaf_head_;
    cur_index_ = 0;
    cur_offset_ = 0;
}

void BptreeIterator::SeekToLast(){
    assert(0);
}

void BptreeIterator::Next(){
    cur_index_++;
    if(Valid()){
        uint64_t hash_fname;
        uint32_t value_len;
        cur_node_->DecodeBufGetKeyValuelen(cur_offset_, hash_fname, value_len);
        cur_offset_ += (8 + 4 + value_len);
    } else {
        if(!IS_INVALID_POINTER(cur_node_->next)) {
            cur_node_ = static_cast<BptreeLeafNode *>(NODE_GET_POINTER(cur_node_->next));
            cur_index_ = 0;
            cur_offset_ = 0;
        } else {
            cur_node_ = nullptr;
            cur_index_ = 0;
            cur_offset_ = 0;
        }
    }
}

void BptreeIterator::Prev(){
    assert(0);
}

string BptreeIterator::fname() const {
    uint64_t hash_fname;
    uint32_t value_len;
    cur_node_->DecodeBufGetKeyValuelen(cur_offset_, hash_fname, value_len);
    return string(cur_node_->buf + cur_offset_ + 8 + 4, value_len - sizeof(inode_id_t));
}

uint64_t BptreeIterator::hash_fname() const {
    return *reinterpret_cast<const uint64_t *>(cur_node_->buf + cur_offset_);
}

inode_id_t BptreeIterator::value() const {
    uint64_t hash_fname;
    uint32_t value_len;
    cur_node_->DecodeBufGetKeyValuelen(cur_offset_, hash_fname, value_len);
    uint32_t value_offset = cur_offset_ + 8 + 4 + (value_len - sizeof(inode_id_t));
    return *reinterpret_cast<inode_id_t *>(cur_node_->buf + value_offset);
}

void MergingIterator::FindSmallest() {
    IteratorWrapper* smallest = NULL;
    for (int i = 0; i < n_; i++) {
        IteratorWrapper* child = &children_[i];
        if (child->Valid()) {
            if (smallest == NULL) {
                smallest = child;
            } else if (child->hash_fname() < smallest->hash_fname()) {
                smallest = child;
            } else if (child->hash_fname() == smallest->hash_fname()){
                child->Next();
            }
        }
    }
    current_ = smallest;
}

void MergingIterator::FindLargest() {
    IteratorWrapper* largest = NULL;
    for (int i = n_-1; i >= 0; i--) {
        IteratorWrapper* child = &children_[i];
        if (child->Valid()) {
            if (largest == NULL) {
                largest = child;
            } else if (child->hash_fname() > largest->hash_fname()) {
                largest = child;
            } else if (child->hash_fname() == largest->hash_fname()) {
                child->Next();
            }
        }
    }
    current_ = largest;
}



} // namespace name