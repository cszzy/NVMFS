/**
 * @Author      : Liu Zhiwen
 * @Create Date : 2021-01-12 14:59:28
 * @Contact     : 993096281@qq.com
 * @Description : 
 */

#ifndef _METADB_DIR_ITERATOR_H_
#define _METADB_DIR_ITERATOR_H_

#include <stdint.h>
#include <assert.h>

#include "metadb/inode.h"
#include "metadb/iterator.h"
#include "dir_nvm_node.h"
#include "nvm_node_allocator.h"

using namespace std;
namespace metadb {

class LinkNodeIterator : public Iterator {
public:
    LinkNodeIterator(pointer_t node, uint32_t key_offset, uint32_t key_num, uint32_t key_len);
    ~LinkNodeIterator() {};

    bool Valid() const ;
    //virtual void Seek(const Slice& target) = 0;
    void SeekToFirst();
    void SeekToLast();
    void Next();
    void Prev();
    string fname() const ;
    uint64_t hash_fname() const ;
    inode_id_t value() const ;
private:
    LinkNode *cur_node_;
    uint32_t key_offset_;
    uint32_t key_num_;
    uint32_t key_len_;

    uint32_t cur_index_;
    uint32_t cur_offset_; 
};


class BptreeIterator : public Iterator {
public:
    BptreeIterator(BptreeLeafNode *head);
    ~BptreeIterator() {};

    bool Valid() const ;
    //virtual void Seek(const Slice& target) = 0;
    void SeekToFirst();
    void SeekToLast();
    void Next();
    void Prev();
    string fname() const ;
    uint64_t hash_fname() const ;
    inode_id_t value() const ;
private:
    BptreeLeafNode *leaf_head_;
    BptreeLeafNode *cur_node_;
    uint32_t cur_index_;
    uint32_t cur_offset_; 
};

class EmptyIterator : public Iterator {
public:
    EmptyIterator() {};
    ~EmptyIterator() {};

    bool Valid() const { return false; }
    //virtual void Seek(const Slice& target) = 0;
    void SeekToFirst() {};
    void SeekToLast() {};
    void Next() {};
    void Prev() {};
    string fname() const { return string(); };
    uint64_t hash_fname() const { return 0; };
    inode_id_t value() const { return 0; };
};

class IteratorWrapper {
public:
    IteratorWrapper(): iter_(NULL), valid_(false) { }
    explicit IteratorWrapper(Iterator* iter): iter_(NULL) {
        Set(iter);
    }
    ~IteratorWrapper() { delete iter_; }
    Iterator* iter() const { return iter_; }

    void Set(Iterator* iter) {
        delete iter_;
        iter_ = iter;
        if (iter_ == NULL) {
        valid_ = false;
        } else {
            Update();
        }
    }

    // Iterator interface methods
    bool Valid() const        { return valid_; }
    string fname() const { return iter_->fname(); };
    uint64_t hash_fname() const { return key_; };
    inode_id_t value() const { return iter_->value(); };
    void Next()               { iter_->Next();        Update(); }
    void Prev()               { iter_->Prev();        Update(); }

    void SeekToFirst()        { iter_->SeekToFirst(); Update(); }
    void SeekToLast()         { iter_->SeekToLast();  Update(); }

private:
    void Update() {
        valid_ = iter_->Valid();
        if (valid_) {
            key_ = iter_->hash_fname();
        }
    }

    Iterator* iter_;
    bool valid_;
    uint64_t key_;
};

class MergingIterator : public Iterator {
public:
    MergingIterator(Iterator** children, int n)
          : children_(new IteratorWrapper[n]),
            n_(n),
            current_(NULL),
            direction_(kForward) {
        for (int i = 0; i < n; i++) {
            children_[i].Set(children[i]);
        }
    }

    ~MergingIterator() {
        delete[] children_;
    }

    bool Valid() const {
        return (current_ != NULL);
    }

    void SeekToFirst() {
        for (int i = 0; i < n_; i++) {
            children_[i].SeekToFirst();
        }
        FindSmallest();
        direction_ = kForward;
    }

    void SeekToLast() {
        for (int i = 0; i < n_; i++) {
            children_[i].SeekToLast();
        }
        FindLargest();
        direction_ = kReverse;
    }

    void Next() {
        assert(Valid());

        // Ensure that all children are positioned after key().
        // If we are moving in the forward direction, it is already
        // true for all of the non-current_ children since current_ is
        // the smallest child and key() == current_->key().  Otherwise,
        // we explicitly position the non-current_ children.
        // if (direction_ != kForward) {
        //     for (int i = 0; i < n_; i++) {
        //         IteratorWrapper* child = &children_[i];
        //         if (child != current_) {
        //             child->Seek(key());
        //             if (child->Valid() &&
        //                 comparator_->Compare(key(), child->key()) == 0) {
        //                 child->Next();
        //             }
        //         }
        //     }
        //     direction_ = kForward;
        // }

        current_->Next();
        FindSmallest();
    }

    void Prev() {
        assert(Valid());

        // Ensure that all children are positioned before key().
        // If we are moving in the reverse direction, it is already
        // true for all of the non-current_ children since current_ is
        // the largest child and key() == current_->key().  Otherwise,
        // we explicitly position the non-current_ children.
        // if (direction_ != kReverse) {
        //     for (int i = 0; i < n_; i++) {
        //         IteratorWrapper* child = &children_[i];
        //         if (child != current_) {
        //             child->Seek(key());
        //             if (child->Valid()) {
        //                 // Child is at first entry >= key().  Step back one to be < key()
        //                 child->Prev();
        //             } else {
        //                 // Child has no entries >= key().  Position at last entry.
        //                 child->SeekToLast();
        //             }
        //         }
        //     }
        //     direction_ = kReverse;
        // }

        current_->Prev();
        FindLargest();
    }
    string fname() const { return current_->fname(); }
    uint64_t hash_fname() const { return current_->hash_fname(); }
    inode_id_t value() const { return current_->value(); }

private:
    void FindSmallest();
    void FindLargest();

    // We might want to use a heap in case there are lots of children.
    // For now we use a simple array since we expect a very small number
    // of children in leveldb.
    IteratorWrapper* children_;
    int n_;
    IteratorWrapper* current_;

    // Which direction is the iterator moving?
    enum Direction {
        kForward,
        kReverse
    };
    Direction direction_;
};

} // namespace name


#endif