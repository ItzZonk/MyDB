#pragma once

#include <mydb/config.hpp>
#include <atomic>
#include <memory>
#include <random>
#include <cassert>
#include <functional>

namespace mydb {

// Lock-free SkipList: O(log n) search/insert, single writer, concurrent readers
template<typename Key, typename Comparator = std::less<Key>>
class SkipList {
public:
    struct Node {
        Key const key;
        std::atomic<Node*> next_[1]; // variable-length array
        
        explicit Node(const Key& k) : key(k) {}
        Node* Next(int level) const { return next_[level].load(std::memory_order_acquire); }
        void SetNext(int level, Node* node) { next_[level].store(node, std::memory_order_release); }
        bool CASNext(int level, Node* expected, Node* desired) {
            return next_[level].compare_exchange_strong(expected, desired,
                std::memory_order_release, std::memory_order_acquire);
        }
    };
    
    class Iterator {
    public:
        explicit Iterator(const SkipList* list) : list_(list), node_(nullptr) {}
        bool Valid() const { return node_ != nullptr; }
        const Key& key() const { assert(Valid()); return node_->key; }
        void Next() { assert(Valid()); node_ = node_->Next(0); }
        void Prev() { assert(Valid()); node_ = list_->FindLessThan(node_->key); if (node_ == list_->head_) node_ = nullptr; }
        void Seek(const Key& target) { node_ = list_->FindGreaterOrEqual(target, nullptr); }
        void SeekToFirst() { node_ = list_->head_->Next(0); }
        void SeekToLast() { node_ = list_->FindLast(); if (node_ == list_->head_) node_ = nullptr; }
    private:
        const SkipList* list_;
        Node* node_;
    };
    
    explicit SkipList(Comparator cmp = Comparator())
        : compare_(cmp), head_(NewNode(Key{}, kSkipListMaxHeight)), max_height_(1), rng_(std::random_device{}()) {
        for (int i = 0; i < kSkipListMaxHeight; ++i) head_->next_[i].store(nullptr, std::memory_order_relaxed);
    }
    
    ~SkipList() {}
    SkipList(const SkipList&) = delete;
    SkipList& operator=(const SkipList&) = delete;
    
    void Insert(const Key& key) {
        Node* prev[kSkipListMaxHeight];
        FindGreaterOrEqual(key, prev);
        int height = RandomHeight();
        if (height > GetMaxHeight()) {
            for (int i = GetMaxHeight(); i < height; ++i) prev[i] = head_;
            max_height_.store(height, std::memory_order_relaxed);
        }
        Node* x = NewNode(key, height);
        for (int i = 0; i < height; ++i) {
            x->next_[i].store(prev[i]->Next(i), std::memory_order_relaxed);
            prev[i]->SetNext(i, x);
        }
    }
    
    bool Contains(const Key& key) const {
        Node* x = FindGreaterOrEqual(key, nullptr);
        return x != nullptr && Equal(key, x->key);
    }
    
    Node* Find(const Key& key) const {
        Node* x = FindGreaterOrEqual(key, nullptr);
        return (x != nullptr && Equal(key, x->key)) ? x : nullptr;
    }
    
    Iterator NewIterator() const { return Iterator(this); }
    
private:
    int GetMaxHeight() const { return max_height_.load(std::memory_order_relaxed); }
    
    Node* NewNode(const Key& key, int height) {
        char* mem = new char[sizeof(Node) + sizeof(std::atomic<Node*>) * (height - 1)];
        return new (mem) Node(key);
    }
    
    int RandomHeight() {
        int h = 1;
        while (h < kSkipListMaxHeight && std::uniform_real_distribution<>(0, 1)(rng_) < kSkipListProbability) h++;
        return h;
    }
    
    bool Equal(const Key& a, const Key& b) const { return !compare_(a, b) && !compare_(b, a); }
    bool KeyIsAfterNode(const Key& key, Node* n) const { return n != nullptr && compare_(n->key, key); }
    
    Node* FindGreaterOrEqual(const Key& key, Node** prev) const {
        Node* x = head_;
        int level = GetMaxHeight() - 1;
        while (true) {
            Node* next = x->Next(level);
            if (KeyIsAfterNode(key, next)) { x = next; }
            else {
                if (prev) prev[level] = x;
                if (level == 0) return next;
                level--;
            }
        }
    }
    
    Node* FindLessThan(const Key& key) const {
        Node* x = head_;
        int level = GetMaxHeight() - 1;
        while (true) {
            Node* next = x->Next(level);
            if (next == nullptr || !compare_(next->key, key)) {
                if (level == 0) return x;
                level--;
            } else { x = next; }
        }
    }
    
    Node* FindLast() const {
        Node* x = head_;
        int level = GetMaxHeight() - 1;
        while (true) {
            Node* next = x->Next(level);
            if (next == nullptr) { if (level == 0) return x; level--; }
            else { x = next; }
        }
    }
    
    Comparator const compare_;
    Node* head_;
    std::atomic<int> max_height_;
    mutable std::mt19937 rng_;
};

} // namespace mydb
