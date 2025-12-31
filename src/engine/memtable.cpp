/**
 * @file memtable.cpp
 * @brief MemTable implementation using SkipList
 */

#include <mydb/engine/memtable.hpp>
#include <mydb/engine/skiplist.hpp>

#include <spdlog/spdlog.h>

namespace mydb {

// ============================================================================
// SkipList Implementation for MemTable
// ============================================================================

class MemTable::SkipListImpl {
public:
    struct Entry {
        std::string key;
        std::string value;
        SequenceNumber sequence;
        OperationType type;
        
        bool operator<(const Entry& other) const {
            if (key != other.key) return key < other.key;
            return sequence > other.sequence;  // Newer first
        }
    };
    
    struct EntryComparator {
        bool operator()(const Entry& a, const Entry& b) const {
            return a < b;
        }
    };
    
    using SkipListType = SkipList<Entry, EntryComparator>;
    
    SkipListType skiplist;
    
    SkipListImpl() : skiplist() {}
};

// ============================================================================
// MemTable Iterator Implementation
// ============================================================================

class MemTableIteratorImpl : public MemTableIterator {
public:
    using SkipListType = MemTable::SkipListImpl::SkipListType;
    
    explicit MemTableIteratorImpl(const SkipListType* skiplist)
        : iter_(skiplist) {}
    
    bool Valid() const override { return iter_.Valid(); }
    
    void SeekToFirst() override { iter_.SeekToFirst(); }
    void SeekToLast() override { iter_.SeekToLast(); }
    
    void Seek(const Slice& key) override {
        MemTable::SkipListImpl::Entry target;
        target.key = key.ToString();
        target.sequence = UINT64_MAX;
        iter_.Seek(target);
    }
    
    void Next() override { iter_.Next(); }
    void Prev() override { iter_.Prev(); }
    
    Slice key() const override {
        return iter_.key().key;
    }
    
    Slice value() const override {
        return iter_.key().value;
    }
    
private:
    mutable SkipListType::Iterator iter_;
};

// ============================================================================
// MemTable Implementation
// ============================================================================

MemTable::MemTable() 
    : table_(std::make_unique<SkipListImpl>()) {
    spdlog::debug("MemTable created");
}

MemTable::~MemTable() {
    spdlog::debug("MemTable destroyed, size: {} bytes, {} entries", 
                  memory_usage_.load(), count_.load());
}

void MemTable::Add(const Slice& key, const Slice& value, 
                   SequenceNumber sequence, OperationType type) {
    SkipListImpl::Entry entry;
    entry.key = key.ToString();
    entry.value = value.ToString();
    entry.sequence = sequence;
    entry.type = type;
    
    // Calculate memory usage
    size_t entry_size = entry.key.size() + entry.value.size() + 
                        sizeof(SequenceNumber) + sizeof(OperationType) +
                        sizeof(void*) * kSkipListMaxHeight;  // Node pointers estimate
    
    table_->skiplist.Insert(entry);
    
    memory_usage_.fetch_add(entry_size, std::memory_order_relaxed);
    count_.fetch_add(1, std::memory_order_relaxed);
    
    // Update sequence bounds
    if (smallest_seq_ == 0 || sequence < smallest_seq_) {
        smallest_seq_ = sequence;
    }
    if (sequence > largest_seq_) {
        largest_seq_ = sequence;
    }
    
    spdlog::trace("MemTable::Add key={}, seq={}, type={}", 
                  key.ToString(), sequence, static_cast<int>(type));
}

Result<std::string> MemTable::Get(const Slice& key, SequenceNumber sequence) const {
    // Create a target entry to search for
    SkipListImpl::Entry target;
    target.key = key.ToString();
    target.sequence = sequence;
    target.type = OperationType::kPut;  // Doesn't matter for search
    
    SkipListImpl::SkipListType::Iterator iter(&table_->skiplist);
    iter.Seek(target);
    
    // The SkipList finds the first entry >= target
    // We need to check if the key matches and sequence <= our snapshot
    while (iter.Valid()) {
        const auto& entry = iter.key();
        
        if (entry.key != key.ToString()) {
            // Went past our key
            break;
        }
        
        if (entry.sequence <= sequence) {
            // Found a matching entry at or before our snapshot
            if (entry.type == OperationType::kDelete) {
                return Status::NotFound("Key was deleted");
            }
            return entry.value;
        }
        
        iter.Next();
    }
    
    return Status::NotFound("Key not found in MemTable");
}

std::unique_ptr<MemTableIterator> MemTable::NewIterator() const {
    return std::unique_ptr<MemTableIteratorImpl>(new MemTableIteratorImpl(&table_->skiplist));
}

} // namespace mydb
