#pragma once

#include <mydb/common/types.hpp>
#include <mydb/common/status.hpp>
#include <mydb/common/slice.hpp>
#include <mydb/config.hpp>
#include <atomic>
#include <memory>
#include <random>
#include <functional>

namespace mydb {

template<typename Key, typename Comparator> class SkipList;

class MemTableIterator {
public:
    virtual ~MemTableIterator() = default;
    virtual bool Valid() const = 0;
    virtual void SeekToFirst() = 0;
    virtual void SeekToLast() = 0;
    virtual void Seek(const Slice& key) = 0;
    virtual void Next() = 0;
    virtual void Prev() = 0;
    virtual Slice key() const = 0;
    virtual Slice value() const = 0;
};

// In-memory sorted table using lock-free SkipList
// Thread Safety: single writer, concurrent readers
class MemTable {
public:
    explicit MemTable();
    ~MemTable();
    MemTable(const MemTable&) = delete;
    MemTable& operator=(const MemTable&) = delete;
    
    void Add(const Slice& key, const Slice& value, SequenceNumber sequence, OperationType type);
    Result<std::string> Get(const Slice& key, SequenceNumber sequence) const;
    std::unique_ptr<MemTableIterator> NewIterator() const;
    
    [[nodiscard]] size_t ApproximateMemoryUsage() const { return memory_usage_.load(std::memory_order_relaxed); }
    [[nodiscard]] bool ShouldFlush() const { return ApproximateMemoryUsage() >= kMemTableMaxSize; }
    [[nodiscard]] size_t Count() const { return count_.load(std::memory_order_relaxed); }
    [[nodiscard]] SequenceNumber SmallestSeq() const { return smallest_seq_; }
    [[nodiscard]] SequenceNumber LargestSeq() const { return largest_seq_; }
    
    friend class MemTableIteratorImpl;
    
private:
    struct Entry {
        std::string key;
        std::string value;
        SequenceNumber sequence;
        OperationType type;
        bool operator<(const Entry& o) const {
            if (key != o.key) return key < o.key;
            return sequence > o.sequence;
        }
    };
    
    struct EntryComparator {
        bool operator()(const Entry& a, const Entry& b) const { return a < b; }
    };
    
    class SkipListImpl;
    std::unique_ptr<SkipListImpl> table_;
    std::atomic<size_t> memory_usage_{0};
    std::atomic<size_t> count_{0};
    SequenceNumber smallest_seq_{0};
    SequenceNumber largest_seq_{0};
};

} // namespace mydb
