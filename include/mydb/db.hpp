#pragma once

// Main database interface - thread-safe, reads concurrent, writes serialized

#include <mydb/common/types.hpp>
#include <mydb/common/status.hpp>
#include <mydb/common/slice.hpp>
#include <mydb/engine/memtable.hpp>
#include <mydb/engine/sstable.hpp>
#include <mydb/engine/wal.hpp>
#include <mydb/engine/wal.hpp>
#include <mydb/engine/compactor.hpp>
#include <mydb/storage/buffer_pool.hpp>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <atomic>
#include <vector>
#include <string>

namespace mydb {

class PythonVM;
class Server;

class Database {
public:
    static Result<std::unique_ptr<Database>> Open(const Options& options);
    ~Database();
    Database(const Database&) = delete;
    Database& operator=(const Database&) = delete;
    
    // Core operations
    Result<std::string> Get(const Slice& key, const ReadOptions& options = {});
    Status Put(const Slice& key, const Slice& value, const WriteOptions& options = {});
    Status Delete(const Slice& key, const WriteOptions& options = {});
    bool Exists(const Slice& key);
    
    // Batch writes
    class WriteBatch {
    public:
        void Put(const Slice& key, const Slice& value);
        void Delete(const Slice& key);
        void Clear();
        [[nodiscard]] size_t Count() const { return operations_.size(); }
    private:
        friend class Database;
        struct Operation { OperationType type; std::string key, value; };
        std::vector<Operation> operations_;
    };
    Status Write(const WriteBatch& batch, const WriteOptions& options = {});
    
    // Iterator
    class Iterator {
    public:
        virtual ~Iterator() = default;
        virtual bool Valid() const = 0;
        virtual void SeekToFirst() = 0;
        virtual void SeekToLast() = 0;
        virtual void Seek(const Slice& key) = 0;
        virtual void Next() = 0;
        virtual void Prev() = 0;
        virtual Slice key() const = 0;
        virtual Slice value() const = 0;
        virtual Status status() const = 0;
    };
    std::unique_ptr<Iterator> NewIterator(const ReadOptions& options = {});
    
    // Maintenance
    Status Flush();
    Status CompactLevel(int level = -1);
    SequenceNumber GetSnapshot();
    void ReleaseSnapshot(SequenceNumber snapshot);
    
#ifdef MYDB_ENABLE_PYTHON
    Result<std::string> ExecutePython(const std::string& script);
#endif
    
    // Stats
    struct Stats {
        uint64_t num_entries{0}, memtable_size{0}, num_sstables{0}, disk_usage{0};
        uint64_t reads{0}, writes{0}, deletes{0}, cache_hits{0}, cache_misses{0};
        SequenceNumber sequence{0};
    };
    [[nodiscard]] Stats GetStats() const;
    [[nodiscard]] std::string GetVersion() const;
    [[nodiscard]] const Options& GetOptions() const { return options_; }
    
    // Phase 3: Introspection
    BufferPoolManager* GetBufferPoolManager() { return buffer_pool_manager_.get(); }
    
private:
    explicit Database(const Options& options);
    Status Initialize();
    Status Recover();
    Status WriteInternal(const Slice& key, const Slice& value, OperationType type, const WriteOptions& options);
    Status RotateMemTable();
    Status FlushImmutableMemTable();
    Result<std::string> GetFromLevels(const Slice& key, SequenceNumber seq);
    
    Options options_;
    std::string db_path_;
    std::unique_ptr<MemTable> memtable_, immutable_memtable_;
    std::unique_ptr<WALWriter> wal_;
    std::unique_ptr<WALManager> wal_manager_;
    std::unique_ptr<BufferPoolManager> buffer_pool_manager_;
    std::unique_ptr<VersionSet> versions_;
    std::unique_ptr<Compactor> compactor_;
#ifdef MYDB_ENABLE_PYTHON
    std::unique_ptr<PythonVM> python_vm_;
#endif
    std::atomic<SequenceNumber> sequence_{0};
    mutable std::shared_mutex mutex_;
    std::mutex write_mutex_;
    mutable Stats stats_;
};

} // namespace mydb
