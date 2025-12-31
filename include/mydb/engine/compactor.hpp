#pragma once

// Leveled Compaction: L0 (overlapping) -> L1+ (sorted, non-overlapping, each 10x larger)

#include <mydb/common/types.hpp>
#include <mydb/common/status.hpp>
#include <mydb/engine/sstable.hpp>
#include <vector>
#include <memory>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <functional>
#include <queue>

namespace mydb {

struct FileMetaData {
    uint64_t file_number, file_size;
    std::string filename, smallest_key, largest_key;
    uint64_t num_entries;
    int level;
    bool Overlaps(const std::string& min_key, const std::string& max_key) const;
};

class VersionSet {
public:
    explicit VersionSet(const std::string& db_path);
    [[nodiscard]] std::vector<FileMetaData> GetFilesAtLevel(int level) const;
    void AddFile(int level, const FileMetaData& file);
    void RemoveFiles(int level, const std::vector<uint64_t>& file_numbers);
    [[nodiscard]] size_t NumFilesAtLevel0() const;
    [[nodiscard]] uint64_t LevelSize(int level) const;
    [[nodiscard]] bool NeedsCompaction(int level) const;
    uint64_t NextFileNumber();
    Status WriteManifest();
    Status LoadManifest();
private:
    std::string db_path_;
    std::vector<std::vector<FileMetaData>> levels_;
    std::atomic<uint64_t> next_file_number_{1};
    mutable std::mutex mutex_;
};

struct CompactionJob {
    int level;
    std::vector<FileMetaData> inputs, outputs;
};

class Compactor {
public:
    using FlushCallback = std::function<void(const FileMetaData&)>;
    
    explicit Compactor(VersionSet* versions, const std::string& db_path);
    ~Compactor();
    Compactor(const Compactor&) = delete;
    Compactor& operator=(const Compactor&) = delete;
    
    void Start();
    void Stop();
    void MaybeScheduleCompaction();
    Status CompactLevel(int level);
    void WaitForCompaction();
    [[nodiscard]] bool IsRunning() const { return running_.load(); }
    
    struct Stats { uint64_t bytes_read{0}, bytes_written{0}, files_compacted{0}, compactions_completed{0}; };
    [[nodiscard]] Stats GetStats() const;
    
private:
    void BackgroundThread();
    Status DoCompaction(CompactionJob& job);
    std::optional<CompactionJob> PickCompaction();
    std::vector<FileMetaData> GetOverlappingFiles(int level, const std::string& min_key, const std::string& max_key);
    
    VersionSet* versions_;
    std::string db_path_;
    std::jthread background_thread_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::atomic<bool> running_{false}, pending_compaction_{false};
    Stats stats_;
};

} // namespace mydb
