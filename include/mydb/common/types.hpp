#pragma once

#include <cstdint>
#include <string>
#include <chrono>
#include <functional>

namespace mydb {

using Key = std::string;
using Value = std::string;
using SequenceNumber = uint64_t;
using Timestamp = uint64_t;

enum class OperationType : uint8_t { kPut = 0x01, kDelete = 0x02 };

// Internal key: [user_key][sequence (7 bytes)][type (1 byte)]
struct InternalKey {
    Key user_key;
    SequenceNumber sequence;
    OperationType type;
    
    InternalKey() = default;
    InternalKey(Key k, SequenceNumber seq, OperationType t) : user_key(std::move(k)), sequence(seq), type(t) {}
    
    bool operator<(const InternalKey& o) const {
        if (user_key != o.user_key) return user_key < o.user_key;
        return sequence > o.sequence; // newer first
    }
    
    bool operator==(const InternalKey& o) const {
        return user_key == o.user_key && sequence == o.sequence && type == o.type;
    }
};

struct KeyValueEntry {
    InternalKey key;
    Value value;
    size_t Size() const { return key.user_key.size() + value.size() + sizeof(SequenceNumber) + 1; }
};

inline Timestamp CurrentTimestamp() {
    return static_cast<Timestamp>(std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
}

struct KeyHash {
    size_t operator()(const Key& key) const { return std::hash<std::string>{}(key); }
};

enum class FileType { kSSTable, kWAL, kManifest, kTemp, kLock };

struct Options {
    std::string db_path = "./mydb_data";
    bool create_if_missing = true;
    bool error_if_exists = false;
    size_t memtable_size = 64 * 1024 * 1024;
    bool enable_wal = true;
    bool sync_writes = false;
    int compaction_threads = 2;
    bool enable_bloom_filter = true;
    size_t bloom_bits_per_key = 10;
    uint16_t port = 6379;
    size_t max_connections = 1024;
    bool enable_python = true;
};

struct ReadOptions {
    SequenceNumber snapshot = 0;
    bool fill_cache = true;
    bool verify_checksums = true;
};

struct WriteOptions {
    bool sync = false;
    bool disable_wal = false;
};

} // namespace mydb
