#pragma once

// WAL Binary Format: [CRC(4B)][Op(1B)][KeyLen(4B)][Key][ValLen(4B)][Val]

#include <mydb/common/types.hpp>
#include <mydb/common/status.hpp>
#include <mydb/common/slice.hpp>
#include <mydb/config.hpp>
#include <string>
#include <fstream>
#include <memory>
#include <mutex>
#include <vector>
#include <functional>

namespace mydb {

struct WALRecord {
    OperationType type;
    std::string key;
    std::string value;
    SequenceNumber sequence;
};

class WALWriter {
public:
    explicit WALWriter(const std::string& filename);
    ~WALWriter();
    WALWriter(const WALWriter&) = delete;
    WALWriter& operator=(const WALWriter&) = delete;
    
    Status Append(OperationType type, const Slice& key, const Slice& value, SequenceNumber sequence);
    Status Sync();
    Status Close();
    [[nodiscard]] size_t Size() const { return size_; }
    [[nodiscard]] const std::string& Filename() const { return filename_; }
    
private:
    std::string filename_;
    std::ofstream file_;
    size_t size_{0};
    std::vector<char> buffer_;
    Status WriteRecord(const Slice& data);
};

class WALReader {
public:
    explicit WALReader(const std::string& filename);
    ~WALReader();
    WALReader(const WALReader&) = delete;
    WALReader& operator=(const WALReader&) = delete;
    
    Result<WALRecord> ReadRecord();
    [[nodiscard]] bool HasMore() const;
    Status ForEach(std::function<Status(const WALRecord&)> callback);
    
private:
    std::string filename_;
    mutable std::ifstream file_;
    size_t offset_{0};
    Result<std::vector<char>> ReadPhysicalRecord();
};

class WALManager {
public:
    explicit WALManager(const std::string& db_path);
    ~WALManager();
    Result<std::unique_ptr<WALWriter>> CreateWriter(SequenceNumber sequence);
    std::vector<std::string> GetWALFiles() const;
    Status CleanupOldWALs(SequenceNumber min_sequence);
private:
    std::string db_path_;
    std::string GenerateFilename(SequenceNumber seq) const;
};

uint32_t CalculateCRC32(const char* data, size_t length);
bool VerifyCRC32(const char* data, size_t length, uint32_t expected);

} // namespace mydb
