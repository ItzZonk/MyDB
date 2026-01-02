/**
 * @file wal.cpp
 * @brief Write-Ahead Log implementation
 */

#include <mydb/engine/wal.hpp>

#include <spdlog/spdlog.h>

#include <filesystem>
#include <algorithm>
#include <cstring>
#include <array>

namespace mydb {

// ============================================================================
// CRC32 Implementation (IEEE 802.3 polynomial)
// ============================================================================

namespace {
    constexpr uint32_t kCRC32Polynomial = 0xEDB88320;
    
    std::array<uint32_t, 256> GenerateCRC32Table() {
        std::array<uint32_t, 256> table{};
        for (uint32_t i = 0; i < 256; ++i) {
            uint32_t crc = i;
            for (int j = 0; j < 8; ++j) {
                crc = (crc >> 1) ^ (kCRC32Polynomial & (-(crc & 1)));
            }
            table[i] = crc;
        }
        return table;
    }
    
    const auto kCRC32Table = GenerateCRC32Table();
}

uint32_t CalculateCRC32(const char* data, size_t length) {
    uint32_t crc = 0xFFFFFFFF;
    for (size_t i = 0; i < length; ++i) {
        crc = kCRC32Table[(crc ^ static_cast<unsigned char>(data[i])) & 0xFF] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFF;
}

bool VerifyCRC32(const char* data, size_t length, uint32_t expected) {
    return CalculateCRC32(data, length) == expected;
}

// ============================================================================
// WALWriter Implementation
// ============================================================================

WALWriter::WALWriter(const std::string& filename)
    : filename_(filename) {
    file_.open(filename_, std::ios::binary | std::ios::app);
    if (!file_.is_open()) {
        spdlog::error("Failed to open WAL file: {}", filename_);
    } else {
        // Get current file size
        file_.seekp(0, std::ios::end);
        size_ = static_cast<size_t>(file_.tellp());
        spdlog::info("Opened WAL file: {}, size: {} bytes", filename_, size_);
    }
    buffer_.reserve(kWALBlockSize);
}

WALWriter::~WALWriter() {
    if (file_.is_open()) {
        Close();
    }
}

Status WALWriter::Append(OperationType type, const Slice& key, 
                          const Slice& value, SequenceNumber sequence) {
    if (!file_.is_open()) {
        return Status::IOError("WAL file not open");
    }
    
    // Build the record:
    // [CRC 4B][Seq 8B][Op 1B][KeyLen 4B][Key][ValLen 4B][Value]
    buffer_.clear();
    
    // Reserve space for CRC (will fill in later)
    buffer_.resize(4);
    
    // Sequence number
    for (int i = 0; i < 8; ++i) {
        buffer_.push_back(static_cast<char>((sequence >> (i * 8)) & 0xFF));
    }
    
    // Operation type
    buffer_.push_back(static_cast<char>(type));
    
    // Key length (4 bytes little-endian)
    uint32_t key_len = static_cast<uint32_t>(key.size());
    for (int i = 0; i < 4; ++i) {
        buffer_.push_back(static_cast<char>((key_len >> (i * 8)) & 0xFF));
    }
    
    // Key data
    buffer_.insert(buffer_.end(), key.begin(), key.end());
    
    // Value length (4 bytes little-endian)
    uint32_t val_len = static_cast<uint32_t>(value.size());
    for (int i = 0; i < 4; ++i) {
        buffer_.push_back(static_cast<char>((val_len >> (i * 8)) & 0xFF));
    }
    
    // Value data
    buffer_.insert(buffer_.end(), value.begin(), value.end());
    
    // Calculate CRC over everything after the CRC field
    uint32_t crc = CalculateCRC32(buffer_.data() + 4, buffer_.size() - 4);
    
    // Fill in CRC at the beginning
    for (int i = 0; i < 4; ++i) {
        buffer_[i] = static_cast<char>((crc >> (i * 8)) & 0xFF);
    }
    
    // Write to file
    file_.write(buffer_.data(), static_cast<std::streamsize>(buffer_.size()));
    if (!file_.good()) {
        spdlog::error("WAL Write failed! Size={}", buffer_.size());
        return Status::IOError("Failed to write to WAL");
    }
    
    // Flush to ensure data reaches OS buffer (persistence against process crash)
    file_.flush();
    
    size_ += buffer_.size();
    
    spdlog::trace("WAL::Append success. seq={}, size={}, total_size={}", 
                  sequence, buffer_.size(), size_);
    
    return Status::Ok();
}

Status WALWriter::Sync() {
    if (!file_.is_open()) {
        return Status::IOError("WAL file not open");
    }
    // No need to flush again if we flush in Append, but Sync usually implies fsync?
    // standard library doesn't give direct fsync on fstream. 
    // file_.flush() is userspace -> kernel.
    // For true disk sync, we need platform specific code, but let's stick to flush for now.
    file_.flush();
    return file_.good() ? Status::Ok() : Status::IOError("Failed to sync WAL");
}

Status WALWriter::Close() {
    if (file_.is_open()) {
        file_.flush();
        file_.close();
        spdlog::info("Closed WAL file: {}", filename_);
    }
    return Status::Ok();
}

// ============================================================================
// WALReader Implementation
// ============================================================================

WALReader::WALReader(const std::string& filename)
    : filename_(filename) {
    file_.open(filename_, std::ios::binary);
    if (!file_.is_open()) {
        spdlog::error("Failed to open WAL file for reading: {}", filename_);
    } else {
        spdlog::info("Opened WAL file for reading: {}", filename_);
    }
}

WALReader::~WALReader() {
    if (file_.is_open()) {
        file_.close();
    }
}

Result<WALRecord> WALReader::ReadRecord() {
    if (!file_.is_open()) {
        return Status::IOError("WAL file not open");
    }
    
    // Read CRC
    char crc_buf[4];
    file_.read(crc_buf, 4);
    if (file_.gcount() < 4) {
        if (file_.eof()) {
            return Status::NotFound("End of WAL");
        }
        return Status::IOError("Failed to read CRC");
    }
    
    uint32_t stored_crc = 0;
    for (int i = 0; i < 4; ++i) {
        stored_crc |= (static_cast<uint32_t>(static_cast<unsigned char>(crc_buf[i])) << (i * 8));
    }
    
    // Read sequence number
    char seq_buf[8];
    file_.read(seq_buf, 8);
    if (file_.gcount() < 8) {
        return Status::Corruption("Truncated record (sequence)");
    }
    
    SequenceNumber sequence = 0;
    for (int i = 0; i < 8; ++i) {
        sequence |= (static_cast<SequenceNumber>(static_cast<unsigned char>(seq_buf[i])) << (i * 8));
    }
    
    // Read operation type
    char op_byte;
    file_.read(&op_byte, 1);
    if (file_.gcount() < 1) {
        return Status::Corruption("Truncated record (op)");
    }
    
    // Read key length
    char key_len_buf[4];
    file_.read(key_len_buf, 4);
    if (file_.gcount() < 4) {
        return Status::Corruption("Truncated record (key_len)");
    }
    
    uint32_t key_len = 0;
    for (int i = 0; i < 4; ++i) {
        key_len |= (static_cast<uint32_t>(static_cast<unsigned char>(key_len_buf[i])) << (i * 8));
    }
    
    // Read key
    std::string key(key_len, '\0');
    file_.read(key.data(), key_len);
    if (static_cast<uint32_t>(file_.gcount()) < key_len) {
        return Status::Corruption("Truncated record (key)");
    }
    
    // Read value length
    char val_len_buf[4];
    file_.read(val_len_buf, 4);
    if (file_.gcount() < 4) {
        return Status::Corruption("Truncated record (val_len)");
    }
    
    uint32_t val_len = 0;
    for (int i = 0; i < 4; ++i) {
        val_len |= (static_cast<uint32_t>(static_cast<unsigned char>(val_len_buf[i])) << (i * 8));
    }
    
    // Read value
    std::string value(val_len, '\0');
    file_.read(value.data(), val_len);
    if (static_cast<uint32_t>(file_.gcount()) < val_len) {
        return Status::Corruption("Truncated record (value)");
    }
    
    // Verify CRC
    std::vector<char> data_for_crc;
    data_for_crc.insert(data_for_crc.end(), seq_buf, seq_buf + 8);
    data_for_crc.push_back(op_byte);
    data_for_crc.insert(data_for_crc.end(), key_len_buf, key_len_buf + 4);
    data_for_crc.insert(data_for_crc.end(), key.begin(), key.end());
    data_for_crc.insert(data_for_crc.end(), val_len_buf, val_len_buf + 4);
    data_for_crc.insert(data_for_crc.end(), value.begin(), value.end());
    
    if (!VerifyCRC32(data_for_crc.data(), data_for_crc.size(), stored_crc)) {
        return Status::Corruption("CRC mismatch in WAL record");
    }
    
    WALRecord record;
    record.type = static_cast<OperationType>(op_byte);
    record.key = std::move(key);
    record.value = std::move(value);
    record.sequence = sequence;
    
    offset_ += 4 + data_for_crc.size();
    
    return record;
}

bool WALReader::HasMore() const {
    return file_.is_open() && !file_.eof() && file_.peek() != EOF;
}

Status WALReader::ForEach(std::function<Status(const WALRecord&)> callback) {
    while (HasMore()) {
        auto result = ReadRecord();
        if (!result.ok()) {
            if (result.status().IsNotFound()) {
                break;  // EOF
            }
            return result.status();
        }
        
        Status s = callback(result.value());
        if (!s.ok()) {
            return s;
        }
    }
    return Status::Ok();
}

// ============================================================================
// WALManager Implementation
// ============================================================================

WALManager::WALManager(const std::string& db_path)
    : db_path_(db_path) {
    std::filesystem::create_directories(db_path_);
}

WALManager::~WALManager() = default;

std::string WALManager::GenerateFilename(SequenceNumber seq) const {
    return db_path_ + "/" + std::to_string(seq) + ".wal";
}

Result<std::unique_ptr<WALWriter>> WALManager::CreateWriter(SequenceNumber sequence) {
    std::string filename = GenerateFilename(sequence);
    auto writer = std::make_unique<WALWriter>(filename);
    
    if (writer->Size() == 0) {
        // Brand new file, check if it was created successfully
        // (WALWriter logs errors internally)
    }
    
    return std::move(writer);
}

std::vector<std::string> WALManager::GetWALFiles() const {
    std::vector<std::string> files;
    
    if (!std::filesystem::exists(db_path_)) {
        return files;
    }
    
    for (const auto& entry : std::filesystem::directory_iterator(db_path_)) {
        if (entry.path().extension() == ".wal") {
            files.push_back(entry.path().string());
        }
    }
    
    // Sort by sequence number numerically
    std::sort(files.begin(), files.end(), [](const std::string& a, const std::string& b) {
        auto path_a = std::filesystem::path(a);
        auto path_b = std::filesystem::path(b);
        try {
            uint64_t seq_a = std::stoull(path_a.stem().string());
            uint64_t seq_b = std::stoull(path_b.stem().string());
            return seq_a < seq_b;
        } catch (...) {
            // Fallback to string sort if parsing fails
            return a < b;
        }
    });
    
    return files;
}

Status WALManager::CleanupOldWALs(SequenceNumber min_sequence) {
    for (const auto& file : GetWALFiles()) {
        // Extract sequence number from filename
        auto filename = std::filesystem::path(file).stem().string();
        try {
            SequenceNumber seq = std::stoull(filename);
            if (seq < min_sequence) {
                std::filesystem::remove(file);
                spdlog::info("Deleted obsolete WAL: {}", file);
            }
        } catch (...) {
            // Not a valid WAL filename, skip
        }
    }
    return Status::Ok();
}

} // namespace mydb
