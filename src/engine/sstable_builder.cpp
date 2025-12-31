/**
 * @file sstable_builder.cpp
 * @brief SSTable builder implementation
 */

#include <mydb/engine/sstable.hpp>
#include <mydb/engine/bloom_filter.hpp>

#include <spdlog/spdlog.h>

#include <cstring>
#include <filesystem>

namespace mydb {

// ============================================================================
// SSTableFooter Implementation
// ============================================================================

std::string SSTableFooter::Encode() const {
    std::string result;
    result.reserve(kEncodedLength);
    
    auto encode_u64 = [&result](uint64_t v) {
        for (int i = 0; i < 8; ++i) {
            result.push_back(static_cast<char>((v >> (i * 8)) & 0xFF));
        }
    };
    
    auto encode_u32 = [&result](uint32_t v) {
        for (int i = 0; i < 4; ++i) {
            result.push_back(static_cast<char>((v >> (i * 8)) & 0xFF));
        }
    };
    
    encode_u64(data_block_offset);
    encode_u64(data_block_size);
    encode_u64(index_block_offset);
    encode_u64(index_block_size);
    encode_u64(bloom_filter_offset);
    encode_u64(bloom_filter_size);
    encode_u64(entry_count);
    encode_u32(magic_number);
    
    return result;
}

Result<SSTableFooter> SSTableFooter::Decode(const Slice& data) {
    if (data.size() < kEncodedLength) {
        return Status::Corruption("Footer too short");
    }
    
    size_t offset = 0;
    
    auto decode_u64 = [&data, &offset]() {
        uint64_t v = 0;
        for (int i = 0; i < 8; ++i) {
            v |= (static_cast<uint64_t>(static_cast<unsigned char>(data[offset + i])) << (i * 8));
        }
        offset += 8;
        return v;
    };
    
    auto decode_u32 = [&data, &offset]() {
        uint32_t v = 0;
        for (int i = 0; i < 4; ++i) {
            v |= (static_cast<uint32_t>(static_cast<unsigned char>(data[offset + i])) << (i * 8));
        }
        offset += 4;
        return v;
    };
    
    SSTableFooter footer;
    footer.data_block_offset = decode_u64();
    footer.data_block_size = decode_u64();
    footer.index_block_offset = decode_u64();
    footer.index_block_size = decode_u64();
    footer.bloom_filter_offset = decode_u64();
    footer.bloom_filter_size = decode_u64();
    footer.entry_count = decode_u64();
    footer.magic_number = decode_u32();
    
    if (footer.magic_number != kMagicNumber) {
        return Status::Corruption("Invalid magic number");
    }
    
    return footer;
}

// ============================================================================
// IndexEntry Implementation
// ============================================================================

std::string IndexEntry::Encode() const {
    std::string result;
    
    // Key length + key + offset + size
    uint32_t key_len = static_cast<uint32_t>(first_key.size());
    for (int i = 0; i < 4; ++i) {
        result.push_back(static_cast<char>((key_len >> (i * 8)) & 0xFF));
    }
    result.append(first_key);
    
    for (int i = 0; i < 8; ++i) {
        result.push_back(static_cast<char>((block_offset >> (i * 8)) & 0xFF));
    }
    for (int i = 0; i < 8; ++i) {
        result.push_back(static_cast<char>((block_size >> (i * 8)) & 0xFF));
    }
    
    return result;
}

Result<IndexEntry> IndexEntry::Decode(const Slice& data) {
    if (data.size() < 20) {  // Minimum: 4 + 0 + 8 + 8
        return Status::Corruption("Index entry too short");
    }
    
    size_t offset = 0;
    
    uint32_t key_len = 0;
    for (int i = 0; i < 4; ++i) {
        key_len |= (static_cast<uint32_t>(static_cast<unsigned char>(data[offset + i])) << (i * 8));
    }
    offset += 4;
    
    if (offset + key_len + 16 > data.size()) {
        return Status::Corruption("Index entry truncated");
    }
    
    IndexEntry entry;
    entry.first_key = std::string(data.data() + offset, key_len);
    offset += key_len;
    
    entry.block_offset = 0;
    for (int i = 0; i < 8; ++i) {
        entry.block_offset |= (static_cast<uint64_t>(static_cast<unsigned char>(data[offset + i])) << (i * 8));
    }
    offset += 8;
    
    entry.block_size = 0;
    for (int i = 0; i < 8; ++i) {
        entry.block_size |= (static_cast<uint64_t>(static_cast<unsigned char>(data[offset + i])) << (i * 8));
    }
    
    return entry;
}

// ============================================================================
// SSTableBuilder Implementation
// ============================================================================

SSTableBuilder::SSTableBuilder(const std::string& filename, const Options& options)
    : filename_(filename)
    , options_(options) {
    file_.open(filename_, std::ios::binary | std::ios::trunc);
    if (!file_.is_open()) {
        spdlog::error("Failed to create SSTable: {}", filename_);
    }
    data_start_offset_ = 0;
    spdlog::debug("SSTableBuilder created: {}", filename_);
}

SSTableBuilder::~SSTableBuilder() {
    if (!finished_ && file_.is_open()) {
        Abandon();
    }
}

Status SSTableBuilder::Add(const Slice& key, const Slice& value) {
    if (finished_) {
        return Status::InvalidArgument("SSTable already finished");
    }
    
    if (!file_.is_open()) {
        return Status::IOError("SSTable file not open");
    }
    
    // If starting a new block, record the first key
    if (entries_in_block_ == 0) {
        first_key_in_block_ = key.ToString();
    }
    
    // Store key for bloom filter
    keys_for_bloom_.push_back(key.ToString());
    
    // Encode key-value pair:
    // [KeyLen 4B][Key][ValLen 4B][Value]
    uint32_t key_len = static_cast<uint32_t>(key.size());
    uint32_t val_len = static_cast<uint32_t>(value.size());
    
    for (int i = 0; i < 4; ++i) {
        data_block_.push_back(static_cast<char>((key_len >> (i * 8)) & 0xFF));
    }
    data_block_.append(key.data(), key.size());
    
    for (int i = 0; i < 4; ++i) {
        data_block_.push_back(static_cast<char>((val_len >> (i * 8)) & 0xFF));
    }
    data_block_.append(value.data(), value.size());
    
    num_entries_++;
    entries_in_block_++;
    
    // Flush block if it's large enough
    if (data_block_.size() >= options_.block_size) {
        MYDB_RETURN_IF_ERROR(FlushBlock());
    }
    
    return Status::Ok();
}

Status SSTableBuilder::FlushBlock() {
    if (data_block_.empty()) {
        return Status::Ok();
    }
    
    // Record index entry for this block
    IndexEntry entry;
    entry.first_key = std::move(first_key_in_block_);
    entry.block_offset = offset_;
    entry.block_size = data_block_.size();
    index_entries_.push_back(std::move(entry));
    
    // Write the block
    file_.write(data_block_.data(), static_cast<std::streamsize>(data_block_.size()));
    if (!file_.good()) {
        return Status::IOError("Failed to write data block");
    }
    
    offset_ += data_block_.size();
    data_block_.clear();
    entries_in_block_ = 0;
    
    return Status::Ok();
}

Status SSTableBuilder::WriteIndexBlock() {
    index_offset_ = offset_;
    
    std::string index_data;
    
    // Encode number of entries
    uint32_t num_entries = static_cast<uint32_t>(index_entries_.size());
    for (int i = 0; i < 4; ++i) {
        index_data.push_back(static_cast<char>((num_entries >> (i * 8)) & 0xFF));
    }
    
    // Encode each entry
    for (const auto& entry : index_entries_) {
        std::string encoded = entry.Encode();
        index_data.append(encoded);
    }
    
    file_.write(index_data.data(), static_cast<std::streamsize>(index_data.size()));
    if (!file_.good()) {
        return Status::IOError("Failed to write index block");
    }
    
    offset_ += index_data.size();
    
    return Status::Ok();
}

Status SSTableBuilder::WriteBloomFilter() {
    bloom_offset_ = offset_;
    
    auto bloom = BloomFilter::Create(keys_for_bloom_, options_.bloom_bits_per_key);
    std::string bloom_data = bloom->Serialize();
    
    file_.write(bloom_data.data(), static_cast<std::streamsize>(bloom_data.size()));
    if (!file_.good()) {
        return Status::IOError("Failed to write bloom filter");
    }
    
    offset_ += bloom_data.size();
    
    return Status::Ok();
}

Status SSTableBuilder::WriteFooter() {
    SSTableFooter footer;
    footer.data_block_offset = data_start_offset_;
    footer.data_block_size = index_offset_ - data_start_offset_;
    footer.index_block_offset = index_offset_;
    footer.index_block_size = bloom_offset_ - index_offset_;
    footer.bloom_filter_offset = bloom_offset_;
    footer.bloom_filter_size = offset_ - bloom_offset_;
    footer.entry_count = num_entries_;
    footer.magic_number = kMagicNumber;
    
    std::string footer_data = footer.Encode();
    file_.write(footer_data.data(), static_cast<std::streamsize>(footer_data.size()));
    if (!file_.good()) {
        return Status::IOError("Failed to write footer");
    }
    
    offset_ += footer_data.size();
    
    return Status::Ok();
}

Status SSTableBuilder::Finish() {
    if (finished_) {
        return Status::InvalidArgument("Already finished");
    }
    
    // Flush any remaining data block
    MYDB_RETURN_IF_ERROR(FlushBlock());
    
    // Write index block
    MYDB_RETURN_IF_ERROR(WriteIndexBlock());
    
    // Write bloom filter
    MYDB_RETURN_IF_ERROR(WriteBloomFilter());
    
    // Write footer
    MYDB_RETURN_IF_ERROR(WriteFooter());
    
    file_.flush();
    file_.close();
    
    finished_ = true;
    spdlog::info("SSTable finished: {}, {} entries, {} bytes", 
                 filename_, num_entries_, offset_);
    
    return Status::Ok();
}

void SSTableBuilder::Abandon() {
    if (file_.is_open()) {
        file_.close();
    }
    std::filesystem::remove(filename_);
    finished_ = true;
    spdlog::debug("SSTable abandoned: {}", filename_);
}

} // namespace mydb
