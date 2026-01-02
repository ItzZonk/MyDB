#pragma once

// SSTable Layout: [Data Blocks][Index Block][Bloom Filter][Footer]

#include <mydb/common/types.hpp>
#include <mydb/common/status.hpp>
#include <mydb/common/slice.hpp>
#include <mydb/config.hpp>
#include <string>
#include <vector>
#include <memory>
#include <fstream>

namespace mydb {

class BloomFilter;

struct SSTableFooter {
    static constexpr size_t kEncodedLength = 60;
    uint64_t data_block_offset, data_block_size;
    uint64_t index_block_offset, index_block_size;
    uint64_t bloom_filter_offset, bloom_filter_size;
    uint64_t entry_count;
    uint32_t magic_number;
    std::string Encode() const;
    static Result<SSTableFooter> Decode(const Slice& data);
};

struct IndexEntry {
    std::string first_key;
    uint64_t block_offset, block_size;
    std::string Encode() const;
    static Result<IndexEntry> Decode(const Slice& data);
};

class SSTableIterator {
public:
    virtual ~SSTableIterator() = default;
    virtual bool Valid() const = 0;
    virtual void SeekToFirst() = 0;
    virtual void SeekToLast() = 0;
    virtual void Seek(const Slice& key) = 0;
    virtual void Next() = 0;
    virtual void Prev() = 0;
    virtual Slice key() const = 0;
    virtual Slice value() const = 0;
};

class SSTableBuilder {
public:
    struct Options {
        size_t block_size = kSSTableBlockSize;
        size_t index_interval = kIndexBlockInterval;
        size_t bloom_bits_per_key = kBloomFilterBitsPerKey;
    };
    
    SSTableBuilder(const std::string& filename, const Options& options = {});
    ~SSTableBuilder();
    SSTableBuilder(const SSTableBuilder&) = delete;
    SSTableBuilder& operator=(const SSTableBuilder&) = delete;
    
    Status Add(const Slice& key, const Slice& value);
    Status Finish();
    void Abandon();
    [[nodiscard]] uint64_t NumEntries() const { return num_entries_; }
    [[nodiscard]] uint64_t FileSize() const { return offset_; }
    
private:
    Status FlushBlock();
    Status WriteIndexBlock();
    Status WriteBloomFilter();
    Status WriteFooter();
    
    std::string filename_;
    Options options_;
    std::ofstream file_;
    std::string data_block_, first_key_in_block_;
    int entries_in_block_{0};
    std::vector<IndexEntry> index_entries_;
    std::unique_ptr<BloomFilter> bloom_filter_;
    std::vector<std::string> keys_for_bloom_;
    uint64_t num_entries_{0}, offset_{0};
    uint64_t data_start_offset_{0}, index_offset_{0}, bloom_offset_{0};
    bool finished_{false};
};

class SSTableReader {
public:
    static Result<std::unique_ptr<SSTableReader>> Open(const std::string& filename);
    ~SSTableReader();
    SSTableReader(const SSTableReader&) = delete;
    SSTableReader& operator=(const SSTableReader&) = delete;
    
    Result<std::string> Get(const Slice& key);
    [[nodiscard]] bool MayContain(const Slice& key) const;
    std::unique_ptr<SSTableIterator> NewIterator() const;
    
    [[nodiscard]] uint64_t NumEntries() const { return footer_.entry_count; }
    [[nodiscard]] uint64_t FileSize() const { return file_size_; }
    [[nodiscard]] const std::string& Filename() const { return filename_; }
    [[nodiscard]] const std::string& SmallestKey() const { return smallest_key_; }
    [[nodiscard]] const std::string& LargestKey() const { return largest_key_; }
    
    friend class SSTableIteratorImpl;
    Result<size_t> FindBlock(const Slice& key) const;
    Result<std::string> ReadBlock(uint64_t offset, uint64_t size) const;
    const std::vector<IndexEntry>& GetIndex() const { return index_; }
    
private:
    explicit SSTableReader(const std::string& filename);
    Status Initialize();
    Status ReadFooter();
    Status ReadIndex();
    Status ReadBloomFilter();
    
    std::string filename_;
    mutable std::ifstream file_;
    uint64_t file_size_{0};
    SSTableFooter footer_;
    std::vector<IndexEntry> index_;
    std::unique_ptr<BloomFilter> bloom_filter_;
    std::string smallest_key_, largest_key_;
};

class SSTableMerger {
public:
    static Status Merge(const std::vector<SSTableReader*>& inputs, const std::string& output_filename);
};

} // namespace mydb
