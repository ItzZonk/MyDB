#pragma once

#include <mydb/common/slice.hpp>
#include <mydb/config.hpp>
#include <vector>
#include <string>
#include <cstdint>
#include <memory>
#include <utility>

namespace mydb {

// Bloom Filter: probabilistic set membership (false positives possible, no false negatives)
class BloomFilter {
public:
    static std::unique_ptr<BloomFilter> Create(const std::vector<std::string>& keys, size_t bits_per_key = kBloomFilterBitsPerKey);
    static std::unique_ptr<BloomFilter> Deserialize(const Slice& data);
    
    [[nodiscard]] bool MayContain(const Slice& key) const;
    [[nodiscard]] std::string Serialize() const;
    [[nodiscard]] size_t Size() const { return bits_.size(); }
    [[nodiscard]] size_t NumHashFunctions() const { return num_hashes_; }
    [[nodiscard]] double FalsePositiveRate() const;
    
    void AddKey(const Slice& key);
    BloomFilter() = default;
    
    std::vector<uint8_t>& bits() { return bits_; }
    size_t& num_hashes() { return num_hashes_; }
    size_t& num_keys() { return num_keys_; }
    
private:
    std::pair<uint32_t, uint32_t> HashKey(const Slice& key) const;
    std::vector<uint8_t> bits_;
    size_t num_hashes_{0};
    size_t num_keys_{0};
};

uint32_t MurmurHash3_32(const void* key, size_t len, uint32_t seed);
void MurmurHash3_128(const void* key, size_t len, uint32_t seed, void* out);

} // namespace mydb
