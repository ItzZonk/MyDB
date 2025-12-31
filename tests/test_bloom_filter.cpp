/**
 * @file test_bloom_filter.cpp
 * @brief Unit tests for Bloom Filter
 */

#include <mydb/engine/bloom_filter.hpp>

#include <gtest/gtest.h>

#include <random>

using namespace mydb;

class BloomFilterTest : public ::testing::Test {
protected:
};

TEST_F(BloomFilterTest, BasicFunctionality) {
    std::vector<std::string> keys = {"apple", "banana", "cherry", "date", "elderberry"};
    
    auto filter = BloomFilter::Create(keys, 10);
    
    // All inserted keys should be found
    for (const auto& key : keys) {
        EXPECT_TRUE(filter->MayContain(key));
    }
    
    // Non-inserted keys might have false positives
    EXPECT_FALSE(filter->MayContain("fig"));  // Likely to be negative
    EXPECT_FALSE(filter->MayContain("grape"));
}

TEST_F(BloomFilterTest, FalsePositiveRate) {
    const int N = 10000;
    std::vector<std::string> keys;
    
    for (int i = 0; i < N; ++i) {
        keys.push_back("key" + std::to_string(i));
    }
    
    auto filter = BloomFilter::Create(keys, 10);
    
    // All inserted keys must be found (no false negatives)
    for (const auto& key : keys) {
        EXPECT_TRUE(filter->MayContain(key));
    }
    
    // Count false positives
    int false_positives = 0;
    for (int i = N; i < 2 * N; ++i) {
        if (filter->MayContain("key" + std::to_string(i))) {
            false_positives++;
        }
    }
    
    // With 10 bits per key, theoretical FP rate is ~1%
    double fp_rate = static_cast<double>(false_positives) / N;
    EXPECT_LT(fp_rate, 0.05);  // Allow up to 5% for test stability
    
    // Check reported FP rate
    double reported_rate = filter->FalsePositiveRate();
    EXPECT_LT(reported_rate, 0.02);  // Should be close to 1%
}

TEST_F(BloomFilterTest, Serialization) {
    std::vector<std::string> keys = {"test1", "test2", "test3"};
    
    auto filter1 = BloomFilter::Create(keys, 10);
    
    // Serialize
    std::string serialized = filter1->Serialize();
    EXPECT_GT(serialized.size(), 0);
    
    // Deserialize
    auto filter2 = BloomFilter::Deserialize(serialized);
    ASSERT_NE(filter2, nullptr);
    
    // Both filters should give same results
    for (const auto& key : keys) {
        EXPECT_EQ(filter1->MayContain(key), filter2->MayContain(key));
    }
    
    EXPECT_EQ(filter1->MayContain("nothere"), filter2->MayContain("nothere"));
}

TEST_F(BloomFilterTest, EmptyFilter) {
    std::vector<std::string> keys;
    
    auto filter = BloomFilter::Create(keys, 10);
    
    // Empty filter should have minimum size
    EXPECT_GE(filter->Size(), 8);
    
    // Nothing should be found
    EXPECT_FALSE(filter->MayContain("anything"));
}

TEST_F(BloomFilterTest, MurmurHash3) {
    // Test hash function
    std::string data = "Hello, World!";
    
    uint32_t h1 = MurmurHash3_32(data.data(), data.size(), 0);
    uint32_t h2 = MurmurHash3_32(data.data(), data.size(), 0);
    
    EXPECT_EQ(h1, h2);  // Same input should give same output
    
    uint32_t h3 = MurmurHash3_32(data.data(), data.size(), 1);
    EXPECT_NE(h1, h3);  // Different seed should give different output
}
