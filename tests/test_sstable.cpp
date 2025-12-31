/**
 * @file test_sstable.cpp
 * @brief Unit tests for SSTable builder and reader
 */

#include <mydb/engine/sstable.hpp>

#include <gtest/gtest.h>

#include <filesystem>

using namespace mydb;

class SSTableTest : public ::testing::Test {
protected:
    void SetUp() override {
        test_dir_ = std::filesystem::temp_directory_path() / "mydb_test_sst";
        std::filesystem::create_directories(test_dir_);
        sst_file_ = (test_dir_ / "test.sst").string();
    }
    
    void TearDown() override {
        std::filesystem::remove_all(test_dir_);
    }
    
    std::filesystem::path test_dir_;
    std::string sst_file_;
};

TEST_F(SSTableTest, BuildAndRead) {
    // Build SSTable
    {
        SSTableBuilder builder(sst_file_);
        
        EXPECT_TRUE(builder.Add("key1", "value1").ok());
        EXPECT_TRUE(builder.Add("key2", "value2").ok());
        EXPECT_TRUE(builder.Add("key3", "value3").ok());
        
        EXPECT_TRUE(builder.Finish().ok());
        EXPECT_EQ(builder.NumEntries(), 3);
    }
    
    // Read SSTable
    {
        auto reader_result = SSTableReader::Open(sst_file_);
        ASSERT_TRUE(reader_result.ok());
        
        auto& reader = reader_result.value();
        EXPECT_EQ(reader->NumEntries(), 3);
        
        // Point lookups
        auto v1 = reader->Get("key1");
        ASSERT_TRUE(v1.ok());
        EXPECT_EQ(v1.value(), "value1");
        
        auto v2 = reader->Get("key2");
        ASSERT_TRUE(v2.ok());
        EXPECT_EQ(v2.value(), "value2");
        
        auto v3 = reader->Get("key3");
        ASSERT_TRUE(v3.ok());
        EXPECT_EQ(v3.value(), "value3");
        
        // Non-existent key
        auto v4 = reader->Get("key4");
        EXPECT_TRUE(v4.status().IsNotFound());
    }
}

TEST_F(SSTableTest, Iterator) {
    // Build
    {
        SSTableBuilder builder(sst_file_);
        for (int i = 0; i < 100; ++i) {
            char key[16], value[16];
            snprintf(key, sizeof(key), "key%05d", i);
            snprintf(value, sizeof(value), "val%05d", i);
            EXPECT_TRUE(builder.Add(key, value).ok());
        }
        EXPECT_TRUE(builder.Finish().ok());
    }
    
    // Iterate
    {
        auto reader_result = SSTableReader::Open(sst_file_);
        ASSERT_TRUE(reader_result.ok());
        
        auto iter = reader_result.value()->NewIterator();
        iter->SeekToFirst();
        
        int count = 0;
        while (iter->Valid()) {
            char expected_key[16], expected_value[16];
            snprintf(expected_key, sizeof(expected_key), "key%05d", count);
            snprintf(expected_value, sizeof(expected_value), "val%05d", count);
            
            EXPECT_EQ(iter->key().ToString(), expected_key);
            EXPECT_EQ(iter->value().ToString(), expected_value);
            
            iter->Next();
            count++;
        }
        
        EXPECT_EQ(count, 100);
    }
}

TEST_F(SSTableTest, BloomFilter) {
    // Build with bloom filter
    {
        SSTableBuilder::Options opts;
        opts.bloom_bits_per_key = 10;
        
        SSTableBuilder builder(sst_file_, opts);
        for (int i = 0; i < 1000; ++i) {
            EXPECT_TRUE(builder.Add("key" + std::to_string(i), "value").ok());
        }
        EXPECT_TRUE(builder.Finish().ok());
    }
    
    // Check bloom filter
    {
        auto reader_result = SSTableReader::Open(sst_file_);
        ASSERT_TRUE(reader_result.ok());
        
        auto& reader = reader_result.value();
        
        // Existing keys should pass bloom filter
        for (int i = 0; i < 1000; ++i) {
            EXPECT_TRUE(reader->MayContain("key" + std::to_string(i)));
        }
        
        // Non-existing keys might have false positives, but most should fail
        int false_positives = 0;
        for (int i = 1000; i < 2000; ++i) {
            if (reader->MayContain("key" + std::to_string(i))) {
                false_positives++;
            }
        }
        
        // With 10 bits per key, false positive rate should be ~1%
        EXPECT_LT(false_positives, 50);  // Allow up to 5% for test stability
    }
}

TEST_F(SSTableTest, LargeSSTable) {
    const int N = 10000;
    
    // Build
    {
        SSTableBuilder builder(sst_file_);
        for (int i = 0; i < N; ++i) {
            char key[16], value[256];
            snprintf(key, sizeof(key), "k%08d", i);
            snprintf(value, sizeof(value), "This is value number %d with some extra text", i);
            EXPECT_TRUE(builder.Add(key, value).ok());
        }
        EXPECT_TRUE(builder.Finish().ok());
    }
    
    // Read
    {
        auto reader_result = SSTableReader::Open(sst_file_);
        ASSERT_TRUE(reader_result.ok());
        
        auto& reader = reader_result.value();
        EXPECT_EQ(reader->NumEntries(), N);
        
        // Random lookups
        for (int i = 0; i < 100; ++i) {
            int idx = rand() % N;
            char key[16];
            snprintf(key, sizeof(key), "k%08d", idx);
            
            auto result = reader->Get(key);
            EXPECT_TRUE(result.ok());
        }
    }
}
