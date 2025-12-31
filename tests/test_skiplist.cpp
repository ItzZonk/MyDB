/**
 * @file test_skiplist.cpp
 * @brief Unit tests for SkipList implementation
 */

#include <mydb/engine/skiplist.hpp>

#include <gtest/gtest.h>

#include <random>
#include <vector>
#include <algorithm>
#include <thread>

using namespace mydb;

class SkipListTest : public ::testing::Test {
protected:
    void SetUp() override {
        skiplist_ = std::make_unique<SkipList<std::string>>();
    }
    
    std::unique_ptr<SkipList<std::string>> skiplist_;
};

TEST_F(SkipListTest, InsertAndContains) {
    skiplist_->Insert("key1");
    skiplist_->Insert("key2");
    skiplist_->Insert("key3");
    
    EXPECT_TRUE(skiplist_->Contains("key1"));
    EXPECT_TRUE(skiplist_->Contains("key2"));
    EXPECT_TRUE(skiplist_->Contains("key3"));
    EXPECT_FALSE(skiplist_->Contains("key4"));
}

TEST_F(SkipListTest, InsertInOrder) {
    std::vector<std::string> keys = {"a", "b", "c", "d", "e"};
    
    for (const auto& key : keys) {
        skiplist_->Insert(key);
    }
    
    auto iter = skiplist_->NewIterator();
    iter.SeekToFirst();
    
    for (const auto& expected : keys) {
        ASSERT_TRUE(iter.Valid());
        EXPECT_EQ(iter.key(), expected);
        iter.Next();
    }
    
    EXPECT_FALSE(iter.Valid());
}

TEST_F(SkipListTest, InsertReverseOrder) {
    std::vector<std::string> keys = {"e", "d", "c", "b", "a"};
    
    for (const auto& key : keys) {
        skiplist_->Insert(key);
    }
    
    auto iter = skiplist_->NewIterator();
    iter.SeekToFirst();
    
    std::vector<std::string> sorted_keys = {"a", "b", "c", "d", "e"};
    for (const auto& expected : sorted_keys) {
        ASSERT_TRUE(iter.Valid());
        EXPECT_EQ(iter.key(), expected);
        iter.Next();
    }
}

TEST_F(SkipListTest, InsertRandomOrder) {
    std::vector<std::string> keys;
    for (int i = 0; i < 100; ++i) {
        keys.push_back("key" + std::to_string(i));
    }
    
    // Shuffle
    std::random_device rd;
    std::mt19937 gen(rd());
    std::shuffle(keys.begin(), keys.end(), gen);
    
    // Insert
    for (const auto& key : keys) {
        skiplist_->Insert(key);
    }
    
    // Verify all keys exist
    for (const auto& key : keys) {
        EXPECT_TRUE(skiplist_->Contains(key));
    }
    
    // Verify sorted order
    std::sort(keys.begin(), keys.end());
    
    auto iter = skiplist_->NewIterator();
    iter.SeekToFirst();
    
    for (const auto& expected : keys) {
        ASSERT_TRUE(iter.Valid());
        EXPECT_EQ(iter.key(), expected);
        iter.Next();
    }
}

TEST_F(SkipListTest, IteratorSeek) {
    for (int i = 0; i < 10; ++i) {
        skiplist_->Insert("key" + std::to_string(i));
    }
    
    auto iter = skiplist_->NewIterator();
    
    // Seek to exact key
    iter.Seek("key5");
    ASSERT_TRUE(iter.Valid());
    EXPECT_EQ(iter.key(), "key5");
    
    // Seek to key between existing keys
    iter.Seek("key55");
    ASSERT_TRUE(iter.Valid());
    EXPECT_EQ(iter.key(), "key6");  // Next key after key55 lexicographically
}

TEST_F(SkipListTest, EmptyList) {
    auto iter = skiplist_->NewIterator();
    iter.SeekToFirst();
    EXPECT_FALSE(iter.Valid());
    
    EXPECT_FALSE(skiplist_->Contains("anything"));
}

TEST_F(SkipListTest, LargeDataset) {
    const int N = 10000;
    
    for (int i = 0; i < N; ++i) {
        skiplist_->Insert("key" + std::to_string(i));
    }
    
    for (int i = 0; i < N; ++i) {
        EXPECT_TRUE(skiplist_->Contains("key" + std::to_string(i)));
    }
    
    EXPECT_FALSE(skiplist_->Contains("nonexistent"));
}

TEST_F(SkipListTest, ConcurrentReads) {
    // Insert some data
    for (int i = 0; i < 1000; ++i) {
        skiplist_->Insert("key" + std::to_string(i));
    }
    
    // Concurrent reads
    std::vector<std::thread> threads;
    std::atomic<int> found{0};
    
    for (int t = 0; t < 4; ++t) {
        threads.emplace_back([this, &found] {
            for (int i = 0; i < 1000; ++i) {
                if (skiplist_->Contains("key" + std::to_string(i))) {
                    found++;
                }
            }
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    EXPECT_EQ(found.load(), 4000);  // All reads should succeed
}
