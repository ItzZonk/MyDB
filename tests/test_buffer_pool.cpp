/**
 * @file test_buffer_pool.cpp
 * @brief Unit tests for Buffer Pool Manager and LRU-K Replacer
 */

#include <gtest/gtest.h>
#include <mydb/storage/buffer_pool.hpp>
#include <mydb/storage/lru_k_replacer.hpp>

#include <filesystem>
#include <thread>
#include <vector>

using namespace mydb;

// ============================================================================
// LRU-K Replacer Tests
// ============================================================================

class LRUKReplacerTest : public ::testing::Test {
protected:
    void SetUp() override {
        replacer = std::make_unique<LRUKReplacer>(5, 2);  // 5 frames, K=2
    }
    
    std::unique_ptr<LRUKReplacer> replacer;
};

TEST_F(LRUKReplacerTest, EmptyReplacer) {
    frame_id_t victim;
    EXPECT_FALSE(replacer->Evict(&victim));
    EXPECT_EQ(replacer->Size(), 0);
}

TEST_F(LRUKReplacerTest, SingleFrame) {
    replacer->RecordAccess(0);
    replacer->SetEvictable(0, true);
    
    EXPECT_EQ(replacer->Size(), 1);
    
    frame_id_t victim;
    EXPECT_TRUE(replacer->Evict(&victim));
    EXPECT_EQ(victim, 0);
    EXPECT_EQ(replacer->Size(), 0);
}

TEST_F(LRUKReplacerTest, InfiniteDistanceFirst) {
    // Frames with < K accesses have infinite distance
    // They should be evicted first (FIFO among them)
    replacer->RecordAccess(0);
    replacer->RecordAccess(1);
    replacer->SetEvictable(0, true);
    replacer->SetEvictable(1, true);
    
    frame_id_t victim;
    EXPECT_TRUE(replacer->Evict(&victim));
    EXPECT_EQ(victim, 0);  // First accessed = first evicted
}

TEST_F(LRUKReplacerTest, KDistanceEviction) {
    // Access all frames K times
    for (int i = 0; i < 3; i++) {
        replacer->RecordAccess(0);
        replacer->RecordAccess(1);
        replacer->RecordAccess(2);
    }
    
    // Access frame 0 again (most recent)
    replacer->RecordAccess(0);
    
    replacer->SetEvictable(0, true);
    replacer->SetEvictable(1, true);
    replacer->SetEvictable(2, true);
    
    // Frame 1 or 2 should be evicted (older K-th access)
    frame_id_t victim;
    EXPECT_TRUE(replacer->Evict(&victim));
    EXPECT_TRUE(victim == 1 || victim == 2);
}

TEST_F(LRUKReplacerTest, NonEvictableFrames) {
    replacer->RecordAccess(0);
    replacer->RecordAccess(1);
    
    replacer->SetEvictable(0, false);  // Pinned
    replacer->SetEvictable(1, true);
    
    frame_id_t victim;
    EXPECT_TRUE(replacer->Evict(&victim));
    EXPECT_EQ(victim, 1);  // Only evictable frame
}

TEST_F(LRUKReplacerTest, RemoveFrame) {
    replacer->RecordAccess(0);
    replacer->SetEvictable(0, true);
    
    replacer->Remove(0);
    
    frame_id_t victim;
    EXPECT_FALSE(replacer->Evict(&victim));
    EXPECT_EQ(replacer->Size(), 0);
}

// ============================================================================
// Buffer Pool Manager Tests
// ============================================================================

class BufferPoolTest : public ::testing::Test {
protected:
    void SetUp() override {
        test_file = std::filesystem::temp_directory_path() / "buffer_pool_test.db";
        std::filesystem::remove(test_file);
        
        BufferPoolConfig config;
        config.pool_size = 10;
        config.replacer_k = 2;
        config.db_file = test_file.string();
        
        bpm = std::make_unique<BufferPoolManager>(config);
    }
    
    void TearDown() override {
        bpm.reset();
        std::filesystem::remove(test_file);
    }
    
    std::filesystem::path test_file;
    std::unique_ptr<BufferPoolManager> bpm;
};

TEST_F(BufferPoolTest, NewPage) {
    page_id_t page_id;
    Page* page = bpm->NewPage(&page_id);
    
    ASSERT_NE(page, nullptr);
    EXPECT_EQ(page_id, 0);
    EXPECT_EQ(page->GetPageId(), page_id);
    
    bpm->UnpinPage(page_id, false);
}

TEST_F(BufferPoolTest, FetchPage) {
    page_id_t page_id;
    Page* page = bpm->NewPage(&page_id);
    ASSERT_NE(page, nullptr);
    
    // Write some data
    std::memcpy(page->GetData(), "Hello, World!", 13);
    bpm->UnpinPage(page_id, true);
    
    // Fetch the page again
    Page* fetched = bpm->FetchPage(page_id);
    ASSERT_NE(fetched, nullptr);
    EXPECT_EQ(std::string(fetched->GetData(), 13), "Hello, World!");
    
    bpm->UnpinPage(page_id, false);
}

TEST_F(BufferPoolTest, PersistentStorage) {
    page_id_t page_id;
    
    {
        Page* page = bpm->NewPage(&page_id);
        std::memcpy(page->GetData(), "Persistent!", 11);
        bpm->UnpinPage(page_id, true);
        bpm->FlushPage(page_id);
    }
    
    // Recreate buffer pool (simulates restart)
    bpm.reset();
    BufferPoolConfig config;
    config.pool_size = 10;
    config.replacer_k = 2;
    config.db_file = test_file.string();
    bpm = std::make_unique<BufferPoolManager>(config);
    
    // Data should persist
    Page* page = bpm->FetchPage(page_id);
    ASSERT_NE(page, nullptr);
    EXPECT_EQ(std::string(page->GetData(), 11), "Persistent!");
    
    bpm->UnpinPage(page_id, false);
}

TEST_F(BufferPoolTest, EvictionWhenFull) {
    std::vector<page_id_t> page_ids;
    
    // Fill the buffer pool
    for (int i = 0; i < 10; i++) {
        page_id_t pid;
        Page* page = bpm->NewPage(&pid);
        ASSERT_NE(page, nullptr);
        page_ids.push_back(pid);
        bpm->UnpinPage(pid, false);
    }
    
    // All pages are unpinned, so allocating more should evict
    page_id_t new_pid;
    Page* new_page = bpm->NewPage(&new_pid);
    ASSERT_NE(new_page, nullptr);
    
    bpm->UnpinPage(new_pid, false);
}

TEST_F(BufferPoolTest, DeletePage) {
    page_id_t page_id;
    Page* page = bpm->NewPage(&page_id);
    ASSERT_NE(page, nullptr);
    bpm->UnpinPage(page_id, false);
    
    EXPECT_TRUE(bpm->DeletePage(page_id));
    
    // Page should still be fetchable (just deallocated)
    // This is implementation-dependent
}

TEST_F(BufferPoolTest, FlushAllPages) {
    std::vector<page_id_t> page_ids;
    
    for (int i = 0; i < 5; i++) {
        page_id_t pid;
        Page* page = bpm->NewPage(&pid);
        std::memcpy(page->GetData(), "Test", 4);
        page_ids.push_back(pid);
        bpm->UnpinPage(pid, true);  // Mark dirty
    }
    
    // Should not throw
    EXPECT_NO_THROW(bpm->FlushAllPages());
}

// ============================================================================
// Page Tests
// ============================================================================

TEST(PageTest, SlottedPageInsert) {
    Page page;
    
    EXPECT_EQ(page.GetPageId(), INVALID_PAGE_ID);
    EXPECT_EQ(page.GetTupleCount(), 0);
    EXPECT_FALSE(page.IsDirty());
    
    // Insert a tuple
    const char* data = "Hello";
    int slot = page.InsertTuple(data, 5);
    
    EXPECT_EQ(slot, 0);
    EXPECT_EQ(page.GetTupleCount(), 1);
    EXPECT_TRUE(page.IsDirty());
}

TEST(PageTest, SlottedPageRetrieval) {
    Page page;
    
    const char* data1 = "First";
    const char* data2 = "Second";
    
    page.InsertTuple(data1, 5);
    page.InsertTuple(data2, 6);
    
    EXPECT_EQ(page.GetTupleCount(), 2);
    
    size_t size;
    const char* retrieved1 = page.GetTuple(0, &size);
    ASSERT_NE(retrieved1, nullptr);
    EXPECT_EQ(size, 5);
    EXPECT_EQ(std::string(retrieved1, size), "First");
    
    const char* retrieved2 = page.GetTuple(1, &size);
    ASSERT_NE(retrieved2, nullptr);
    EXPECT_EQ(size, 6);
    EXPECT_EQ(std::string(retrieved2, size), "Second");
}

TEST(PageTest, SlottedPageDelete) {
    Page page;
    
    page.InsertTuple("Delete me", 9);
    EXPECT_EQ(page.GetTupleCount(), 1);
    
    EXPECT_TRUE(page.DeleteTuple(0));
    
    size_t size;
    const char* data = page.GetTuple(0, &size);
    EXPECT_EQ(data, nullptr);  // Deleted
}

TEST(PageTest, ResetMemory) {
    Page page;
    
    page.InsertTuple("Test", 4);
    page.SetPageId(42);
    
    page.ResetMemory();
    
    EXPECT_EQ(page.GetPageId(), INVALID_PAGE_ID);
    EXPECT_EQ(page.GetTupleCount(), 0);
    EXPECT_FALSE(page.IsDirty());
}
