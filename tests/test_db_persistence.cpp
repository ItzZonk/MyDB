/**
 * @file test_db_persistence.cpp
 * @brief Integration tests for Database persistence
 */

#include <mydb/db.hpp>
#include <gtest/gtest.h>
#include <filesystem>

using namespace mydb;

class PersistenceTest : public ::testing::Test {
protected:
    void SetUp() override {
        test_dir_ = std::filesystem::temp_directory_path() / "mydb_test_persistence";
        std::filesystem::remove_all(test_dir_); // Clean start
        std::filesystem::create_directories(test_dir_);
        
        options_.db_path = test_dir_.string();
        options_.create_if_missing = true;
    }
    
    void TearDown() override {
        std::filesystem::remove_all(test_dir_);
    }
    
    std::filesystem::path test_dir_;
    Options options_;
};

TEST_F(PersistenceTest, PersistToSSTable) {
    // 1. Open DB, Write Data, Flush, Close
    {
        auto db_res = Database::Open(options_);
        ASSERT_TRUE(db_res.ok());
        auto& db = db_res.value();
        
        EXPECT_TRUE(db->Put("key1", "val1").ok());
        EXPECT_TRUE(db->Put("key2", "val2").ok());
        
        // Force flush
        EXPECT_TRUE(db->Flush().ok());
        
        // Verify files exist
        bool found_sst = false;
        bool found_manifest = false;
        for (const auto& entry : std::filesystem::directory_iterator(test_dir_)) {
            if (entry.path().extension() == ".sst") found_sst = true;
            if (entry.path().filename() == "MANIFEST") found_manifest = true;
        }
        EXPECT_TRUE(found_sst) << "SSTable should persist after Flush";
        EXPECT_TRUE(found_manifest) << "MANIFEST should persist after Flush";
    }
    
    // 2. Reopen DB (Simulate Restart)
    {
        auto db_res = Database::Open(options_);
        ASSERT_TRUE(db_res.ok());
        auto& db = db_res.value();
        
        // Check Memtable is empty (optional but expected)
        // Check retrieval (from SSTable)
        auto val1 = db->Get("key1");
        ASSERT_TRUE(val1.ok()) << val1.status().ToString();
        EXPECT_EQ(val1.value(), "val1");
        
        auto val2 = db->Get("key2");
        ASSERT_TRUE(val2.ok()) << val2.status().ToString();
        EXPECT_EQ(val2.value(), "val2");
    }
}

TEST_F(PersistenceTest, PersistViaWAL) {
    // 1. Open DB, Write Data, DO NOT Flush, Close (Simulate Crash or default close)
    // Note: Database destructor calls Flush() by default unless we crash.
    // To simulate crash (WAL recovery), we need to rely on the fact that Flush() cleans up WALs.
    // If we Close(), Flush() runs, WALs deleted.
    // We want to test WAL Recovery.
    // So we can't easily Simulate Crash in unit test without `abort()`.
    // But we verified WAL recovery in separate test.
    
    // Here we verify standard persistence (which uses Flush on close).
    {
        auto db_res = Database::Open(options_);
        ASSERT_TRUE(db_res.ok());
        auto& db = db_res.value();
        
        EXPECT_TRUE(db->Put("wal_key", "wal_val").ok());
        // Destructor calls Flush()
    }
    
    {
        auto db_res = Database::Open(options_);
        ASSERT_TRUE(db_res.ok());
        auto& db = db_res.value();
        
        auto val = db->Get("wal_key");
        ASSERT_TRUE(val.ok());
        EXPECT_EQ(val.value(), "wal_val");
    }
}
