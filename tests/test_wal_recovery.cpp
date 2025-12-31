/**
 * @file test_wal_recovery.cpp
 * @brief Unit tests for Write-Ahead Log
 */

#include <mydb/engine/wal.hpp>

#include <gtest/gtest.h>

#include <filesystem>
#include <random>

using namespace mydb;

class WALTest : public ::testing::Test {
protected:
    void SetUp() override {
        test_dir_ = std::filesystem::temp_directory_path() / "mydb_test_wal";
        std::filesystem::create_directories(test_dir_);
        wal_file_ = (test_dir_ / "test.wal").string();
    }
    
    void TearDown() override {
        std::filesystem::remove_all(test_dir_);
    }
    
    std::filesystem::path test_dir_;
    std::string wal_file_;
};

TEST_F(WALTest, WriteAndRead) {
    // Write some records
    {
        WALWriter writer(wal_file_);
        
        EXPECT_TRUE(writer.Append(OperationType::kPut, "key1", "value1", 1).ok());
        EXPECT_TRUE(writer.Append(OperationType::kPut, "key2", "value2", 2).ok());
        EXPECT_TRUE(writer.Append(OperationType::kDelete, "key1", "", 3).ok());
        EXPECT_TRUE(writer.Sync().ok());
    }
    
    // Read them back
    {
        WALReader reader(wal_file_);
        
        auto r1 = reader.ReadRecord();
        ASSERT_TRUE(r1.ok());
        EXPECT_EQ(r1.value().type, OperationType::kPut);
        EXPECT_EQ(r1.value().key, "key1");
        EXPECT_EQ(r1.value().value, "value1");
        EXPECT_EQ(r1.value().sequence, 1);
        
        auto r2 = reader.ReadRecord();
        ASSERT_TRUE(r2.ok());
        EXPECT_EQ(r2.value().key, "key2");
        EXPECT_EQ(r2.value().sequence, 2);
        
        auto r3 = reader.ReadRecord();
        ASSERT_TRUE(r3.ok());
        EXPECT_EQ(r3.value().type, OperationType::kDelete);
        EXPECT_EQ(r3.value().key, "key1");
        
        auto r4 = reader.ReadRecord();
        EXPECT_TRUE(r4.status().IsNotFound());  // EOF
    }
}

TEST_F(WALTest, CRCValidation) {
    // Write a record
    {
        WALWriter writer(wal_file_);
        EXPECT_TRUE(writer.Append(OperationType::kPut, "key", "value", 1).ok());
    }
    
    // Corrupt the file
    {
        std::fstream file(wal_file_, std::ios::binary | std::ios::in | std::ios::out);
        file.seekp(5);  // Seek past CRC to corrupt data
        char garbage = 'X';
        file.write(&garbage, 1);
    }
    
    // Try to read - should detect corruption
    {
        WALReader reader(wal_file_);
        auto result = reader.ReadRecord();
        EXPECT_TRUE(result.status().IsCorruption());
    }
}

TEST_F(WALTest, ForEachCallback) {
    // Write records
    {
        WALWriter writer(wal_file_);
        for (int i = 0; i < 100; ++i) {
            EXPECT_TRUE(writer.Append(
                OperationType::kPut,
                "key" + std::to_string(i),
                "value" + std::to_string(i),
                i + 1
            ).ok());
        }
    }
    
    // Read with ForEach
    {
        WALReader reader(wal_file_);
        int count = 0;
        
        Status s = reader.ForEach([&count](const WALRecord& record) {
            EXPECT_EQ(record.key, "key" + std::to_string(count));
            count++;
            return Status::Ok();
        });
        
        EXPECT_TRUE(s.ok());
        EXPECT_EQ(count, 100);
    }
}

TEST_F(WALTest, LargeValues) {
    std::string large_value(1024 * 1024, 'X');  // 1 MB value
    
    {
        WALWriter writer(wal_file_);
        EXPECT_TRUE(writer.Append(OperationType::kPut, "big", large_value, 1).ok());
    }
    
    {
        WALReader reader(wal_file_);
        auto result = reader.ReadRecord();
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value().value.size(), large_value.size());
        EXPECT_EQ(result.value().value, large_value);
    }
}

TEST_F(WALTest, WALManager) {
    WALManager manager(test_dir_.string());
    
    // Create multiple WAL files
    auto writer1 = manager.CreateWriter(100);
    ASSERT_TRUE(writer1.ok());
    writer1.value()->Append(OperationType::kPut, "k1", "v1", 100);
    writer1.value()->Close();
    
    auto writer2 = manager.CreateWriter(200);
    ASSERT_TRUE(writer2.ok());
    writer2.value()->Append(OperationType::kPut, "k2", "v2", 200);
    writer2.value()->Close();
    
    // Get WAL files
    auto files = manager.GetWALFiles();
    EXPECT_EQ(files.size(), 2);
    
    // Cleanup old WALs
    EXPECT_TRUE(manager.CleanupOldWALs(150).ok());
    
    files = manager.GetWALFiles();
    EXPECT_EQ(files.size(), 1);
}
