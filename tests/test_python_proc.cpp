/**
 * @file test_python_proc.cpp
 * @brief Unit tests for Python stored procedures
 */

#include <mydb/db.hpp>
#include <mydb/scripting/python_vm.hpp>

#include <gtest/gtest.h>

#include <filesystem>

using namespace mydb;

class PythonProcTest : public ::testing::Test {
protected:
    void SetUp() override {
        test_dir_ = std::filesystem::temp_directory_path() / "mydb_test_python";
        std::filesystem::remove_all(test_dir_);
        
        Options options;
        options.db_path = test_dir_.string();
        options.create_if_missing = true;
        options.enable_python = true;
        
        auto result = Database::Open(options);
        if (result.ok()) {
            db_ = std::move(result.value());
        }
    }
    
    void TearDown() override {
        db_.reset();
        std::filesystem::remove_all(test_dir_);
    }
    
    std::filesystem::path test_dir_;
    std::unique_ptr<Database> db_;
};

#ifdef MYDB_ENABLE_PYTHON

TEST_F(PythonProcTest, SimpleScript) {
    ASSERT_NE(db_, nullptr);
    
    auto result = db_->ExecutePython("x = 1 + 1");
    EXPECT_TRUE(result.ok());
}

TEST_F(PythonProcTest, PutAndGet) {
    ASSERT_NE(db_, nullptr);
    
    // Put via Python
    auto result = db_->ExecutePython(R"(
import mydb
mydb.put("python_key", "python_value")
)");
    EXPECT_TRUE(result.ok());
    
    // Verify via C++
    auto value = db_->Get("python_key");
    ASSERT_TRUE(value.ok());
    EXPECT_EQ(value.value(), "python_value");
}

TEST_F(PythonProcTest, GetFromPython) {
    ASSERT_NE(db_, nullptr);
    
    // Put via C++
    EXPECT_TRUE(db_->Put("test_key", "test_value").ok());
    
    // Get via Python
    auto result = db_->ExecutePython(R"(
import mydb
result = mydb.get("test_key")
)");
    EXPECT_TRUE(result.ok());
}

TEST_F(PythonProcTest, ExistsCheck) {
    ASSERT_NE(db_, nullptr);
    
    db_->Put("exists_key", "value");
    
    auto result = db_->ExecutePython(R"(
import mydb
assert mydb.exists("exists_key") == True
assert mydb.exists("nonexistent") == False
)");
    EXPECT_TRUE(result.ok());
}

TEST_F(PythonProcTest, DeleteKey) {
    ASSERT_NE(db_, nullptr);
    
    db_->Put("to_delete", "value");
    EXPECT_TRUE(db_->Exists("to_delete"));
    
    auto result = db_->ExecutePython(R"(
import mydb
mydb.delete("to_delete")
)");
    EXPECT_TRUE(result.ok());
    
    EXPECT_FALSE(db_->Exists("to_delete"));
}

TEST_F(PythonProcTest, FibonacciExample) {
    ASSERT_NE(db_, nullptr);
    
    // Store Fibonacci sequence in the database
    auto result = db_->ExecutePython(R"(
import mydb

def fibonacci(n):
    if n <= 1:
        return n
    return fibonacci(n - 1) + fibonacci(n - 2)

# Store first 10 Fibonacci numbers
for i in range(10):
    mydb.put(f"fib_{i}", str(fibonacci(i)))
)");
    EXPECT_TRUE(result.ok());
    
    // Verify results
    EXPECT_EQ(db_->Get("fib_0").value(), "0");
    EXPECT_EQ(db_->Get("fib_1").value(), "1");
    EXPECT_EQ(db_->Get("fib_2").value(), "1");
    EXPECT_EQ(db_->Get("fib_3").value(), "2");
    EXPECT_EQ(db_->Get("fib_4").value(), "3");
    EXPECT_EQ(db_->Get("fib_5").value(), "5");
}

TEST_F(PythonProcTest, ErrorHandling) {
    ASSERT_NE(db_, nullptr);
    
    // Syntax error
    auto result = db_->ExecutePython("this is not valid python");
    EXPECT_FALSE(result.ok());
    
    // Runtime error
    result = db_->ExecutePython("x = 1 / 0");
    EXPECT_FALSE(result.ok());
}

#else

TEST_F(PythonProcTest, PythonDisabled) {
    // When Python is disabled, these tests just verify the stubs work
    PythonVM vm(nullptr);
    EXPECT_FALSE(vm.IsInitialized());
    
    auto result = vm.Execute("test");
    EXPECT_FALSE(result.ok());
    EXPECT_TRUE(result.status().IsNotSupported());
}

#endif
