#pragma once

#include <mydb/common/types.hpp>
#include <mydb/common/status.hpp>
#include <string>
#include <memory>
#include <optional>

namespace mydb {

class Database;

#ifdef MYDB_ENABLE_PYTHON

// Embedded Python VM with mydb module: get, put, delete, exists
class PythonVM {
public:
    explicit PythonVM(Database* db);
    ~PythonVM();
    PythonVM(const PythonVM&) = delete;
    PythonVM& operator=(const PythonVM&) = delete;
    
    Result<std::string> Execute(const std::string& script);
    Result<std::string> ExecuteWithResult(const std::string& script, const std::string& result_var = "result");
    [[nodiscard]] bool IsInitialized() const { return initialized_; }
    [[nodiscard]] const std::string& LastError() const { return last_error_; }
    Status Reset();
    
private:
    class Impl;
    std::unique_ptr<Impl> impl_;
    Database* db_;
    bool initialized_{false};
    std::string last_error_;
};

#else

class PythonVM {
public:
    explicit PythonVM(Database*) {}
    Result<std::string> Execute(const std::string&) { return Status::NotSupported("Python disabled"); }
    Result<std::string> ExecuteWithResult(const std::string&, const std::string& = "result") { return Status::NotSupported("Python disabled"); }
    [[nodiscard]] bool IsInitialized() const { return false; }
    [[nodiscard]] const std::string& LastError() const { static std::string m = "Python disabled"; return m; }
    Status Reset() { return Status::NotSupported("Python disabled"); }
};

#endif

} // namespace mydb
