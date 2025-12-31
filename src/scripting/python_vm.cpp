/**
 * @file python_vm.cpp
 * @brief Embedded Python interpreter implementation
 */

#include <mydb/scripting/python_vm.hpp>
#include <mydb/db.hpp>

#include <spdlog/spdlog.h>

#ifdef MYDB_ENABLE_PYTHON

#include <pybind11/embed.h>
#include <pybind11/stl.h>

namespace py = pybind11;

namespace mydb {

// ============================================================================
// Python Module Definition
// ============================================================================

// Forward declaration of the Database class for Python bindings
class PyDatabaseWrapper {
public:
    explicit PyDatabaseWrapper(Database* db) : db_(db) {}
    
    py::object get(const std::string& key) {
        auto result = db_->Get(key);
        if (result.ok()) {
            return py::str(result.value());
        }
        return py::none();
    }
    
    void put(const std::string& key, const std::string& value) {
        Status s = db_->Put(key, value);
        if (!s.ok()) {
            throw std::runtime_error(s.ToString());
        }
    }
    
    void del(const std::string& key) {
        Status s = db_->Delete(key);
        if (!s.ok()) {
            throw std::runtime_error(s.ToString());
        }
    }
    
    bool exists(const std::string& key) {
        return db_->Exists(key);
    }
    
    py::dict stats() {
        auto s = db_->GetStats();
        py::dict result;
        result["entries"] = s.num_entries;
        result["memtable_size"] = s.memtable_size;
        result["sstables"] = s.num_sstables;
        result["disk_usage"] = s.disk_usage;
        result["reads"] = s.reads;
        result["writes"] = s.writes;
        return result;
    }
    
private:
    Database* db_;
};

// ============================================================================
// PythonVM Implementation
// ============================================================================

class PythonVM::Impl {
public:
    explicit Impl(Database* db) : db_(db), wrapper_(db) {
        try {
            // Start the Python interpreter
            guard_ = std::make_unique<py::scoped_interpreter>();
            
            // Create the mydb module
            py::module_ sys = py::module_::import("sys");
            
            // Add custom module
            auto mydb_module = py::module_::create_extension_module(
                "mydb", "MyDB Python bindings", new py::module_::module_def
            );
            
            // Bind functions
            mydb_module.def("get", [this](const std::string& key) {
                return wrapper_.get(key);
            }, "Get a value by key");
            
            mydb_module.def("put", [this](const std::string& key, const std::string& value) {
                wrapper_.put(key, value);
            }, "Put a key-value pair");
            
            mydb_module.def("delete", [this](const std::string& key) {
                wrapper_.del(key);
            }, "Delete a key");
            
            mydb_module.def("exists", [this](const std::string& key) {
                return wrapper_.exists(key);
            }, "Check if a key exists");
            
            mydb_module.def("stats", [this]() {
                return wrapper_.stats();
            }, "Get database statistics");
            
            // Register the module
            py::dict modules = sys.attr("modules");
            modules["mydb"] = mydb_module;
            
            spdlog::info("Python VM initialized successfully");
            initialized_ = true;
            
        } catch (const py::error_already_set& e) {
            last_error_ = e.what();
            spdlog::error("Failed to initialize Python: {}", last_error_);
            initialized_ = false;
        }
    }
    
    ~Impl() = default;
    
    Result<std::string> Execute(const std::string& script) {
        if (!initialized_) {
            return Status::NotSupported("Python not initialized: " + last_error_);
        }
        
        try {
            // Execute the script
            py::exec(script);
            return std::string("OK");
            
        } catch (const py::error_already_set& e) {
            last_error_ = e.what();
            return Status::InvalidArgument(last_error_);
        } catch (const std::exception& e) {
            last_error_ = e.what();
            return Status::InvalidArgument(last_error_);
        }
    }
    
    Result<std::string> ExecuteWithResult(const std::string& script, 
                                          const std::string& result_var) {
        if (!initialized_) {
            return Status::NotSupported("Python not initialized: " + last_error_);
        }
        
        try {
            py::dict locals;
            py::exec(script, py::globals(), locals);
            
            if (locals.contains(result_var.c_str())) {
                py::object result = locals[result_var.c_str()];
                return py::str(result).cast<std::string>();
            }
            
            return std::string("");
            
        } catch (const py::error_already_set& e) {
            last_error_ = e.what();
            return Status::InvalidArgument(last_error_);
        } catch (const std::exception& e) {
            last_error_ = e.what();
            return Status::InvalidArgument(last_error_);
        }
    }
    
    bool IsInitialized() const { return initialized_; }
    const std::string& LastError() const { return last_error_; }
    
private:
    Database* db_;
    PyDatabaseWrapper wrapper_;
    std::unique_ptr<py::scoped_interpreter> guard_;
    bool initialized_{false};
    std::string last_error_;
};

PythonVM::PythonVM(Database* db)
    : impl_(std::make_unique<Impl>(db))
    , db_(db)
    , initialized_(impl_->IsInitialized()) {
}

PythonVM::~PythonVM() = default;

Result<std::string> PythonVM::Execute(const std::string& script) {
    return impl_->Execute(script);
}

Result<std::string> PythonVM::ExecuteWithResult(const std::string& script,
                                                 const std::string& result_var) {
    return impl_->ExecuteWithResult(script, result_var);
}

Status PythonVM::Reset() {
    impl_ = std::make_unique<Impl>(db_);
    initialized_ = impl_->IsInitialized();
    return initialized_ ? Status::Ok() : Status::InvalidArgument(impl_->LastError());
}

} // namespace mydb

#endif // MYDB_ENABLE_PYTHON
