#pragma once

#include <string>
#include <variant>
#include <optional>
#include <utility>

namespace mydb {

enum class StatusCode {
    kOk, kNotFound, kCorruption, kNotSupported, kInvalidArgument,
    kIOError, kAlreadyExists, kBusy, kTimedOut, kAborted, kOutOfMemory, kUnknown
};

class Status {
public:
    Status() : code_(StatusCode::kOk) {}
    
    static Status Ok() { return Status(); }
    static Status NotFound(std::string msg = "") { return Status(StatusCode::kNotFound, std::move(msg)); }
    static Status Corruption(std::string msg = "") { return Status(StatusCode::kCorruption, std::move(msg)); }
    static Status NotSupported(std::string msg = "") { return Status(StatusCode::kNotSupported, std::move(msg)); }
    static Status InvalidArgument(std::string msg = "") { return Status(StatusCode::kInvalidArgument, std::move(msg)); }
    static Status IOError(std::string msg = "") { return Status(StatusCode::kIOError, std::move(msg)); }
    static Status AlreadyExists(std::string msg = "") { return Status(StatusCode::kAlreadyExists, std::move(msg)); }
    static Status Busy(std::string msg = "") { return Status(StatusCode::kBusy, std::move(msg)); }
    static Status TimedOut(std::string msg = "") { return Status(StatusCode::kTimedOut, std::move(msg)); }
    static Status Aborted(std::string msg = "") { return Status(StatusCode::kAborted, std::move(msg)); }
    static Status OutOfMemory(std::string msg = "") { return Status(StatusCode::kOutOfMemory, std::move(msg)); }
    
    [[nodiscard]] bool ok() const { return code_ == StatusCode::kOk; }
    [[nodiscard]] bool IsNotFound() const { return code_ == StatusCode::kNotFound; }
    [[nodiscard]] bool IsCorruption() const { return code_ == StatusCode::kCorruption; }
    [[nodiscard]] bool IsIOError() const { return code_ == StatusCode::kIOError; }
    [[nodiscard]] StatusCode code() const { return code_; }
    [[nodiscard]] const std::string& message() const { return message_; }
    
    [[nodiscard]] std::string ToString() const {
        const char* names[] = {"OK","NotFound","Corruption","NotSupported","InvalidArgument",
                               "IOError","AlreadyExists","Busy","TimedOut","Aborted","OutOfMemory","Unknown"};
        std::string result = names[static_cast<int>(code_)];
        if (!message_.empty()) result += ": " + message_;
        return result;
    }
    
    explicit operator bool() const { return ok(); }
    
private:
    Status(StatusCode code, std::string msg) : code_(code), message_(std::move(msg)) {}
    StatusCode code_;
    std::string message_;
};

// Result<T> - similar to std::expected (C++23)
template<typename T>
class Result {
public:
    Result(T value) : data_(std::move(value)) {}
    Result(Status status) : data_(std::move(status)) {
        if (std::get<Status>(data_).ok()) 
            data_ = Status::InvalidArgument("Result with Ok but no value");
    }
    
    static Result Ok(T value) { return Result(std::move(value)); }
    static Result Error(Status status) { return Result(std::move(status)); }
    
    [[nodiscard]] bool ok() const { return std::holds_alternative<T>(data_); }
    explicit operator bool() const { return ok(); }
    
    [[nodiscard]] const T& value() const& { return std::get<T>(data_); }
    [[nodiscard]] T& value() & { return std::get<T>(data_); }
    [[nodiscard]] T&& value() && { return std::get<T>(std::move(data_)); }
    [[nodiscard]] const T* operator->() const { return &std::get<T>(data_); }
    [[nodiscard]] T* operator->() { return &std::get<T>(data_); }
    [[nodiscard]] const T& operator*() const& { return std::get<T>(data_); }
    [[nodiscard]] T& operator*() & { return std::get<T>(data_); }
    
    [[nodiscard]] Status status() const {
        return ok() ? Status::Ok() : std::get<Status>(data_);
    }
    
    [[nodiscard]] T value_or(T def) const& { return ok() ? std::get<T>(data_) : def; }
    
private:
    std::variant<T, Status> data_;
};

template<>
class Result<void> {
public:
    Result() = default;
    Result(Status status) : status_(std::move(status)) {}
    static Result Ok() { return Result(); }
    static Result Error(Status status) { return Result(std::move(status)); }
    [[nodiscard]] bool ok() const { return status_.ok(); }
    explicit operator bool() const { return ok(); }
    [[nodiscard]] const Status& status() const { return status_; }
private:
    Status status_;
};

#define MYDB_RETURN_IF_ERROR(expr) \
    do { auto _s = (expr); if (!_s.ok()) return _s; } while (0)

#define MYDB_ASSIGN_OR_RETURN(var, expr) \
    auto _r_##var = (expr); \
    if (!_r_##var.ok()) return _r_##var.status(); \
    var = std::move(_r_##var.value())

} // namespace mydb
