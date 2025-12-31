#pragma once

// Linux: io_uring, Windows/other: thread-pool fallback

#include <mydb/common/status.hpp>
#include <mydb/config.hpp>
#include <memory>
#include <functional>
#include <cstdint>
#include <vector>
#include <span>

namespace mydb {

using IOCallback = std::function<void(int result, int flags)>;

enum class IOOp { kAccept, kRead, kWrite, kClose, kConnect, kRecv, kSend };

struct IORequest {
    IOOp op;
    int fd;
    void* buffer;
    size_t length;
    uint64_t offset;
    IOCallback callback;
    void* user_data;
};

class IOContext {
public:
    virtual ~IOContext() = default;
    static std::unique_ptr<IOContext> Create(size_t queue_depth = kIOURingQueueDepth);
    
    virtual Status SubmitAccept(int listen_fd, IOCallback callback) = 0;
    virtual Status SubmitRead(int fd, std::span<char> buffer, IOCallback callback) = 0;
    virtual Status SubmitWrite(int fd, std::span<const char> buffer, IOCallback callback) = 0;
    virtual Status SubmitClose(int fd, IOCallback callback) = 0;
    virtual Status SubmitRecv(int fd, std::span<char> buffer, int flags, IOCallback callback) = 0;
    virtual Status SubmitSend(int fd, std::span<const char> buffer, int flags, IOCallback callback) = 0;
    virtual int ProcessCompletions(int min_completions = 1) = 0;
    virtual void Run() = 0;
    virtual void Stop() = 0;
protected:
    IOContext() = default;
};

#ifdef MYDB_USE_IO_URING
class IOUringContext : public IOContext {
public:
    explicit IOUringContext(size_t queue_depth);
    ~IOUringContext() override;
    Status SubmitAccept(int listen_fd, IOCallback callback) override;
    Status SubmitRead(int fd, std::span<char> buffer, IOCallback callback) override;
    Status SubmitWrite(int fd, std::span<const char> buffer, IOCallback callback) override;
    Status SubmitClose(int fd, IOCallback callback) override;
    Status SubmitRecv(int fd, std::span<char> buffer, int flags, IOCallback callback) override;
    Status SubmitSend(int fd, std::span<const char> buffer, int flags, IOCallback callback) override;
    int ProcessCompletions(int min_completions) override;
    void Run() override;
    void Stop() override;
private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};
#endif

class ThreadPoolIOContext : public IOContext {
public:
    explicit ThreadPoolIOContext(size_t num_threads = 4);
    ~ThreadPoolIOContext() override;
    Status SubmitAccept(int listen_fd, IOCallback callback) override;
    Status SubmitRead(int fd, std::span<char> buffer, IOCallback callback) override;
    Status SubmitWrite(int fd, std::span<const char> buffer, IOCallback callback) override;
    Status SubmitClose(int fd, IOCallback callback) override;
    Status SubmitRecv(int fd, std::span<char> buffer, int flags, IOCallback callback) override;
    Status SubmitSend(int fd, std::span<const char> buffer, int flags, IOCallback callback) override;
    int ProcessCompletions(int min_completions) override;
    void Run() override;
    void Stop() override;
private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace mydb
