/**
 * @file io_context.cpp
 * @brief Async I/O context implementation
 */

#include <mydb/network/io_context.hpp>

#include <spdlog/spdlog.h>

#include <thread>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <future>
#include <atomic>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")
#else
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace mydb {

// ============================================================================
// Factory Method
// ============================================================================

std::unique_ptr<IOContext> IOContext::Create(size_t queue_depth) {
#ifdef MYDB_USE_IO_URING
    return std::make_unique<IOUringContext>(queue_depth);
#else
    return std::make_unique<ThreadPoolIOContext>(4);
#endif
}

// ============================================================================
// Thread Pool I/O Context (Fallback for Windows/non-io_uring systems)
// ============================================================================

class ThreadPoolIOContext::Impl {
public:
    explicit Impl(size_t num_threads) : running_(true) {
        for (size_t i = 0; i < num_threads; ++i) {
            workers_.emplace_back([this] { WorkerLoop(); });
        }
        spdlog::info("ThreadPoolIOContext created with {} threads", num_threads);
    }
    
    ~Impl() {
        Stop();
        for (auto& worker : workers_) {
            if (worker.joinable()) {
                worker.join();
            }
        }
    }
    
    void Stop() {
        running_.store(false);
        cv_.notify_all();
    }
    
    void Submit(std::function<void()> task) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            tasks_.push(std::move(task));
        }
        cv_.notify_one();
    }
    
    int ProcessCompletions(int min_completions) {
        std::unique_lock<std::mutex> lock(completion_mutex_);
        
        if (min_completions > 0 && completions_.empty()) {
            completion_cv_.wait_for(lock, std::chrono::milliseconds(100), [this] {
                return !completions_.empty() || !running_.load();
            });
        }
        
        int count = 0;
        while (!completions_.empty()) {
            auto completion = std::move(completions_.front());
            completions_.pop();
            lock.unlock();
            
            completion();
            count++;
            
            lock.lock();
        }
        
        return count;
    }
    
    void AddCompletion(std::function<void()> completion) {
        {
            std::lock_guard<std::mutex> lock(completion_mutex_);
            completions_.push(std::move(completion));
        }
        completion_cv_.notify_one();
    }
    
    std::atomic<bool>& running() { return running_; }
    
private:
    void WorkerLoop() {
        while (running_.load()) {
            std::function<void()> task;
            
            {
                std::unique_lock<std::mutex> lock(mutex_);
                cv_.wait(lock, [this] {
                    return !tasks_.empty() || !running_.load();
                });
                
                if (!running_.load() && tasks_.empty()) {
                    return;
                }
                
                if (!tasks_.empty()) {
                    task = std::move(tasks_.front());
                    tasks_.pop();
                }
            }
            
            if (task) {
                task();
            }
        }
    }
    
    std::atomic<bool> running_;
    std::vector<std::thread> workers_;
    std::queue<std::function<void()>> tasks_;
    std::mutex mutex_;
    std::condition_variable cv_;
    
    std::queue<std::function<void()>> completions_;
    std::mutex completion_mutex_;
    std::condition_variable completion_cv_;
};

ThreadPoolIOContext::ThreadPoolIOContext(size_t num_threads)
    : impl_(std::make_unique<Impl>(num_threads)) {
}

ThreadPoolIOContext::~ThreadPoolIOContext() = default;

Status ThreadPoolIOContext::SubmitAccept(int listen_fd, IOCallback callback) {
    impl_->Submit([this, listen_fd, callback = std::move(callback)] {
#ifdef _WIN32
        SOCKET client = accept(static_cast<SOCKET>(listen_fd), nullptr, nullptr);
        int result = (client != INVALID_SOCKET) ? static_cast<int>(client) : -1;
#else
        int result = accept(listen_fd, nullptr, nullptr);
#endif
        impl_->AddCompletion([callback, result] {
            callback(result, 0);
        });
    });
    return Status::Ok();
}

Status ThreadPoolIOContext::SubmitRead(int fd, std::span<char> buffer, IOCallback callback) {
    impl_->Submit([this, fd, buffer, callback = std::move(callback)] {
#ifdef _WIN32
        int result = recv(static_cast<SOCKET>(fd), buffer.data(), 
                          static_cast<int>(buffer.size()), 0);
#else
        ssize_t result = read(fd, buffer.data(), buffer.size());
#endif
        impl_->AddCompletion([callback, result = static_cast<int>(result)] {
            callback(result, 0);
        });
    });
    return Status::Ok();
}

Status ThreadPoolIOContext::SubmitWrite(int fd, std::span<const char> buffer, IOCallback callback) {
    impl_->Submit([this, fd, buffer, callback = std::move(callback)] {
#ifdef _WIN32
        int result = send(static_cast<SOCKET>(fd), buffer.data(), 
                          static_cast<int>(buffer.size()), 0);
#else
        ssize_t result = write(fd, buffer.data(), buffer.size());
#endif
        impl_->AddCompletion([callback, result = static_cast<int>(result)] {
            callback(result, 0);
        });
    });
    return Status::Ok();
}

Status ThreadPoolIOContext::SubmitClose(int fd, IOCallback callback) {
    impl_->Submit([this, fd, callback = std::move(callback)] {
#ifdef _WIN32
        int result = closesocket(static_cast<SOCKET>(fd));
#else
        int result = close(fd);
#endif
        impl_->AddCompletion([callback, result] {
            callback(result, 0);
        });
    });
    return Status::Ok();
}

Status ThreadPoolIOContext::SubmitRecv(int fd, std::span<char> buffer, int flags, IOCallback callback) {
    impl_->Submit([this, fd, buffer, flags, callback = std::move(callback)] {
#ifdef _WIN32
        int result = recv(static_cast<SOCKET>(fd), buffer.data(), 
                          static_cast<int>(buffer.size()), flags);
#else
        ssize_t result = recv(fd, buffer.data(), buffer.size(), flags);
#endif
        impl_->AddCompletion([callback, result = static_cast<int>(result)] {
            callback(result, 0);
        });
    });
    return Status::Ok();
}

Status ThreadPoolIOContext::SubmitSend(int fd, std::span<const char> buffer, int flags, IOCallback callback) {
    impl_->Submit([this, fd, buffer, flags, callback = std::move(callback)] {
#ifdef _WIN32
        int result = send(static_cast<SOCKET>(fd), buffer.data(), 
                          static_cast<int>(buffer.size()), flags);
#else
        ssize_t result = send(fd, buffer.data(), buffer.size(), flags);
#endif
        impl_->AddCompletion([callback, result = static_cast<int>(result)] {
            callback(result, 0);
        });
    });
    return Status::Ok();
}

int ThreadPoolIOContext::ProcessCompletions(int min_completions) {
    return impl_->ProcessCompletions(min_completions);
}

void ThreadPoolIOContext::Run() {
    while (impl_->running().load()) {
        ProcessCompletions(1);
    }
}

void ThreadPoolIOContext::Stop() {
    impl_->Stop();
}

// ============================================================================
// io_uring Context (Linux only)
// ============================================================================

#ifdef MYDB_USE_IO_URING

#include <liburing.h>

class IOUringContext::Impl {
public:
    explicit Impl(size_t queue_depth) {
        int ret = io_uring_queue_init(queue_depth, &ring_, 0);
        if (ret < 0) {
            spdlog::error("Failed to initialize io_uring: {}", strerror(-ret));
            initialized_ = false;
        } else {
            initialized_ = true;
            spdlog::info("IOUringContext created with queue depth {}", queue_depth);
        }
    }
    
    ~Impl() {
        if (initialized_) {
            io_uring_queue_exit(&ring_);
        }
    }
    
    bool IsInitialized() const { return initialized_; }
    io_uring* Ring() { return &ring_; }
    
    void Stop() { running_.store(false); }
    std::atomic<bool>& running() { return running_; }
    
private:
    io_uring ring_;
    bool initialized_{false};
    std::atomic<bool> running_{true};
};

IOUringContext::IOUringContext(size_t queue_depth)
    : impl_(std::make_unique<Impl>(queue_depth)) {
}

IOUringContext::~IOUringContext() = default;

Status IOUringContext::SubmitAccept(int listen_fd, IOCallback callback) {
    auto* sqe = io_uring_get_sqe(impl_->Ring());
    if (!sqe) return Status::Busy("SQ full");
    
    io_uring_prep_accept(sqe, listen_fd, nullptr, nullptr, 0);
    io_uring_sqe_set_data(sqe, new IOCallback(std::move(callback)));
    io_uring_submit(impl_->Ring());
    
    return Status::Ok();
}

Status IOUringContext::SubmitRead(int fd, std::span<char> buffer, IOCallback callback) {
    auto* sqe = io_uring_get_sqe(impl_->Ring());
    if (!sqe) return Status::Busy("SQ full");
    
    io_uring_prep_read(sqe, fd, buffer.data(), buffer.size(), 0);
    io_uring_sqe_set_data(sqe, new IOCallback(std::move(callback)));
    io_uring_submit(impl_->Ring());
    
    return Status::Ok();
}

Status IOUringContext::SubmitWrite(int fd, std::span<const char> buffer, IOCallback callback) {
    auto* sqe = io_uring_get_sqe(impl_->Ring());
    if (!sqe) return Status::Busy("SQ full");
    
    io_uring_prep_write(sqe, fd, buffer.data(), buffer.size(), 0);
    io_uring_sqe_set_data(sqe, new IOCallback(std::move(callback)));
    io_uring_submit(impl_->Ring());
    
    return Status::Ok();
}

Status IOUringContext::SubmitClose(int fd, IOCallback callback) {
    auto* sqe = io_uring_get_sqe(impl_->Ring());
    if (!sqe) return Status::Busy("SQ full");
    
    io_uring_prep_close(sqe, fd);
    io_uring_sqe_set_data(sqe, new IOCallback(std::move(callback)));
    io_uring_submit(impl_->Ring());
    
    return Status::Ok();
}

Status IOUringContext::SubmitRecv(int fd, std::span<char> buffer, int flags, IOCallback callback) {
    auto* sqe = io_uring_get_sqe(impl_->Ring());
    if (!sqe) return Status::Busy("SQ full");
    
    io_uring_prep_recv(sqe, fd, buffer.data(), buffer.size(), flags);
    io_uring_sqe_set_data(sqe, new IOCallback(std::move(callback)));
    io_uring_submit(impl_->Ring());
    
    return Status::Ok();
}

Status IOUringContext::SubmitSend(int fd, std::span<const char> buffer, int flags, IOCallback callback) {
    auto* sqe = io_uring_get_sqe(impl_->Ring());
    if (!sqe) return Status::Busy("SQ full");
    
    io_uring_prep_send(sqe, fd, buffer.data(), buffer.size(), flags);
    io_uring_sqe_set_data(sqe, new IOCallback(std::move(callback)));
    io_uring_submit(impl_->Ring());
    
    return Status::Ok();
}

int IOUringContext::ProcessCompletions(int min_completions) {
    io_uring_cqe* cqe;
    int count = 0;
    
    while (count < min_completions || io_uring_peek_cqe(impl_->Ring(), &cqe) == 0) {
        int ret = io_uring_wait_cqe(impl_->Ring(), &cqe);
        if (ret < 0) break;
        
        auto* callback = static_cast<IOCallback*>(io_uring_cqe_get_data(cqe));
        if (callback) {
            (*callback)(cqe->res, cqe->flags);
            delete callback;
        }
        
        io_uring_cqe_seen(impl_->Ring(), cqe);
        count++;
        
        if (count >= min_completions && io_uring_peek_cqe(impl_->Ring(), &cqe) < 0) {
            break;
        }
    }
    
    return count;
}

void IOUringContext::Run() {
    while (impl_->running().load()) {
        ProcessCompletions(1);
    }
}

void IOUringContext::Stop() {
    impl_->Stop();
}

#endif // MYDB_USE_IO_URING

} // namespace mydb
