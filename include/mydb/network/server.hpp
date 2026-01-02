#pragma once

#include <mydb/common/types.hpp>
#include <mydb/common/status.hpp>
#include <mydb/network/io_context.hpp>
#include <mydb/config.hpp>
#include <memory>
#include <functional>
#include <string>
#include <vector>
#include <unordered_map>
#include <atomic>
#include <mutex>
#include <span>

namespace mydb {

class Database;

class Connection {
public:
    using ReadCallback = std::function<void(Connection*, int)>;

    Connection(int fd, IOContext* io_context, ReadCallback callback);
    ~Connection();
    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;
    
    [[nodiscard]] int Fd() const { return fd_; }
    [[nodiscard]] bool IsActive() const { return active_.load(); }
    void Close();
    void StartRead();
    Status SendResponse(const std::vector<char>& data);
    [[nodiscard]] const std::string& RemoteAddress() const { return remote_addr_; }
    
    // Public buffers for Server access (simplified for this stage)
    std::vector<char> pending_buffer_;
    std::vector<char> read_buffer_;

private:
    int fd_;
    IOContext* io_context_;
    ReadCallback callback_;
    std::atomic<bool> active_{true};
    std::string remote_addr_;
    std::vector<char> write_buffer_;
    
    static constexpr size_t kReadBufferSize = 8192;
};

using RequestHandler = std::function<std::vector<char>(const std::vector<char>&, Connection*)>;

class Server {
public:
    explicit Server(const Options& options = {});
    ~Server();
    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;
    
    void SetDatabase(Database* db);
    void SetRequestHandler(RequestHandler handler);
    Status Start();
    void Stop();
    [[nodiscard]] bool IsRunning() const { return running_.load(); }
    [[nodiscard]] uint16_t Port() const { return port_; }
    [[nodiscard]] size_t NumConnections() const;
    
    struct Stats { uint64_t requests_processed{0}, bytes_received{0}, bytes_sent{0}, connections_accepted{0}, active_connections{0}; };
    [[nodiscard]] Stats GetStats() const;
    
private:
    void HandleAccept(int result);
    void HandleRead(Connection* conn, int result);
    std::vector<char> DefaultHandler(const std::vector<char>& request, Connection* conn);
    
    Options options_;
    uint16_t port_;
    int listen_fd_{-1};
    std::unique_ptr<IOContext> io_context_;
    std::atomic<bool> running_{false};
    Database* db_{nullptr};
    RequestHandler request_handler_;
    std::unordered_map<int, std::unique_ptr<Connection>> connections_;
    mutable std::mutex connections_mutex_;
    Stats stats_;
};

} // namespace mydb
