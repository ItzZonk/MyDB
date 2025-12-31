/**
 * @file server.cpp
 * @brief TCP Server implementation
 */

#include <mydb/network/server.hpp>
#include <mydb/network/protocol.hpp>
#include <mydb/db.hpp>

#include <spdlog/spdlog.h>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")
using socket_t = SOCKET;
#define INVALID_SOCK INVALID_SOCKET
#else
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
using socket_t = int;
#define INVALID_SOCK -1
#endif

namespace mydb {

// ============================================================================
// Connection Implementation
// ============================================================================

Connection::Connection(int fd, IOContext* io_context)
    : fd_(fd)
    , io_context_(io_context) {
    read_buffer_.resize(kReadBufferSize);
}

Connection::~Connection() {
    Close();
}

void Connection::Close() {
    if (active_.exchange(false)) {
#ifdef _WIN32
        closesocket(static_cast<SOCKET>(fd_));
#else
        close(fd_);
#endif
        spdlog::debug("Connection closed: fd={}", fd_);
    }
}

Status Connection::StartRead() {
    if (!active_.load()) {
        return Status::InvalidArgument("Connection not active");
    }
    
    return io_context_->SubmitRecv(
        fd_,
        std::span<char>(read_buffer_.data(), read_buffer_.size()),
        0,
        [this](int result, int) {
            if (result <= 0) {
                Close();
            }
        }
    );
}

Status Connection::SendResponse(const std::vector<char>& data) {
    if (!active_.load()) {
        return Status::InvalidArgument("Connection not active");
    }
    
    write_buffer_ = data;
    
    return io_context_->SubmitSend(
        fd_,
        std::span<const char>(write_buffer_.data(), write_buffer_.size()),
        0,
        [this](int result, int) {
            if (result <= 0) {
                Close();
            }
        }
    );
}

// ============================================================================
// Server Implementation
// ============================================================================

Server::Server(const Options& options)
    : options_(options)
    , port_(options.port) {
}

Server::~Server() {
    Stop();
}

void Server::SetDatabase(Database* db) {
    db_ = db;
}

void Server::SetRequestHandler(RequestHandler handler) {
    request_handler_ = std::move(handler);
}

Status Server::Start() {
#ifdef _WIN32
    // Initialize Winsock
    WSADATA wsaData;
    if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
        return Status::IOError("Failed to initialize Winsock");
    }
#endif
    
    // Create socket
#ifdef _WIN32
    listen_fd_ = static_cast<int>(socket(AF_INET, SOCK_STREAM, IPPROTO_TCP));
#else
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
#endif
    
    if (listen_fd_ == INVALID_SOCK) {
        return Status::IOError("Failed to create socket");
    }
    
    // Set socket options
    int opt = 1;
#ifdef _WIN32
    setsockopt(static_cast<SOCKET>(listen_fd_), SOL_SOCKET, SO_REUSEADDR, 
               reinterpret_cast<const char*>(&opt), sizeof(opt));
#else
    setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
#endif
    
    // Bind
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port_);
    
    if (bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        return Status::IOError("Failed to bind to port " + std::to_string(port_));
    }
    
    // Listen
    if (listen(listen_fd_, 128) < 0) {
        return Status::IOError("Failed to listen");
    }
    
    // Create I/O context
    io_context_ = IOContext::Create(kIOURingQueueDepth);
    
    running_.store(true);
    
    spdlog::info("Server listening on port {}", port_);
    
    // Start accepting connections
    io_context_->SubmitAccept(listen_fd_, [this](int result, int) {
        HandleAccept(result);
    });
    
    return Status::Ok();
}

void Server::Stop() {
    if (!running_.exchange(false)) {
        return;
    }
    
    io_context_->Stop();
    
    // Close all connections
    {
        std::lock_guard<std::mutex> lock(connections_mutex_);
        connections_.clear();
    }
    
    // Close listen socket
    if (listen_fd_ != INVALID_SOCK) {
#ifdef _WIN32
        closesocket(static_cast<SOCKET>(listen_fd_));
        WSACleanup();
#else
        close(listen_fd_);
#endif
        listen_fd_ = INVALID_SOCK;
    }
    
    spdlog::info("Server stopped");
}

void Server::HandleAccept(int result) {
    if (!running_.load()) return;
    
    if (result >= 0) {
        auto conn = std::make_unique<Connection>(result, io_context_.get());
        
        {
            std::lock_guard<std::mutex> lock(connections_mutex_);
            connections_[result] = std::move(conn);
            stats_.connections_accepted++;
            stats_.active_connections = connections_.size();
        }
        
        spdlog::debug("Accepted connection: fd={}", result);
        
        // Start reading from the new connection
        connections_[result]->StartRead();
    }
    
    // Queue up next accept
    io_context_->SubmitAccept(listen_fd_, [this](int res, int) {
        HandleAccept(res);
    });
}

void Server::HandleRead(Connection* conn, int result) {
    if (result <= 0) {
        // Connection closed or error
        std::lock_guard<std::mutex> lock(connections_mutex_);
        connections_.erase(conn->Fd());
        stats_.active_connections = connections_.size();
        return;
    }
    
    // TODO: Parse request and process
    stats_.bytes_received += result;
}

std::vector<char> Server::DefaultHandler(const std::vector<char>& request, Connection* conn) {
    if (!db_) {
        return Protocol::EncodeResponse(ErrorResponse{
            response::kError, "Database not initialized"
        });
    }
    
    auto req_result = Protocol::ParseRequest(Slice(request.data(), request.size()));
    if (!req_result.ok()) {
        return Protocol::EncodeResponse(ErrorResponse{
            response::kInvalidRequest, req_result.status().message()
        });
    }
    
    const auto& req = req_result.value();
    
    return std::visit([this](auto&& r) -> std::vector<char> {
        using T = std::decay_t<decltype(r)>;
        
        if constexpr (std::is_same_v<T, PutRequest>) {
            Status s = db_->Put(r.key, r.value);
            if (s.ok()) {
                return Protocol::EncodeResponse(OkResponse{"OK"});
            }
            return Protocol::EncodeResponse(ErrorResponse{response::kError, s.message()});
        }
        else if constexpr (std::is_same_v<T, GetRequest>) {
            ReadOptions opts;
            if (r.snapshot.has_value()) {
                opts.snapshot = r.snapshot.value();
            }
            // If snapshot is empty/0, Database::Get will default to latest sequence.
            
            auto result = db_->Get(r.key, opts);
            if (result.ok()) {
                return Protocol::EncodeResponse(ValueResponse{result.value()});
            }
            return Protocol::EncodeResponse(ErrorResponse{response::kNotFound, "Not found"});
        }
        else if constexpr (std::is_same_v<T, DeleteRequest>) {
            Status s = db_->Delete(r.key);
            if (s.ok()) {
                return Protocol::EncodeResponse(OkResponse{"OK"});
            }
            return Protocol::EncodeResponse(ErrorResponse{response::kError, s.message()});
        }
        else if constexpr (std::is_same_v<T, PingRequest>) {
            return Protocol::EncodeResponse(OkResponse{"PONG"});
        }
        else if constexpr (std::is_same_v<T, StatusRequest>) {
            auto stats = db_->GetStats();
            return Protocol::EncodeResponse(StatusResponse{
                stats.num_entries,
                stats.memtable_size,
                stats.num_sstables,
                db_->GetVersion()
            });
        }
        else if constexpr (std::is_same_v<T, FlushRequest>) {
            Status s = db_->Flush();
            if (s.ok()) {
                return Protocol::EncodeResponse(OkResponse{"Flushed"});
            }
            return Protocol::EncodeResponse(ErrorResponse{response::kError, s.message()});
        }
        else if constexpr (std::is_same_v<T, CompactRequest>) {
            Status s = db_->CompactLevel(r.level);
            if (s.ok()) {
                return Protocol::EncodeResponse(OkResponse{"Compacted"});
            }
            return Protocol::EncodeResponse(ErrorResponse{response::kError, s.message()});
        }
        else if constexpr (std::is_same_v<T, ExecPythonRequest>) {
#ifdef MYDB_ENABLE_PYTHON
            auto result = db_->ExecutePython(r.script);
            if (result.ok()) {
                return Protocol::EncodeResponse(ValueResponse{result.value()});
            }
            return Protocol::EncodeResponse(ErrorResponse{response::kError, result.status().message()});
#else
            return Protocol::EncodeResponse(ErrorResponse{
                response::kNotFound, "Python not enabled"
            });
#endif
        }
        else {
            return Protocol::EncodeResponse(ErrorResponse{
                response::kInvalidRequest, "Unknown request type"
            });
        }
    }, req);
}

size_t Server::NumConnections() const {
    std::lock_guard<std::mutex> lock(connections_mutex_);
    return connections_.size();
}

Server::Stats Server::GetStats() const {
    return stats_;
}

} // namespace mydb
