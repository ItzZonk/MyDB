/**
 * @file server.cpp
 * @brief TCP Server implementation
 */

#include <mydb/network/server.hpp>
#include <mydb/network/protocol.hpp>
#include <mydb/db.hpp>

#include <spdlog/spdlog.h>
#include <sstream>

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

Connection::Connection(int fd, IOContext* io_context, ReadCallback callback)
    : fd_(fd)
    , io_context_(io_context)
    , callback_(std::move(callback)) {
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

void Connection::StartRead() {
    if (!active_.load()) return;
    
    io_context_->SubmitRecv(
        fd_,
        std::span<char>(read_buffer_.data(), read_buffer_.size()),
        0,
        [this](int result, int) {
            callback_(this, result);
        }
    );
}

Status Connection::SendResponse(const std::vector<char>& data) {
    if (!active_.load()) return Status::InvalidArgument("Connection not active");
    
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
    WSADATA wsaData;
    if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
        return Status::IOError("Failed to initialize Winsock");
    }
#endif
    
#ifdef _WIN32
    listen_fd_ = static_cast<int>(socket(AF_INET, SOCK_STREAM, IPPROTO_TCP));
#else
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
#endif
    
    if (listen_fd_ == INVALID_SOCK) {
        return Status::IOError("Failed to create socket");
    }
    
    int opt = 1;
#ifdef _WIN32
    setsockopt(static_cast<SOCKET>(listen_fd_), SOL_SOCKET, SO_REUSEADDR, 
               reinterpret_cast<const char*>(&opt), sizeof(opt));
#else
    setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
#endif
    
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port_);
    
    if (bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        return Status::IOError("Failed to bind to port " + std::to_string(port_));
    }
    
    if (listen(listen_fd_, 128) < 0) {
        return Status::IOError("Failed to listen");
    }
    
    io_context_ = IOContext::Create(kIOURingQueueDepth);
    running_.store(true);
    
    spdlog::info("Server listening on port {}", port_);
    
    io_context_->SubmitAccept(listen_fd_, [this](int result, int) {
        HandleAccept(result);
    });
    
    return Status::Ok();
}

void Server::Stop() {
    if (!running_.exchange(false)) return;
    
    io_context_->Stop();
    
    {
        std::lock_guard<std::mutex> lock(connections_mutex_);
        connections_.clear();
    }
    
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
        auto conn = std::make_unique<Connection>(
            result, 
            io_context_.get(),
            [this](Connection* c, int res) { HandleRead(c, res); }
        );
        
        {
            std::lock_guard<std::mutex> lock(connections_mutex_);
            connections_[result] = std::move(conn);
            stats_.connections_accepted++;
            stats_.active_connections = connections_.size();
        }
        
        spdlog::debug("Accepted connection: fd={}", result);
        connections_[result]->StartRead();
    }
    
    io_context_->SubmitAccept(listen_fd_, [this](int res, int) {
        HandleAccept(res);
    });
}

void Server::HandleRead(Connection* conn, int result) {
    if (result <= 0) {
        std::lock_guard<std::mutex> lock(connections_mutex_);
        connections_.erase(conn->Fd());
        stats_.active_connections = connections_.size();
        return;
    }
    
    stats_.bytes_received += result;
    
    conn->pending_buffer_.insert(
        conn->pending_buffer_.end(), 
        conn->read_buffer_.begin(), 
        conn->read_buffer_.begin() + result
    );
    
    // Process messages
    while (Protocol::HasCompleteMessage(Slice(conn->pending_buffer_.data(), conn->pending_buffer_.size()))) {
        Slice s(conn->pending_buffer_.data(), conn->pending_buffer_.size());
        
        auto req_result = Protocol::ParseRequest(s);
        
        if (req_result.ok()) {
            std::vector<char> response = request_handler_ 
                ? request_handler_(conn->pending_buffer_, conn)
                : DefaultHandler(conn->pending_buffer_, conn);
                
            conn->SendResponse(response);
            conn->pending_buffer_.clear(); 
            break; 
        } else {
             spdlog::error("Parse error: {}", req_result.status().message());
             conn->pending_buffer_.clear();
             break; 
        }
    }

    conn->StartRead();
}

std::vector<char> Server::DefaultHandler(const std::vector<char>& request, Connection* conn) {
    if (!db_) {
        return Protocol::EncodeResponse(ErrorResponse{response::kError, "Database not initialized"});
    }
    
    auto req_result = Protocol::ParseRequest(Slice(request.data(), request.size()));
    if (!req_result.ok()) {
        return Protocol::EncodeResponse(ErrorResponse{
            response::kInvalidRequest, req_result.status().message()
        });
    }
    
    const auto& req = req_result.value();
    
    spdlog::info("Processing request, variant index: {}", req.index());
    
    return std::visit([this](auto&& r) -> std::vector<char> {
        using T = std::decay_t<decltype(r)>;
        
        if constexpr (std::is_same_v<T, PutRequest>) {
            Status s = db_->Put(r.key, r.value);
            return Protocol::EncodeResponse(s.ok() ? Response{OkResponse{"OK"}} 
                                                   : Response{ErrorResponse{response::kError, s.message()}});
        }
        else if constexpr (std::is_same_v<T, GetRequest>) {
            ReadOptions opts;
            if (r.snapshot.has_value()) opts.snapshot = r.snapshot.value();
            auto result = db_->Get(r.key, opts);
            
            spdlog::info("GET request: key='{}' field.has_value={} field='{}'", 
                r.key, r.field.has_value(), r.field.value_or("(none)"));
            
            if (result.ok()) {
                std::string val = result.value();
                
                // Field Extraction Logic
                if (r.field.has_value()) {
                    std::string field_name = r.field.value();
                    spdlog::info("Attempting extraction. val='{}', field='{}'", val, field_name);
                    
                    // Try to find field:value pattern (supports quoted and unquoted keys)
                    size_t pos = val.find("\"" + field_name + "\"");
                    if (pos == std::string::npos) {
                        pos = val.find(field_name + ":");
                    }
                    if (pos == std::string::npos) {
                        pos = val.find(field_name);
                    }
                    
                    if (pos != std::string::npos) {
                        size_t colon = val.find(":", pos);
                        if (colon != std::string::npos) {
                            size_t start = colon + 1;
                            while (start < val.size() && isspace(val[start])) start++;
                            
                            // Find end: comma, semicolon, or closing brace
                            size_t end = val.find_first_of(",;}", start);
                            if (end == std::string::npos) end = val.size();
                            
                            std::string extracted = val.substr(start, end - start);
                            
                            // Trim whitespace
                            while (!extracted.empty() && isspace(extracted.back())) extracted.pop_back();
                            
                            // Remove quotes if present
                            if (extracted.size() >= 2 && extracted.front() == '"' && extracted.back() == '"') {
                                extracted = extracted.substr(1, extracted.size() - 2);
                            }
                            
                            return Protocol::EncodeResponse(ValueResponse{extracted});
                        }
                    }
                    return Protocol::EncodeResponse(ErrorResponse{response::kNotFound, "Field not found"});
                }
                
                return Protocol::EncodeResponse(ValueResponse{val});
            }
            return Protocol::EncodeResponse(ErrorResponse{response::kNotFound, "Not found"});
        }
        else if constexpr (std::is_same_v<T, DeleteRequest>) {
            Status s = db_->Delete(r.key);
             return Protocol::EncodeResponse(s.ok() ? Response{OkResponse{"OK"}} 
                                                    : Response{ErrorResponse{response::kError, s.message()}});
        }
        else if constexpr (std::is_same_v<T, PingRequest>) {
            return Protocol::EncodeResponse(OkResponse{"PONG"});
        }
        else if constexpr (std::is_same_v<T, StatusRequest>) {
            auto stats = db_->GetStats();
            return Protocol::EncodeResponse(StatusResponse{
                stats.num_entries, stats.memtable_size, stats.num_sstables, db_->GetVersion()
            });
        }
        else if constexpr (std::is_same_v<T, FlushRequest>) {
             Status s = db_->Flush();
             return Protocol::EncodeResponse(s.ok() ? Response{OkResponse{"Flushed"}} 
                                                    : Response{ErrorResponse{response::kError, s.message()}});
        }
        else if constexpr (std::is_same_v<T, CompactRequest>) {
             Status s = db_->CompactLevel(r.level);
             return Protocol::EncodeResponse(s.ok() ? Response{OkResponse{"Compacted"}} 
                                                    : Response{ErrorResponse{response::kError, s.message()}});
        }
        else if constexpr (std::is_same_v<T, IntrospectRequest>) {
            if (r.target == "BUFFERPOOL") {
                auto* bpm = db_->GetBufferPoolManager();
                if (!bpm) return Protocol::EncodeResponse(ErrorResponse{response::kError, "BufferPool not initialized"});
                
                auto state = bpm->GetState();
                std::ostringstream oss;
                for (const auto& f : state) {
                    oss << "id:" << f.page_id << " pin:" << f.pin_count << " dirty:" << f.is_dirty << "\n";
                }
                return Protocol::EncodeResponse(ValueResponse{oss.str()});
            }
            return Protocol::EncodeResponse(ErrorResponse{response::kInvalidRequest, "Unknown introspect target"});
        }
        else if constexpr (std::is_same_v<T, ExecPythonRequest>) {
#ifdef MYDB_ENABLE_PYTHON
            auto result = db_->ExecutePython(r.script);
            if (result.ok()) return Protocol::EncodeResponse(ValueResponse{result.value()});
            return Protocol::EncodeResponse(ErrorResponse{response::kError, result.status().message()});
#else
            return Protocol::EncodeResponse(ErrorResponse{response::kError, "Python disabled"});
#endif
        }
        else {
            return Protocol::EncodeResponse(ErrorResponse{response::kInvalidRequest, "Unknown type"});
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
