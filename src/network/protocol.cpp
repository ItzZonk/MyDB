/**
 * @file protocol.cpp
 * @brief RESP (Redis Serialization Protocol) implementation
 */

#include <mydb/network/protocol.hpp>

#include <string>
#include <charconv>
#include <algorithm>

namespace mydb {

// ============================================================================
// Utility
// ============================================================================

std::optional<size_t> Protocol::FindCRLF(const Slice& data, size_t offset) {
    for (size_t i = offset; i + 1 < data.size(); ++i) {
        if (data[i] == '\r' && data[i+1] == '\n') {
            return i;
        }
    }
    return std::nullopt;
}

// ============================================================================
// RESP Encoding Helpers
// ============================================================================

void Protocol::EncodeSimpleString(std::vector<char>& buf, const std::string& str) {
    buf.push_back('+');
    buf.insert(buf.end(), str.begin(), str.end());
    buf.push_back('\r');
    buf.push_back('\n');
}

void Protocol::EncodeError(std::vector<char>& buf, const std::string& str) {
    buf.push_back('-');
    buf.insert(buf.end(), str.begin(), str.end());
    buf.push_back('\r');
    buf.push_back('\n');
}

void Protocol::EncodeInteger(std::vector<char>& buf, int64_t val) {
    buf.push_back(':');
    std::string s = std::to_string(val);
    buf.insert(buf.end(), s.begin(), s.end());
    buf.push_back('\r');
    buf.push_back('\n');
}

void Protocol::EncodeBulkString(std::vector<char>& buf, const std::string& str) {
    buf.push_back('$');
    std::string len_str = std::to_string(str.size());
    buf.insert(buf.end(), len_str.begin(), len_str.end());
    buf.push_back('\r');
    buf.push_back('\n');
    buf.insert(buf.end(), str.begin(), str.end());
    buf.push_back('\r');
    buf.push_back('\n');
}

void Protocol::EncodeArray(std::vector<char>& buf, size_t count) {
    buf.push_back('*');
    std::string s = std::to_string(count);
    buf.insert(buf.end(), s.begin(), s.end());
    buf.push_back('\r');
    buf.push_back('\n');
}

// ============================================================================
// RESP Decoding Helpers
// ============================================================================

Result<std::string> Protocol::DecodeSimpleString(const Slice& data, size_t& offset) {
    if (offset >= data.size()) return Status::Corruption("Buffer underflow");
    if (data[offset] != '+') return Status::InvalidArgument("Expected Simple String (+)");
    
    auto crlf = FindCRLF(data, offset + 1);
    if (!crlf) return Status::Corruption("Missing CRLF");
    
    std::string res(data.data() + offset + 1, *crlf - (offset + 1));
    offset = *crlf + 2;
    return res;
}

Result<std::string> Protocol::DecodeError(const Slice& data, size_t& offset) {
    if (offset >= data.size()) return Status::Corruption("Buffer underflow");
    if (data[offset] != '-') return Status::InvalidArgument("Expected Error (-)");
    
    auto crlf = FindCRLF(data, offset + 1);
    if (!crlf) return Status::Corruption("Missing CRLF");
    
    std::string res(data.data() + offset + 1, *crlf - (offset + 1));
    offset = *crlf + 2;
    return res;
}

Result<int64_t> Protocol::DecodeInteger(const Slice& data, size_t& offset) {
    if (offset >= data.size()) return Status::Corruption("Buffer underflow");
    if (data[offset] != ':') return Status::InvalidArgument("Expected Integer (:)");
    
    auto crlf = FindCRLF(data, offset + 1);
    if (!crlf) return Status::Corruption("Missing CRLF");
    
    std::string str(data.data() + offset + 1, *crlf - (offset + 1));
    offset = *crlf + 2;
    
    try {
        return std::stoll(str);
    } catch (...) {
        return Status::InvalidArgument("Invalid integer format");
    }
}

Result<std::string> Protocol::DecodeBulkString(const Slice& data, size_t& offset) {
    if (offset >= data.size()) return Status::Corruption("Buffer underflow");
    if (data[offset] != '$') return Status::InvalidArgument("Expected Bulk String ($)");
    
    auto header_crlf = FindCRLF(data, offset + 1);
    if (!header_crlf) return Status::Corruption("Missing header CRLF");
    
    std::string len_str(data.data() + offset + 1, *header_crlf - (offset + 1));
    offset = *header_crlf + 2;
    
    int64_t len = 0;
    try {
        len = std::stoll(len_str);
    } catch (...) {
        return Status::InvalidArgument("Invalid length");
    }
    
    if (len == -1) return Status::NotFound("Null Bulk String");
    if (len < 0) return Status::InvalidArgument("Negative length");
    
    if (offset + len + 2 > data.size()) return Status::Corruption("String truncated");
    
    std::string res(data.data() + offset, static_cast<size_t>(len));
    
    // Validate trailing CRLF
    if (data[offset + len] != '\r' || data[offset + len + 1] != '\n') {
        return Status::Corruption("Missing trailing CRLF");
    }
    
    offset += len + 2;
    return res;
}

Result<size_t> Protocol::DecodeArrayHeader(const Slice& data, size_t& offset) {
    if (offset >= data.size()) return Status::Corruption("Buffer underflow");
    if (data[offset] != '*') return Status::InvalidArgument("Expected Array (*)");
    
    auto crlf = FindCRLF(data, offset + 1);
    if (!crlf) return Status::Corruption("Missing CRLF");
    
    std::string len_str(data.data() + offset + 1, *crlf - (offset + 1));
    offset = *crlf + 2;
    
    try {
        int64_t len = std::stoll(len_str);
        if (len < 0) return 0; // Null array treated as empty
        return static_cast<size_t>(len);
    } catch (...) {
        return Status::InvalidArgument("Invalid array length");
    }
}

// ============================================================================
// Public Interface
// ============================================================================

bool Protocol::HasCompleteMessage(const Slice& data) {
    // Basic structural check only. 
    // A robust implementation would parse lengths recursively.
    // For now, check if we have matching CRLF for the apparent structure?
    // Actually, simple check: Do we have at least one CRLF?
    // And if it's Bulk String ($N), do we have N bytes + CRLF?
    
    if (data.empty()) return false;
    
    size_t offset = 0;
    char type = data[0];
    auto first_crlf = FindCRLF(data, 0);
    if (!first_crlf) return false;
    
    if (type == '+' || type == '-' || type == ':') {
        return true; 
    }
    
    if (type == '$') {
        // Parse length
        std::string len_str(data.data() + 1, *first_crlf - 1);
        try {
            int64_t len = std::stoll(len_str);
            if (len == -1) return true; // Null bulk string
            if (len < 0) return false;
            
            // Need headers + body + CRLF
            size_t required = (*first_crlf + 2) + len + 2;
            return data.size() >= required;
        } catch (...) { return false; }
    }
    
    if (type == '*') {
        // Only minimal check for array - this is hard without full parse
        // Assume incomplete if we can't parse successfully?
        // But ParseRequest will return error if incomplete.
        // We need this for buffer accumulation.
        // Let's implement a 'Dry Run' parser if needed, or just return true if it looks plausible
        // But for reliable networking, we need exact framing.
        // Let's rely on ParseRequest failure with 'Corruption' vs 'NotEnoughData'?
        // Or implement a Scan helper.
        
        // Quick scan for array
        std::string count_str(data.data() + 1, *first_crlf - 1);
        try {
            int64_t count = std::stoll(count_str);
            size_t pos = *first_crlf + 2;
            for (int i=0; i<count; ++i) {
                if (pos >= data.size()) return false;
                char subtype = data[pos];
                auto next_crlf = FindCRLF(data, pos);
                if (!next_crlf) return false;
                
                if (subtype == '$') {
                    std::string slen(data.data() + pos + 1, *next_crlf - (pos + 1));
                    int64_t len = std::stoll(slen);
                    pos = *next_crlf + 2;
                    if (len >= 0) {
                         pos += len + 2;
                    }
                } else if (subtype == ':') {
                     pos = *next_crlf + 2;
                } else {
                     // Assume simple string or error
                     pos = *next_crlf + 2;
                }
            }
            return pos <= data.size();
        } catch (...) { return false; }
    }
    
    return false;
}

Result<Request> Protocol::ParseRequest(const Slice& data) {
    size_t offset = 0;
    
    // Expecting RESP Array: *N \r\n ...
    auto array_len_res = DecodeArrayHeader(data, offset);
    if (!array_len_res.ok()) return array_len_res.status();
    
    size_t argc = array_len_res.value();
    if (argc == 0) return Status::InvalidArgument("Empty command");
    
    std::vector<std::string> args;
    args.reserve(argc);
    
    for (size_t i = 0; i < argc; ++i) {
        // Arguments are usually Bulk Strings
        auto arg_res = DecodeBulkString(data, offset);
        if (!arg_res.ok()) return arg_res.status();
        args.push_back(arg_res.value());
    }
    
    if (args.empty()) return Status::InvalidArgument("Empty command args");
    
    std::string cmd = args[0];
    // Case-insensitive normalization
    for (auto& c : cmd) c = static_cast<char>(toupper(c));
    
    if (cmd == "GET") {
        if (args.size() < 2) return Status::InvalidArgument("GET requires key");
        std::optional<SequenceNumber> snap;
        std::optional<std::string> field;
        
        if (args.size() >= 3) {
            // Try to parse as snapshot ID (number)
            bool is_number = true;
            try { 
                size_t idx;
                snap = std::stoull(args[2], &idx);
                if (idx != args[2].size()) is_number = false; // Not fully numeric
            } catch (...) { is_number = false; }
            
            // If not a number, treat as field
            if (!is_number) {
                snap.reset();
                field = args[2];
            }
        }
        return Request{GetRequest{args[1], snap, field}};
    }
    else if (cmd == "PUT" || cmd == "SET") {
        if (args.size() < 3) return Status::InvalidArgument("PUT requires key and value");
        return Request{PutRequest{args[1], args[2]}};
    }
    else if (cmd == "DEL" || cmd == "DELETE") {
        if (args.size() < 2) return Status::InvalidArgument("DEL requires key");
        return Request{DeleteRequest{args[1]}};
    }
    else if (cmd == "PING") {
        return Request{PingRequest{}};
    }
    else if (cmd == "STATUS" || cmd == "INFO") {
        return Request{StatusRequest{}};
    }
    else if (cmd == "FLUSH") {
        return Request{FlushRequest{}};
    }
    else if (cmd == "COMPACT") {
        int level = -1;
        if (args.size() >= 2) {
            try { level = std::stoi(args[1]); } catch (...) {}
        }
        return Request{CompactRequest{level}};
    }
    else if (cmd == "EXEC" || cmd == "PYTHON") {
        if (args.size() < 2) return Status::InvalidArgument("EXEC requires script");
        return Request{ExecPythonRequest{args[1]}};
    }
    else if (cmd == "INTROSPECT") {
        if (args.size() < 2) return Status::InvalidArgument("INTROSPECT requires target");
        return Request{IntrospectRequest{args[1]}};
    }
    
    return Status::InvalidArgument("Unknown command: " + cmd);
}

std::vector<char> Protocol::EncodeRequest(const Request& request) {
    std::vector<char> buf;
    
    std::visit([&](auto&& req) {
        using T = std::decay_t<decltype(req)>;
        
        if constexpr (std::is_same_v<T, PutRequest>) {
            EncodeArray(buf, 3);
            EncodeBulkString(buf, "PUT");
            EncodeBulkString(buf, req.key);
            EncodeBulkString(buf, req.value);
        }
        else if constexpr (std::is_same_v<T, DeleteRequest>) {
            EncodeArray(buf, 2);
            EncodeBulkString(buf, "DEL");
            EncodeBulkString(buf, req.key);
        }
        else if constexpr (std::is_same_v<T, GetRequest>) {
            size_t count = 2;
            if (req.snapshot.has_value()) count++;
            if (req.field.has_value()) count++;
            
            EncodeArray(buf, count);
            EncodeBulkString(buf, "GET");
            EncodeBulkString(buf, req.key);
            
            // Order matters? Protocol usually: CMD KEY [ARGS...]
            // Our Parse parses args[2] as either.
            // When encoding, we should probably be consistent.
            // If both present? Unlikely given logic.
            // Just append available ones.
            if (req.snapshot.has_value()) {
                EncodeBulkString(buf, std::to_string(req.snapshot.value()));
            }
            if (req.field.has_value()) {
                EncodeBulkString(buf, req.field.value());
            }
        }
        else if constexpr (std::is_same_v<T, ExecPythonRequest>) {
            EncodeArray(buf, 2);
            EncodeBulkString(buf, "EXEC");
            EncodeBulkString(buf, req.script);
        }
        else if constexpr (std::is_same_v<T, PingRequest>) {
            EncodeArray(buf, 1);
            EncodeBulkString(buf, "PING");
        }
        else if constexpr (std::is_same_v<T, StatusRequest>) {
            EncodeArray(buf, 1);
            EncodeBulkString(buf, "STATUS");
        }
        else if constexpr (std::is_same_v<T, FlushRequest>) {
            EncodeArray(buf, 1);
            EncodeBulkString(buf, "FLUSH");
        }
        else if constexpr (std::is_same_v<T, CompactRequest>) {
            size_t count = (req.level >= 0) ? 2 : 1;
            EncodeArray(buf, count);
            EncodeBulkString(buf, "COMPACT");
            if (req.level >= 0) {
                 EncodeBulkString(buf, std::to_string(req.level));
            }
        }
        else if constexpr (std::is_same_v<T, IntrospectRequest>) {
            EncodeArray(buf, 2);
            EncodeBulkString(buf, "INTROSPECT");
            EncodeBulkString(buf, req.target);
        }
    }, request);
    
    return buf;
}

std::vector<char> Protocol::EncodeResponse(const Response& response) {
    std::vector<char> buf;
    
    std::visit([&](auto&& resp) {
        using T = std::decay_t<decltype(resp)>;
        
        if constexpr (std::is_same_v<T, OkResponse>) {
            EncodeSimpleString(buf, resp.message.empty() ? "OK" : resp.message);
        }
        else if constexpr (std::is_same_v<T, ValueResponse>) {
            EncodeBulkString(buf, resp.value);
        }
        else if constexpr (std::is_same_v<T, ErrorResponse>) {
            EncodeError(buf, "ERR " + resp.message);
        }
        else if constexpr (std::is_same_v<T, StatusResponse>) {
            // Encode as Bulk String with key:value lines
            std::string info;
            info += "entries:" + std::to_string(resp.entries) + "\n";
            info += "memtable_size:" + std::to_string(resp.memtable_size) + "\n";
            info += "sstables:" + std::to_string(resp.sstable_count) + "\n";
            info += "version:" + resp.version + "\n";
            EncodeBulkString(buf, info);
        }
    }, response);
    
    return buf;
}

Result<Response> Protocol::ParseResponse(const Slice& data) {
    if (data.empty()) return Status::InvalidArgument("Empty data");
    size_t offset = 0;
    
    char type = data[0];
    
    if (type == '+') {
        auto res = DecodeSimpleString(data, offset);
        if (!res.ok()) return res.status();
        return Response{OkResponse{res.value()}};
    }
    else if (type == '-') {
        // Error
        auto res = DecodeError(data, offset);
        if (!res.ok()) return res.status();
        // Strip ERR prefix if present
        std::string msg = res.value();
        if (msg.rfind("ERR ", 0) == 0) msg.erase(0, 4);
        return Response{ErrorResponse{response::kError, msg}};
    }
    else if (type == '$') {
        auto res = DecodeBulkString(data, offset);
        if (res.status().code() == StatusCode::kNotFound) {
             // Null Bulk String -> "Not Found" error for now?
             // Or we need a NullResponse type?
             // For Get, existing code expects ErrorResponse(kNotFound) on miss.
             return Response{ErrorResponse{response::kNotFound, "Not found"}};
        }
        if (!res.ok()) return res.status();
        
        // Is it status info? No way to know from type.
        // Assume ValueResponse. 
        // If the caller expects StatusResponse, it might need to parse the string.
        // But ParseResponse returns variant.
        // Let's assume generic ValueResponse for bulk strings.
        // But wait, DecodeBulkString is also used for Status.
        // Existing Dashboard logic expects StatusResponse struct.
        // But Protocol::ParseResponse returns `Response` variant.
        // If I return ValueResponse, dashboard will print the string.
        // Which is actually FINE for `dashboard.cpp` console!
        // `render_console` logic:
        // if ValueResponse -> print r.value
        // So formatting Status as string is actually BETTER for console.
        
        return Response{ValueResponse{res.value()}};
    }
    else if (type == ':') {
        auto res = DecodeInteger(data, offset);
        if (!res.ok()) return res.status();
        return Response{ValueResponse{std::to_string(res.value())}};
    }
    
    return Status::InvalidArgument("Unknown RESP type");
}

} // namespace mydb
