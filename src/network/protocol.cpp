/**
 * @file protocol.cpp
 * @brief Binary protocol implementation
 */

#include <mydb/network/protocol.hpp>

#include <cstring>

namespace mydb {

// ============================================================================
// Encode Helpers
// ============================================================================

void Protocol::EncodeUint8(std::vector<char>& buf, uint8_t value) {
    buf.push_back(static_cast<char>(value));
}

void Protocol::EncodeUint32(std::vector<char>& buf, uint32_t value) {
    for (int i = 0; i < 4; ++i) {
        buf.push_back(static_cast<char>((value >> (i * 8)) & 0xFF));
    }
}

void Protocol::EncodeUint64(std::vector<char>& buf, uint64_t value) {
    for (int i = 0; i < 8; ++i) {
        buf.push_back(static_cast<char>((value >> (i * 8)) & 0xFF));
    }
}

void Protocol::EncodeString(std::vector<char>& buf, const std::string& str) {
    EncodeUint32(buf, static_cast<uint32_t>(str.size()));
    buf.insert(buf.end(), str.begin(), str.end());
}

// ============================================================================
// Decode Helpers
// ============================================================================

Result<uint8_t> Protocol::DecodeUint8(const Slice& data, size_t& offset) {
    if (offset >= data.size()) {
        return Status::Corruption("Buffer underflow");
    }
    return static_cast<uint8_t>(data[offset++]);
}

Result<uint32_t> Protocol::DecodeUint32(const Slice& data, size_t& offset) {
    if (offset + 4 > data.size()) {
        return Status::Corruption("Buffer underflow");
    }
    uint32_t value = 0;
    for (int i = 0; i < 4; ++i) {
        value |= (static_cast<uint32_t>(static_cast<unsigned char>(data[offset + i])) << (i * 8));
    }
    offset += 4;
    return value;
}

Result<uint64_t> Protocol::DecodeUint64(const Slice& data, size_t& offset) {
    if (offset + 8 > data.size()) {
        return Status::Corruption("Buffer underflow");
    }
    uint64_t value = 0;
    for (int i = 0; i < 8; ++i) {
        value |= (static_cast<uint64_t>(static_cast<unsigned char>(data[offset + i])) << (i * 8));
    }
    offset += 8;
    return value;
}

Result<std::string> Protocol::DecodeString(const Slice& data, size_t& offset) {
    auto len_result = DecodeUint32(data, offset);
    if (!len_result.ok()) return len_result.status();
    
    uint32_t len = len_result.value();
    if (offset + len > data.size()) {
        return Status::Corruption("String truncated");
    }
    
    std::string result(data.data() + offset, len);
    offset += len;
    return result;
}

// ============================================================================
// Request Parsing
// ============================================================================

bool Protocol::HasCompleteMessage(const Slice& data) {
    if (data.size() < kHeaderSize) return false;
    
    size_t offset = 1;  // Skip opcode
    uint32_t len = 0;
    for (int i = 0; i < 4; ++i) {
        len |= (static_cast<uint32_t>(static_cast<unsigned char>(data[offset + i])) << (i * 8));
    }
    
    return data.size() >= kHeaderSize + len;
}

size_t Protocol::GetMessageLength(const Slice& header) {
    if (header.size() < kHeaderSize) return 0;
    
    size_t offset = 1;  // Skip opcode
    uint32_t len = 0;
    for (int i = 0; i < 4; ++i) {
        len |= (static_cast<uint32_t>(static_cast<unsigned char>(header[offset + i])) << (i * 8));
    }
    return kHeaderSize + len;
}

Result<Request> Protocol::ParseRequest(const Slice& data) {
    if (data.size() < kHeaderSize) {
        return Status::InvalidArgument("Request too short");
    }
    
    size_t offset = 0;
    
    auto op_result = DecodeUint8(data, offset);
    if (!op_result.ok()) return op_result.status();
    uint8_t opcode = op_result.value();
    
    // Skip payload length
    offset += 4;
    
    switch (opcode) {
        case opcode::kPut: {
            auto key_result = DecodeString(data, offset);
            if (!key_result.ok()) return key_result.status();
            
            auto val_result = DecodeString(data, offset);
            if (!val_result.ok()) return val_result.status();
            
            return Request{PutRequest{key_result.value(), val_result.value()}};
        }
        
        case opcode::kDelete: {
            auto key_result = DecodeString(data, offset);
            if (!key_result.ok()) return key_result.status();
            
            return Request{DeleteRequest{key_result.value()}};
        }
        
        case opcode::kGet: {
            auto key_result = DecodeString(data, offset);
            if (!key_result.ok()) return key_result.status();
            
            std::optional<SequenceNumber> snapshot;
            if (offset < data.size()) {
                auto has_snapshot = DecodeUint8(data, offset);
                if (has_snapshot.ok() && has_snapshot.value() == 1) {
                    auto snap_val = DecodeUint64(data, offset);
                    if (snap_val.ok()) {
                        snapshot = snap_val.value();
                    }
                }
            }
            
            return Request{GetRequest{key_result.value(), snapshot}};
        }
        
        case opcode::kExecPython: {
            auto script_result = DecodeString(data, offset);
            if (!script_result.ok()) return script_result.status();
            
            return Request{ExecPythonRequest{script_result.value()}};
        }
        
        case opcode::kPing:
            return Request{PingRequest{}};
        
        case opcode::kStatus:
            return Request{StatusRequest{}};
        
        case opcode::kFlush:
            return Request{FlushRequest{}};
        
        case opcode::kCompact: {
            int level = -1;
            if (offset < data.size()) {
                auto level_result = DecodeUint32(data, offset);
                if (level_result.ok()) {
                    level = static_cast<int>(level_result.value());
                }
            }
            return Request{CompactRequest{level}};
        }
        
        default:
            return Status::InvalidArgument("Unknown opcode");
    }
}

// ============================================================================
// Request Encoding
// ============================================================================

std::vector<char> Protocol::EncodeRequest(const Request& request) {
    std::vector<char> buf;
    std::vector<char> payload;
    
    std::visit([&](auto&& req) {
        using T = std::decay_t<decltype(req)>;
        
        if constexpr (std::is_same_v<T, PutRequest>) {
            EncodeUint8(buf, opcode::kPut);
            EncodeString(payload, req.key);
            EncodeString(payload, req.value);
        }
        else if constexpr (std::is_same_v<T, DeleteRequest>) {
            EncodeUint8(buf, opcode::kDelete);
            EncodeString(payload, req.key);
        }
        else if constexpr (std::is_same_v<T, GetRequest>) {
            EncodeUint8(buf, opcode::kGet);
            EncodeString(payload, req.key);
            if (req.snapshot.has_value()) {
                EncodeUint8(payload, 1);
                EncodeUint64(payload, req.snapshot.value());
            } else {
                EncodeUint8(payload, 0);
            }
        }
        else if constexpr (std::is_same_v<T, ExecPythonRequest>) {
            EncodeUint8(buf, opcode::kExecPython);
            EncodeString(payload, req.script);
        }
        else if constexpr (std::is_same_v<T, PingRequest>) {
            EncodeUint8(buf, opcode::kPing);
        }
        else if constexpr (std::is_same_v<T, StatusRequest>) {
            EncodeUint8(buf, opcode::kStatus);
        }
        else if constexpr (std::is_same_v<T, FlushRequest>) {
            EncodeUint8(buf, opcode::kFlush);
        }
        else if constexpr (std::is_same_v<T, CompactRequest>) {
            EncodeUint8(buf, opcode::kCompact);
            EncodeUint32(payload, static_cast<uint32_t>(req.level));
        }
    }, request);
    
    EncodeUint32(buf, static_cast<uint32_t>(payload.size()));
    buf.insert(buf.end(), payload.begin(), payload.end());
    
    return buf;
}

// ============================================================================
// Response Encoding
// ============================================================================

std::vector<char> Protocol::EncodeResponse(const Response& response) {
    std::vector<char> buf;
    
    std::visit([&](auto&& resp) {
        using T = std::decay_t<decltype(resp)>;
        
        if constexpr (std::is_same_v<T, OkResponse>) {
            EncodeUint8(buf, response::kOk);
            EncodeString(buf, resp.message);
        }
        else if constexpr (std::is_same_v<T, ValueResponse>) {
            EncodeUint8(buf, response::kOk);
            EncodeString(buf, resp.value);
        }
        else if constexpr (std::is_same_v<T, ErrorResponse>) {
            EncodeUint8(buf, resp.code);
            EncodeString(buf, resp.message);
        }
        else if constexpr (std::is_same_v<T, StatusResponse>) {
            EncodeUint8(buf, response::kOk);
            EncodeUint64(buf, resp.entries);
            EncodeUint64(buf, resp.memtable_size);
            EncodeUint64(buf, resp.sstable_count);
            EncodeString(buf, resp.version);
        }
    }, response);
    
    return buf;
}

// ============================================================================
// Response Parsing
// ============================================================================

Result<Response> Protocol::ParseResponse(const Slice& data) {
    if (data.empty()) {
        return Status::InvalidArgument("Empty response");
    }
    
    size_t offset = 0;
    
    auto code_result = DecodeUint8(data, offset);
    if (!code_result.ok()) return code_result.status();
    
    uint8_t code = code_result.value();
    
    if (code == response::kOk) {
        auto str_result = DecodeString(data, offset);
        if (str_result.ok()) {
            return Response{ValueResponse{str_result.value()}};
        }
        return Response{OkResponse{""}};
    }
    
    auto msg_result = DecodeString(data, offset);
    std::string msg = msg_result.ok() ? msg_result.value() : "";
    
    return Response{ErrorResponse{code, msg}};
}

} // namespace mydb
