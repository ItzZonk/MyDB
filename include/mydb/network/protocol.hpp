#pragma once

// Protocol: [Op(1B)][PayloadLen(4B)][Payload(var)]

#include <mydb/common/types.hpp>
#include <mydb/common/status.hpp>
#include <mydb/common/slice.hpp>
#include <mydb/config.hpp>
#include <vector>
#include <string>
#include <variant>
#include <optional>

namespace mydb {

struct PutRequest { std::string key, value; };
struct DeleteRequest { std::string key; };
struct GetRequest {
    std::string key;
    std::optional<SequenceNumber> snapshot; // If empty, use latest
};
struct ExecPythonRequest { std::string script; };
struct PingRequest {};
struct StatusRequest {};
struct FlushRequest {};
struct CompactRequest { int level{-1}; };

using Request = std::variant<PutRequest, DeleteRequest, GetRequest, ExecPythonRequest, PingRequest, StatusRequest, FlushRequest, CompactRequest>;

struct OkResponse { std::string message; };
struct ValueResponse { std::string value; };
struct ErrorResponse { uint8_t code; std::string message; };
struct StatusResponse { uint64_t entries, memtable_size, sstable_count; std::string version; };

using Response = std::variant<OkResponse, ValueResponse, ErrorResponse, StatusResponse>;

class Protocol {
public:
    static Result<Request> ParseRequest(const Slice& data);
    static std::vector<char> EncodeRequest(const Request& request);
    static Result<Response> ParseResponse(const Slice& data);
    static std::vector<char> EncodeResponse(const Response& response);
    
    static constexpr size_t kHeaderSize = 5;
    static bool HasCompleteMessage(const Slice& data);
    static size_t GetMessageLength(const Slice& header);
    
private:
    static void EncodeUint8(std::vector<char>& buf, uint8_t value);
    static void EncodeUint32(std::vector<char>& buf, uint32_t value);
    static void EncodeUint64(std::vector<char>& buf, uint64_t value);
    static void EncodeString(std::vector<char>& buf, const std::string& str);
    static Result<uint8_t> DecodeUint8(const Slice& data, size_t& offset);
    static Result<uint32_t> DecodeUint32(const Slice& data, size_t& offset);
    static Result<uint64_t> DecodeUint64(const Slice& data, size_t& offset);
    static Result<std::string> DecodeString(const Slice& data, size_t& offset);
};

} // namespace mydb
