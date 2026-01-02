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
    std::optional<std::string> field;       // JSON field to extract
};
struct ExecPythonRequest { std::string script; };
struct PingRequest {};
struct StatusRequest {};
struct FlushRequest {};
struct CompactRequest { int level{-1}; };
struct IntrospectRequest { std::string target; };

using Request = std::variant<PutRequest, DeleteRequest, GetRequest, ExecPythonRequest, PingRequest, StatusRequest, FlushRequest, CompactRequest, IntrospectRequest>;

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
    
    // RESP Helpers
    static bool HasCompleteMessage(const Slice& data);
    
private:
    // RESP Encoding
    static void EncodeSimpleString(std::vector<char>& buf, const std::string& str);
    static void EncodeError(std::vector<char>& buf, const std::string& str);
    static void EncodeInteger(std::vector<char>& buf, int64_t val);
    static void EncodeBulkString(std::vector<char>& buf, const std::string& str);
    static void EncodeArray(std::vector<char>& buf, size_t count);
    
    // RESP Decoding
    static Result<std::string> DecodeSimpleString(const Slice& data, size_t& offset);
    static Result<std::string> DecodeError(const Slice& data, size_t& offset);
    static Result<int64_t> DecodeInteger(const Slice& data, size_t& offset);
    static Result<std::string> DecodeBulkString(const Slice& data, size_t& offset);
    static Result<size_t> DecodeArrayHeader(const Slice& data, size_t& offset);
    
    // Utility
    static std::optional<size_t> FindCRLF(const Slice& data, size_t offset);
};

} // namespace mydb
