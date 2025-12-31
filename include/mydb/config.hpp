#pragma once

#include <cstdint>
#include <cstddef>
#include <string_view>

namespace mydb {

// Version
inline constexpr std::string_view kVersion = "1.0.0";
inline constexpr uint32_t kMagicNumber = 0x4D594442; // "MYDB"

// Memory
inline constexpr size_t kMemTableMaxSize = 64 * 1024 * 1024;  // 64 MB
inline constexpr size_t kWriteBufferSize = 4 * 1024 * 1024;   // 4 MB
inline constexpr size_t kBlockSize = 4 * 1024;                // 4 KB

// SkipList
inline constexpr int kSkipListMaxHeight = 12;
inline constexpr double kSkipListProbability = 0.25;

// SSTable
inline constexpr size_t kSSTableBlockSize = 4 * 1024;
inline constexpr size_t kIndexBlockInterval = 16;
inline constexpr size_t kBloomFilterBitsPerKey = 10;  // ~1% false positive
inline constexpr size_t kMaxSSTableSize = 64 * 1024 * 1024;

// WAL
inline constexpr size_t kWALBlockSize = 32 * 1024;
inline constexpr size_t kWALMaxSize = 128 * 1024 * 1024;

// Compaction
inline constexpr size_t kLevel0CompactionTrigger = 4;
inline constexpr size_t kMaxLevels = 7;
inline constexpr size_t kLevelSizeMultiplier = 10;

// Network
inline constexpr uint16_t kDefaultPort = 6379;
inline constexpr size_t kMaxConnections = 1024;
inline constexpr size_t kReadBufferSize = 64 * 1024;
inline constexpr size_t kIOURingQueueDepth = 256;

// Protocol Opcodes
namespace opcode {
    inline constexpr uint8_t kPut = 0x01;
    inline constexpr uint8_t kDelete = 0x02;
    inline constexpr uint8_t kGet = 0x03;
    inline constexpr uint8_t kExecPython = 0x04;
    inline constexpr uint8_t kPing = 0x05;
    inline constexpr uint8_t kStatus = 0x06;
    inline constexpr uint8_t kFlush = 0x07;
    inline constexpr uint8_t kCompact = 0x08;
}

// Response Codes
namespace response {
    inline constexpr uint8_t kOk = 0x00;
    inline constexpr uint8_t kError = 0x01;
    inline constexpr uint8_t kNotFound = 0x02;
    inline constexpr uint8_t kCorrupted = 0x03;
    inline constexpr uint8_t kInvalidRequest = 0x04;
}

} // namespace mydb
