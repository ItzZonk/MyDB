/**
 * @file page.hpp
 * @brief Slotted page implementation for relational storage
 * 
 * Implements the standard slotted page design used in modern RDBMS:
 * - Fixed 4KB page size (matching OS page size)
 * - Header with metadata (page ID, LSN, tuple count)
 * - Slot array growing downward from header
 * - Tuple data growing upward from end of page
 * 
 * This design enables efficient space management and variable-length tuples.
 * 
 * @see UGent Courses: Besturingssystemen (Operating Systems), Systems Architecture
 */

#pragma once

#include <cstdint>
#include <cstring>
#include <atomic>

namespace mydb {

// Type aliases for clarity
using page_id_t = int32_t;
using frame_id_t = int32_t;
using lsn_t = int64_t;
using txn_id_t = int64_t;
using slot_offset_t = uint16_t;

// Constants
static constexpr page_id_t INVALID_PAGE_ID = -1;
static constexpr frame_id_t INVALID_FRAME_ID = -1;
static constexpr size_t PAGE_SIZE = 4096;  // 4KB standard page

/**
 * @brief Slot directory entry pointing to tuple data
 */
struct Slot {
    slot_offset_t offset{0};   ///< Offset from start of page to tuple data
    uint16_t length{0};         ///< Length of tuple data in bytes
    bool is_valid{false};       ///< Whether this slot contains valid data
};

/**
 * @brief Page header containing metadata
 * 
 * Located at the beginning of each page. Tracks:
 * - Page identification (page_id, LSN)
 * - Space management (tuple_count, free_space_pointer)
 * - Concurrency control (pin_count, dirty flag)
 */
struct PageHeader {
    page_id_t page_id{INVALID_PAGE_ID};
    lsn_t lsn{0};                          ///< Log Sequence Number for recovery
    uint16_t tuple_count{0};               ///< Number of valid tuples
    slot_offset_t free_space_pointer{0};   ///< Points to start of free space
    slot_offset_t slot_array_end{0};       ///< Points to end of slot array
    uint32_t checksum{0};                  ///< Data integrity check
};

/**
 * @brief Represents a database page in memory
 * 
 * The Page class wraps a raw 4KB byte array and provides methods
 * for slotted page operations. This is the fundamental unit of
 * storage in the buffer pool.
 * 
 * Memory Layout:
 * ```
 * +------------------+
 * |   Page Header    |  <- Fixed size header
 * +------------------+
 * |   Slot Array     |  <- Grows downward
 * |        |         |
 * |        v         |
 * |                  |
 * |   Free Space     |
 * |                  |
 * |        ^         |
 * |        |         |
 * |   Tuple Data     |  <- Grows upward from end
 * +------------------+
 * ```
 */
class Page {
public:
    Page() {
        ResetMemory();
    }

    /**
     * @brief Get raw data pointer for direct access
     * @return Pointer to the underlying page data
     */
    char* GetData() { return data_; }
    const char* GetData() const { return data_; }

    /**
     * @brief Get the page identifier
     */
    page_id_t GetPageId() const { return GetHeader()->page_id; }
    void SetPageId(page_id_t page_id) { GetHeader()->page_id = page_id; }

    /**
     * @brief Get/Set the Log Sequence Number
     */
    lsn_t GetLSN() const { return GetHeader()->lsn; }
    void SetLSN(lsn_t lsn) { GetHeader()->lsn = lsn; }

    /**
     * @brief Pin count management for buffer pool
     */
    int GetPinCount() const { return pin_count_; }
    void IncrementPinCount() { ++pin_count_; }
    void DecrementPinCount() { 
        if (pin_count_ > 0) --pin_count_; 
    }

    /**
     * @brief Dirty flag for write-back policy
     */
    bool IsDirty() const { return is_dirty_; }
    void SetDirty(bool dirty) { is_dirty_ = dirty; }

    /**
     * @brief Get the number of tuples stored in this page
     */
    uint16_t GetTupleCount() const { return GetHeader()->tuple_count; }

    /**
     * @brief Calculate remaining free space
     * @return Number of bytes available for new tuples
     */
    size_t GetFreeSpace() const {
        const auto* header = GetHeader();
        size_t header_size = sizeof(PageHeader);
        size_t slots_size = header->tuple_count * sizeof(Slot);
        return PAGE_SIZE - header_size - slots_size - 
               (PAGE_SIZE - header->free_space_pointer);
    }

    /**
     * @brief Insert tuple data into the page
     * @param data Pointer to tuple data
     * @param size Size of tuple data in bytes
     * @return Slot index if successful, -1 if page is full
     */
    int InsertTuple(const char* data, size_t size) {
        if (size + sizeof(Slot) > GetFreeSpace()) {
            return -1;  // Not enough space
        }

        auto* header = GetHeader();
        
        // Calculate new tuple position (grow from end)
        slot_offset_t tuple_offset = header->free_space_pointer - static_cast<slot_offset_t>(size);
        
        // Copy tuple data
        std::memcpy(data_ + tuple_offset, data, size);
        
        // Create slot entry
        Slot* slots = GetSlotArray();
        int slot_idx = header->tuple_count;
        slots[slot_idx].offset = tuple_offset;
        slots[slot_idx].length = static_cast<uint16_t>(size);
        slots[slot_idx].is_valid = true;
        
        // Update header
        header->free_space_pointer = tuple_offset;
        header->tuple_count++;
        
        is_dirty_ = true;
        return slot_idx;
    }

    /**
     * @brief Get tuple data by slot index
     * @param slot_idx Index of the slot
     * @param[out] size Size of the tuple data
     * @return Pointer to tuple data, nullptr if invalid
     */
    const char* GetTuple(int slot_idx, size_t* size) const {
        const auto* header = GetHeader();
        if (slot_idx < 0 || slot_idx >= header->tuple_count) {
            return nullptr;
        }
        
        const Slot* slots = GetSlotArray();
        if (!slots[slot_idx].is_valid) {
            return nullptr;
        }
        
        *size = slots[slot_idx].length;
        return data_ + slots[slot_idx].offset;
    }

    /**
     * @brief Delete tuple by slot index (marks as invalid)
     * @param slot_idx Index of the slot to delete
     * @return true if successful
     */
    bool DeleteTuple(int slot_idx) {
        auto* header = GetHeader();
        if (slot_idx < 0 || slot_idx >= header->tuple_count) {
            return false;
        }
        
        Slot* slots = GetSlotArray();
        if (!slots[slot_idx].is_valid) {
            return false;
        }
        
        slots[slot_idx].is_valid = false;
        is_dirty_ = true;
        return true;
    }

    /**
     * @brief Reset page to initial empty state
     */
    void ResetMemory() {
        std::memset(data_, 0, PAGE_SIZE);
        auto* header = GetHeader();
        header->page_id = INVALID_PAGE_ID;
        header->lsn = 0;
        header->tuple_count = 0;
        header->free_space_pointer = PAGE_SIZE;
        header->slot_array_end = sizeof(PageHeader);
        pin_count_ = 0;
        is_dirty_ = false;
    }

private:
    /**
     * @brief Get header pointer (at start of page)
     */
    PageHeader* GetHeader() {
        return reinterpret_cast<PageHeader*>(data_);
    }
    const PageHeader* GetHeader() const {
        return reinterpret_cast<const PageHeader*>(data_);
    }

    /**
     * @brief Get slot array pointer (after header)
     */
    Slot* GetSlotArray() {
        return reinterpret_cast<Slot*>(data_ + sizeof(PageHeader));
    }
    const Slot* GetSlotArray() const {
        return reinterpret_cast<const Slot*>(data_ + sizeof(PageHeader));
    }

    // Page data storage
    char data_[PAGE_SIZE]{};
    
    // Buffer pool metadata (not persisted to disk)
    int pin_count_{0};
    bool is_dirty_{false};
};

}  // namespace mydb
