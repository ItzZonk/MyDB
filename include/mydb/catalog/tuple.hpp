/**
 * @file tuple.hpp
 * @brief Tuple (row) representation with veracity support
 * 
 * A tuple is the in-memory representation of a database row.
 * Supports serialization/deserialization for disk storage and
 * includes a veracity (confidence) score for data quality tracking.
 * 
 * The veracity feature aligns with UGent DDCM research on data quality.
 * 
 * @see UGent Research: DDCM Group (Prof. Guy De Tré)
 */

#pragma once

#include <mydb/catalog/schema.hpp>
#include <mydb/catalog/value.hpp>
#include <mydb/storage/page.hpp>

#include <vector>
#include <cstring>

namespace mydb {

/**
 * @brief Record ID - uniquely identifies a tuple on disk
 */
struct RID {
    page_id_t page_id{INVALID_PAGE_ID};
    uint32_t slot_num{0};

    RID() = default;
    RID(page_id_t pid, uint32_t slot) : page_id(pid), slot_num(slot) {}

    bool operator==(const RID& other) const {
        return page_id == other.page_id && slot_num == other.slot_num;
    }

    bool operator!=(const RID& other) const {
        return !(*this == other);
    }

    bool IsValid() const {
        return page_id != INVALID_PAGE_ID;
    }

    std::string ToString() const {
        return "(" + std::to_string(page_id) + ", " + std::to_string(slot_num) + ")";
    }
};

/**
 * @brief Tuple header containing metadata
 * 
 * Stored at the beginning of each serialized tuple.
 */
struct TupleHeader {
    uint32_t size{0};           ///< Total size of tuple data in bytes
    float veracity{1.0f};       ///< Confidence score [0.0, 1.0]
    uint32_t timestamp{0};      ///< Creation/modification timestamp
    bool is_deleted{false};     ///< Tombstone flag for soft deletes
};

/**
 * @brief Represents a database row (tuple)
 * 
 * A tuple holds a list of values corresponding to a schema.
 * It can be serialized to a byte array for storage and
 * deserialized back to retrieve individual column values.
 * 
 * Special Features:
 * - **Veracity Score**: Each tuple has a confidence score (0.0 to 1.0)
 *   indicating data quality. This aligns with the "Veracity" dimension
 *   of Big Data and UGent's DDCM research.
 * 
 * Memory Layout (Serialized):
 * ```
 * +----------------+
 * | TupleHeader    |  <- Size, veracity, timestamp, deleted flag
 * +----------------+
 * | Column 1 Data  |
 * +----------------+
 * | Column 2 Data  |
 * +----------------+
 * | ...            |
 * +----------------+
 * ```
 */
class Tuple {
public:
    /**
     * @brief Default constructor creates an empty tuple
     */
    Tuple() = default;

    /**
     * @brief Construct a tuple with values
     * @param values List of values (must match schema order)
     * @param schema The schema for this tuple
     * @param veracity Confidence score (default: 1.0 = fully trusted)
     */
    Tuple(std::vector<Value> values, const Schema* schema, float veracity = 1.0f)
        : values_(std::move(values)), veracity_(veracity) {
        
        if (schema && values_.size() != schema->GetColumnCount()) {
            throw std::runtime_error("Value count mismatch with schema");
        }
    }

    /**
     * @brief Get value at a specific column index
     * @param schema The schema (for type information)
     * @param column_idx Column index
     * @return The value at that column
     */
    Value GetValue(const Schema* schema, uint32_t column_idx) const {
        if (column_idx >= values_.size()) {
            throw std::out_of_range("Column index out of range");
        }
        return values_[column_idx];
    }

    /**
     * @brief Get value by column name
     */
    Value GetValue(const Schema* schema, const std::string& column_name) const {
        int idx = schema->GetColumnIndex(column_name);
        if (idx < 0) {
            throw std::runtime_error("Column not found: " + column_name);
        }
        return values_[idx];
    }

    /**
     * @brief Get all values
     */
    const std::vector<Value>& GetValues() const { return values_; }

    /**
     * @brief Get the veracity (confidence) score
     * 
     * The veracity score indicates data quality:
     * - 1.0: Fully trusted (e.g., verified data)
     * - 0.5-0.9: Partially trusted (e.g., user input)
     * - 0.0-0.5: Low confidence (e.g., web scraped, sensor noise)
     */
    float GetVeracity() const { return veracity_; }

    /**
     * @brief Set the veracity score
     */
    void SetVeracity(float veracity) { 
        veracity_ = std::clamp(veracity, 0.0f, 1.0f); 
    }

    /**
     * @brief Get the Record ID
     */
    const RID& GetRID() const { return rid_; }

    /**
     * @brief Set the Record ID
     */
    void SetRID(const RID& rid) { rid_ = rid; }

    /**
     * @brief Check if tuple is empty
     */
    bool IsEmpty() const { return values_.empty(); }

    /**
     * @brief Get number of columns
     */
    size_t GetColumnCount() const { return values_.size(); }

    /**
     * @brief Serialize tuple to byte buffer
     * @param buffer Destination buffer
     * @param schema The schema for serialization
     * @return Number of bytes written
     * 
     * The serialized format includes a header followed by column data.
     */
    size_t SerializeTo(char* buffer, const Schema* schema) const {
        char* ptr = buffer;
        
        // Write header
        TupleHeader header;
        header.veracity = veracity_;
        header.is_deleted = false;
        std::memcpy(ptr, &header, sizeof(TupleHeader));
        ptr += sizeof(TupleHeader);
        
        // Write each column value
        for (size_t i = 0; i < values_.size(); ++i) {
            ptr += values_[i].SerializeTo(ptr);
        }
        
        // Update size in header
        size_t total_size = ptr - buffer;
        header.size = static_cast<uint32_t>(total_size);
        std::memcpy(buffer, &header, sizeof(TupleHeader));
        
        return total_size;
    }

    /**
     * @brief Get serialized size
     */
    size_t GetSerializedSize(const Schema* schema) const {
        size_t size = sizeof(TupleHeader);
        for (size_t i = 0; i < values_.size(); ++i) {
            const auto& col = schema->GetColumn(static_cast<uint32_t>(i));
            if (col.IsVariableLength()) {
                size += sizeof(uint32_t) + values_[i].GetAsString().size();
            } else {
                size += col.GetLength();
            }
        }
        return size;
    }

    /**
     * @brief Deserialize tuple from byte buffer
     * @param buffer Source buffer
     * @param schema The schema for deserialization
     * @return The deserialized tuple
     */
    static Tuple DeserializeFrom(const char* buffer, const Schema* schema) {
        const char* ptr = buffer;
        
        // Read header
        TupleHeader header;
        std::memcpy(&header, ptr, sizeof(TupleHeader));
        ptr += sizeof(TupleHeader);
        
        // Read each column value
        std::vector<Value> values;
        values.reserve(schema->GetColumnCount());
        
        for (size_t i = 0; i < schema->GetColumnCount(); ++i) {
            const auto& col = schema->GetColumn(static_cast<uint32_t>(i));
            Value val = Value::DeserializeFrom(col.GetType(), ptr, col.GetLength());
            values.push_back(std::move(val));
            
            // Advance pointer
            if (col.IsVariableLength()) {
                uint32_t len;
                std::memcpy(&len, ptr, sizeof(len));
                ptr += sizeof(len) + len;
            } else {
                ptr += col.GetLength();
            }
        }
        
        Tuple tuple(std::move(values), schema, header.veracity);
        return tuple;
    }

    /**
     * @brief Convert tuple to string representation
     */
    std::string ToString(const Schema* schema = nullptr) const {
        std::string result = "(";
        for (size_t i = 0; i < values_.size(); ++i) {
            if (i > 0) result += ", ";
            result += values_[i].ToString();
        }
        result += ") [veracity=" + std::to_string(veracity_) + "]";
        return result;
    }

private:
    std::vector<Value> values_;
    float veracity_{1.0f};
    RID rid_;
};

}  // namespace mydb

// Hash function for RID (for use in unordered containers)
namespace std {
    template <>
    struct hash<mydb::RID> {
        size_t operator()(const mydb::RID& rid) const {
            return hash<int64_t>()(
                (static_cast<int64_t>(rid.page_id) << 32) | rid.slot_num
            );
        }
    };
}
