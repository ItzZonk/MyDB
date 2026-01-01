/**
 * @file schema.hpp
 * @brief Table schema and column definitions
 * 
 * Defines the structure of database tables including column names,
 * types, and layout information for tuple serialization.
 * 
 * @see UGent Courses: Databases, Objectgericht Programmeren
 */

#pragma once

#include <mydb/catalog/value.hpp>

#include <string>
#include <vector>
#include <stdexcept>
#include <unordered_map>

namespace mydb {

/**
 * @brief Represents a column in a table schema
 * 
 * Stores the column name, data type, length (for VARCHAR),
 * and computed offset within a tuple.
 */
class Column {
public:
    /**
     * @brief Construct a fixed-size column
     * @param name Column name
     * @param type Data type (must not be VARCHAR)
     */
    Column(std::string name, TypeId type)
        : name_(std::move(name)), 
          type_(type), 
          length_(GetTypeSize(type)),
          variable_length_(false) {
        if (type == TypeId::VARCHAR) {
            throw std::invalid_argument("VARCHAR requires explicit length");
        }
    }

    /**
     * @brief Construct a variable-length column (VARCHAR)
     * @param name Column name
     * @param type Data type (typically VARCHAR)
     * @param length Maximum length for VARCHAR
     */
    Column(std::string name, TypeId type, uint32_t length)
        : name_(std::move(name)), 
          type_(type), 
          length_(length),
          variable_length_(type == TypeId::VARCHAR) {}

    /**
     * @brief Get the column name
     */
    const std::string& GetName() const { return name_; }

    /**
     * @brief Get the column type
     */
    TypeId GetType() const { return type_; }

    /**
     * @brief Get the column length
     * 
     * For fixed-size types, returns the type size.
     * For VARCHAR, returns the maximum length.
     */
    uint32_t GetLength() const { return length_; }

    /**
     * @brief Check if this is a variable-length column
     */
    bool IsVariableLength() const { return variable_length_; }

    /**
     * @brief Get the fixed storage size for this column
     * 
     * For VARCHAR, returns 4 bytes (length prefix) + max length.
     * For fixed types, returns the type size.
     */
    uint32_t GetStorageSize() const {
        if (variable_length_) {
            return sizeof(uint32_t) + length_;  // Length prefix + data
        }
        return length_;
    }

    /**
     * @brief Get the offset of this column within a tuple
     */
    uint32_t GetOffset() const { return offset_; }

    /**
     * @brief Set the offset (called by Schema)
     */
    void SetOffset(uint32_t offset) { offset_ = offset; }

    /**
     * @brief Convert to string representation
     */
    std::string ToString() const {
        std::string result = name_ + " " + TypeIdToString(type_);
        if (type_ == TypeId::VARCHAR) {
            result += "(" + std::to_string(length_) + ")";
        }
        return result;
    }

private:
    std::string name_;
    TypeId type_;
    uint32_t length_;
    bool variable_length_;
    uint32_t offset_{0};  ///< Offset within tuple (computed by Schema)
};

/**
 * @brief Represents the schema of a table
 * 
 * A schema contains an ordered list of columns and provides
 * methods for computing tuple sizes and column lookups.
 */
class Schema {
public:
    /**
     * @brief Construct a schema from a list of columns
     * @param columns The columns in order
     */
    explicit Schema(std::vector<Column> columns)
        : columns_(std::move(columns)) {
        
        // Compute offsets and total tuple size
        uint32_t offset = 0;
        for (size_t i = 0; i < columns_.size(); ++i) {
            columns_[i].SetOffset(offset);
            column_indices_[columns_[i].GetName()] = i;
            offset += columns_[i].GetStorageSize();
        }
        tuple_size_ = offset;
    }

    /**
     * @brief Get a column by index
     */
    const Column& GetColumn(uint32_t idx) const {
        if (idx >= columns_.size()) {
            throw std::out_of_range("Column index out of range");
        }
        return columns_[idx];
    }

    /**
     * @brief Get a column by name
     */
    const Column& GetColumn(const std::string& name) const {
        auto it = column_indices_.find(name);
        if (it == column_indices_.end()) {
            throw std::out_of_range("Column not found: " + name);
        }
        return columns_[it->second];
    }

    /**
     * @brief Get column index by name
     * @return Column index, or -1 if not found
     */
    int GetColumnIndex(const std::string& name) const {
        auto it = column_indices_.find(name);
        if (it == column_indices_.end()) {
            return -1;
        }
        return static_cast<int>(it->second);
    }

    /**
     * @brief Get all columns
     */
    const std::vector<Column>& GetColumns() const { return columns_; }

    /**
     * @brief Get number of columns
     */
    size_t GetColumnCount() const { return columns_.size(); }

    /**
     * @brief Get the fixed tuple size
     * 
     * Note: For schemas with VARCHAR, this is the maximum size.
     */
    uint32_t GetTupleSize() const { return tuple_size_; }

    /**
     * @brief Check if schema has any variable-length columns
     */
    bool HasVariableLengthColumns() const {
        for (const auto& col : columns_) {
            if (col.IsVariableLength()) return true;
        }
        return false;
    }

    /**
     * @brief Convert to string representation
     */
    std::string ToString() const {
        std::string result = "(";
        for (size_t i = 0; i < columns_.size(); ++i) {
            if (i > 0) result += ", ";
            result += columns_[i].ToString();
        }
        result += ")";
        return result;
    }

private:
    std::vector<Column> columns_;
    std::unordered_map<std::string, size_t> column_indices_;
    uint32_t tuple_size_{0};
};

}  // namespace mydb
