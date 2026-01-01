/**
 * @file value.hpp
 * @brief Runtime value representation for database tuples
 * 
 * Provides a type-safe wrapper for values that can be stored in the database.
 * Supports integers, floats, booleans, and variable-length strings.
 * 
 * @see UGent Courses: Objectgericht Programmeren (OOP)
 */

#pragma once

#include <string>
#include <variant>
#include <stdexcept>
#include <cstring>

namespace mydb {

/**
 * @brief Supported data types in the database
 */
enum class TypeId {
    INVALID = 0,
    BOOLEAN,
    INTEGER,
    BIGINT,
    FLOAT,
    DOUBLE,
    VARCHAR,
    TIMESTAMP
};

/**
 * @brief Get the size in bytes for a fixed-size type
 */
inline size_t GetTypeSize(TypeId type) {
    switch (type) {
        case TypeId::BOOLEAN: return 1;
        case TypeId::INTEGER: return 4;
        case TypeId::BIGINT: return 8;
        case TypeId::FLOAT: return 4;
        case TypeId::DOUBLE: return 8;
        case TypeId::TIMESTAMP: return 8;
        case TypeId::VARCHAR: return 0;  // Variable length
        default: return 0;
    }
}

/**
 * @brief Get string representation of a type
 */
inline std::string TypeIdToString(TypeId type) {
    switch (type) {
        case TypeId::BOOLEAN: return "BOOLEAN";
        case TypeId::INTEGER: return "INTEGER";
        case TypeId::BIGINT: return "BIGINT";
        case TypeId::FLOAT: return "FLOAT";
        case TypeId::DOUBLE: return "DOUBLE";
        case TypeId::VARCHAR: return "VARCHAR";
        case TypeId::TIMESTAMP: return "TIMESTAMP";
        default: return "INVALID";
    }
}

/**
 * @brief A typed value that can be stored in a tuple
 * 
 * Uses std::variant for type-safe storage of different value types.
 * Supports comparison, serialization, and type checking.
 */
class Value {
public:
    // Type alias for the underlying storage
    using Storage = std::variant<
        std::monostate,  // NULL/Invalid
        bool,            // BOOLEAN
        int32_t,         // INTEGER
        int64_t,         // BIGINT
        float,           // FLOAT
        double,          // DOUBLE
        std::string      // VARCHAR, TIMESTAMP
    >;

    /**
     * @brief Default constructor creates a NULL value
     */
    Value() : type_(TypeId::INVALID), data_(std::monostate{}) {}

    /**
     * @brief Construct a boolean value
     */
    explicit Value(bool val) : type_(TypeId::BOOLEAN), data_(val) {}

    /**
     * @brief Construct an integer value
     */
    explicit Value(int32_t val) : type_(TypeId::INTEGER), data_(val) {}

    /**
     * @brief Construct a bigint value
     */
    explicit Value(int64_t val) : type_(TypeId::BIGINT), data_(val) {}

    /**
     * @brief Construct a float value
     */
    explicit Value(float val) : type_(TypeId::FLOAT), data_(val) {}

    /**
     * @brief Construct a double value
     */
    explicit Value(double val) : type_(TypeId::DOUBLE), data_(val) {}

    /**
     * @brief Construct a varchar value
     */
    explicit Value(const std::string& val) : type_(TypeId::VARCHAR), data_(val) {}
    explicit Value(std::string&& val) : type_(TypeId::VARCHAR), data_(std::move(val)) {}
    explicit Value(const char* val) : type_(TypeId::VARCHAR), data_(std::string(val)) {}

    /**
     * @brief Construct a value with explicit type
     */
    Value(TypeId type, const std::string& val) : type_(type), data_(val) {}

    /**
     * @brief Get the type of this value
     */
    TypeId GetTypeId() const { return type_; }

    /**
     * @brief Check if this value is NULL
     */
    bool IsNull() const { 
        return type_ == TypeId::INVALID || 
               std::holds_alternative<std::monostate>(data_); 
    }

    /**
     * @brief Get the value as a boolean
     */
    bool GetAsBoolean() const {
        if (type_ != TypeId::BOOLEAN) {
            throw std::runtime_error("Value is not a boolean");
        }
        return std::get<bool>(data_);
    }

    /**
     * @brief Get the value as an integer
     */
    int32_t GetAsInteger() const {
        if (type_ != TypeId::INTEGER) {
            throw std::runtime_error("Value is not an integer");
        }
        return std::get<int32_t>(data_);
    }

    /**
     * @brief Get the value as a bigint
     */
    int64_t GetAsBigInt() const {
        if (type_ == TypeId::INTEGER) {
            return static_cast<int64_t>(std::get<int32_t>(data_));
        }
        if (type_ != TypeId::BIGINT) {
            throw std::runtime_error("Value is not a bigint");
        }
        return std::get<int64_t>(data_);
    }

    /**
     * @brief Get the value as a float
     */
    float GetAsFloat() const {
        if (type_ != TypeId::FLOAT) {
            throw std::runtime_error("Value is not a float");
        }
        return std::get<float>(data_);
    }

    /**
     * @brief Get the value as a double
     */
    double GetAsDouble() const {
        if (type_ == TypeId::FLOAT) {
            return static_cast<double>(std::get<float>(data_));
        }
        if (type_ != TypeId::DOUBLE) {
            throw std::runtime_error("Value is not a double");
        }
        return std::get<double>(data_);
    }

    /**
     * @brief Get the value as a string
     */
    const std::string& GetAsString() const {
        if (type_ != TypeId::VARCHAR && type_ != TypeId::TIMESTAMP) {
            throw std::runtime_error("Value is not a string");
        }
        return std::get<std::string>(data_);
    }

    /**
     * @brief Convert value to string representation
     */
    std::string ToString() const {
        if (IsNull()) return "NULL";
        
        switch (type_) {
            case TypeId::BOOLEAN:
                return GetAsBoolean() ? "true" : "false";
            case TypeId::INTEGER:
                return std::to_string(GetAsInteger());
            case TypeId::BIGINT:
                return std::to_string(GetAsBigInt());
            case TypeId::FLOAT:
                return std::to_string(GetAsFloat());
            case TypeId::DOUBLE:
                return std::to_string(GetAsDouble());
            case TypeId::VARCHAR:
            case TypeId::TIMESTAMP:
                return GetAsString();
            default:
                return "INVALID";
        }
    }

    /**
     * @brief Serialize value to byte buffer
     * @param buffer Destination buffer
     * @return Number of bytes written
     */
    size_t SerializeTo(char* buffer) const {
        switch (type_) {
            case TypeId::BOOLEAN: {
                *buffer = GetAsBoolean() ? 1 : 0;
                return 1;
            }
            case TypeId::INTEGER: {
                int32_t val = GetAsInteger();
                std::memcpy(buffer, &val, sizeof(val));
                return sizeof(val);
            }
            case TypeId::BIGINT:
            case TypeId::TIMESTAMP: {
                int64_t val = GetAsBigInt();
                std::memcpy(buffer, &val, sizeof(val));
                return sizeof(val);
            }
            case TypeId::FLOAT: {
                float val = GetAsFloat();
                std::memcpy(buffer, &val, sizeof(val));
                return sizeof(val);
            }
            case TypeId::DOUBLE: {
                double val = GetAsDouble();
                std::memcpy(buffer, &val, sizeof(val));
                return sizeof(val);
            }
            case TypeId::VARCHAR: {
                const auto& str = GetAsString();
                uint32_t len = static_cast<uint32_t>(str.size());
                std::memcpy(buffer, &len, sizeof(len));
                std::memcpy(buffer + sizeof(len), str.data(), len);
                return sizeof(len) + len;
            }
            default:
                return 0;
        }
    }

    /**
     * @brief Deserialize value from byte buffer
     * @param type The expected type
     * @param buffer Source buffer
     * @param length For VARCHAR, the max length
     * @return The deserialized value
     */
    static Value DeserializeFrom(TypeId type, const char* buffer, uint32_t length = 0) {
        switch (type) {
            case TypeId::BOOLEAN:
                return Value(*buffer != 0);
            case TypeId::INTEGER: {
                int32_t val;
                std::memcpy(&val, buffer, sizeof(val));
                return Value(val);
            }
            case TypeId::BIGINT: {
                int64_t val;
                std::memcpy(&val, buffer, sizeof(val));
                return Value(val);
            }
            case TypeId::FLOAT: {
                float val;
                std::memcpy(&val, buffer, sizeof(val));
                return Value(val);
            }
            case TypeId::DOUBLE: {
                double val;
                std::memcpy(&val, buffer, sizeof(val));
                return Value(val);
            }
            case TypeId::VARCHAR: {
                uint32_t len;
                std::memcpy(&len, buffer, sizeof(len));
                return Value(std::string(buffer + sizeof(len), len));
            }
            default:
                return Value();
        }
    }

    /**
     * @brief Equality comparison
     */
    bool operator==(const Value& other) const {
        if (type_ != other.type_) return false;
        return data_ == other.data_;
    }

    bool operator!=(const Value& other) const {
        return !(*this == other);
    }

    /**
     * @brief Less-than comparison for ordering
     */
    bool operator<(const Value& other) const {
        if (type_ != other.type_) {
            return type_ < other.type_;
        }
        return data_ < other.data_;
    }

private:
    TypeId type_;
    Storage data_;
};

}  // namespace mydb
