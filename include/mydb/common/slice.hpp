#pragma once

#include <cstring>
#include <string>
#include <string_view>
#include <cassert>
#include <functional>

namespace mydb {

// Non-owning view of binary data (like string_view but for binary)
class Slice {
public:
    Slice() : data_(nullptr), size_(0) {}
    Slice(const char* data, size_t size) : data_(data), size_(size) {}
    Slice(const char* data) : data_(data), size_(data ? std::strlen(data) : 0) {}
    Slice(const std::string& s) : data_(s.data()), size_(s.size()) {}
    Slice(std::string_view sv) : data_(sv.data()), size_(sv.size()) {}
    Slice(const Slice&) = default;
    Slice& operator=(const Slice&) = default;
    
    [[nodiscard]] const char* data() const { return data_; }
    [[nodiscard]] size_t size() const { return size_; }
    [[nodiscard]] bool empty() const { return size_ == 0; }
    [[nodiscard]] char operator[](size_t i) const { return data_[i]; }
    
    [[nodiscard]] std::string ToString() const { return std::string(data_, size_); }
    [[nodiscard]] std::string_view ToStringView() const { return {data_, size_}; }
    
    [[nodiscard]] int Compare(const Slice& o) const {
        size_t min_len = std::min(size_, o.size_);
        int r = std::memcmp(data_, o.data_, min_len);
        if (r == 0) r = (size_ < o.size_) ? -1 : (size_ > o.size_) ? 1 : 0;
        return r;
    }
    
    [[nodiscard]] bool operator==(const Slice& o) const { return size_ == o.size_ && (size_ == 0 || std::memcmp(data_, o.data_, size_) == 0); }
    [[nodiscard]] bool operator!=(const Slice& o) const { return !(*this == o); }
    [[nodiscard]] bool operator<(const Slice& o) const { return Compare(o) < 0; }
    [[nodiscard]] bool operator<=(const Slice& o) const { return Compare(o) <= 0; }
    [[nodiscard]] bool operator>(const Slice& o) const { return Compare(o) > 0; }
    [[nodiscard]] bool operator>=(const Slice& o) const { return Compare(o) >= 0; }
    
    [[nodiscard]] bool StartsWith(const Slice& p) const { return size_ >= p.size_ && std::memcmp(data_, p.data_, p.size_) == 0; }
    [[nodiscard]] bool EndsWith(const Slice& s) const { return size_ >= s.size_ && std::memcmp(data_ + size_ - s.size_, s.data_, s.size_) == 0; }
    
    void RemovePrefix(size_t n) { data_ += n; size_ -= n; }
    void Truncate(size_t n) { size_ = n; }
    
    [[nodiscard]] const char* begin() const { return data_; }
    [[nodiscard]] const char* end() const { return data_ + size_; }
    
private:
    const char* data_;
    size_t size_;
};

struct SliceHash {
    size_t operator()(const Slice& s) const {
        size_t h = 14695981039346656037ULL;
        for (size_t i = 0; i < s.size(); ++i) { h ^= static_cast<unsigned char>(s[i]); h *= 1099511628211ULL; }
        return h;
    }
};

} // namespace mydb

namespace std {
    template<> struct hash<mydb::Slice> {
        size_t operator()(const mydb::Slice& s) const { return mydb::SliceHash{}(s); }
    };
}
