/**
 * @file disk_manager.hpp
 * @brief Page-based disk I/O manager
 * 
 * Handles reading and writing fixed-size pages to the database file.
 * This is the lowest layer of the storage hierarchy, directly interfacing
 * with the file system.
 * 
 * Key responsibilities:
 * - Page allocation and deallocation
 * - Binary read/write of page-sized blocks
 * - File offset calculation from page IDs
 * 
 * @see UGent Courses: Besturingssystemen (Operating Systems)
 */

#pragma once

#include <mydb/storage/page.hpp>

#include <fstream>
#include <string>
#include <mutex>
#include <atomic>
#include <vector>
#include <filesystem>

namespace mydb {

/**
 * @brief Manages page-level I/O to the database file
 * 
 * The DiskManager provides a simple abstraction over the file system,
 * treating the database as an array of fixed-size pages. All I/O is
 * performed in PAGE_SIZE chunks for efficiency and alignment with
 * the buffer pool.
 * 
 * Thread Safety: All operations are protected by a mutex.
 */
class DiskManager {
public:
    /**
     * @brief Construct disk manager for a database file
     * @param db_file Path to the database file
     */
    explicit DiskManager(const std::string& db_file) 
        : db_file_path_(db_file) {
        
        // Create parent directories if needed
        auto parent = std::filesystem::path(db_file).parent_path();
        if (!parent.empty() && !std::filesystem::exists(parent)) {
            std::filesystem::create_directories(parent);
        }
        
        // Open file for read/write, create if doesn't exist
        db_io_.open(db_file, 
                    std::ios::binary | std::ios::in | std::ios::out);
        
        if (!db_io_.is_open()) {
            // File doesn't exist, create it
            db_io_.clear();
            db_io_.open(db_file, 
                        std::ios::binary | std::ios::trunc | std::ios::out);
            db_io_.close();
            db_io_.open(db_file, 
                        std::ios::binary | std::ios::in | std::ios::out);
        }
        
        if (!db_io_.is_open()) {
            throw std::runtime_error("Failed to open database file: " + db_file);
        }
        
        // Determine number of existing pages
        db_io_.seekg(0, std::ios::end);
        size_t file_size = db_io_.tellg();
        next_page_id_ = static_cast<page_id_t>(file_size / PAGE_SIZE);
    }

    ~DiskManager() {
        ShutDown();
    }

    // Non-copyable
    DiskManager(const DiskManager&) = delete;
    DiskManager& operator=(const DiskManager&) = delete;

    /**
     * @brief Close the database file
     */
    void ShutDown() {
        std::lock_guard<std::mutex> lock(latch_);
        if (db_io_.is_open()) {
            db_io_.flush();
            db_io_.close();
        }
    }

    /**
     * @brief Read a page from disk into memory
     * @param page_id The page to read
     * @param page_data Buffer to read into (must be PAGE_SIZE bytes)
     * 
     * Seeks to the correct file offset and reads PAGE_SIZE bytes.
     */
    void ReadPage(page_id_t page_id, char* page_data) {
        std::lock_guard<std::mutex> lock(latch_);
        
        if (page_id < 0) {
            throw std::invalid_argument("Invalid page ID: " + std::to_string(page_id));
        }
        
        size_t offset = static_cast<size_t>(page_id) * PAGE_SIZE;
        
        db_io_.seekg(offset);
        if (!db_io_.good()) {
            // Page doesn't exist yet, zero-fill
            std::memset(page_data, 0, PAGE_SIZE);
            return;
        }
        
        db_io_.read(page_data, PAGE_SIZE);
        
        // Handle partial read (end of file)
        if (db_io_.gcount() < static_cast<std::streamsize>(PAGE_SIZE)) {
            std::memset(page_data + db_io_.gcount(), 0, 
                        PAGE_SIZE - db_io_.gcount());
        }
        
        db_io_.clear();  // Clear any EOF flags
    }

    /**
     * @brief Write a page from memory to disk
     * @param page_id The page to write
     * @param page_data Buffer to write from (must be PAGE_SIZE bytes)
     * 
     * Seeks to the correct file offset and writes PAGE_SIZE bytes.
     * Automatically extends the file if necessary.
     */
    void WritePage(page_id_t page_id, const char* page_data) {
        std::lock_guard<std::mutex> lock(latch_);
        
        if (page_id < 0) {
            throw std::invalid_argument("Invalid page ID: " + std::to_string(page_id));
        }
        
        size_t offset = static_cast<size_t>(page_id) * PAGE_SIZE;
        
        db_io_.seekp(offset);
        db_io_.write(page_data, PAGE_SIZE);
        
        if (!db_io_.good()) {
            throw std::runtime_error("Failed to write page " + std::to_string(page_id));
        }
        
        db_io_.flush();
    }

    /**
     * @brief Allocate a new page
     * @return The ID of the newly allocated page
     * 
     * Uses atomic increment to generate unique page IDs.
     * The actual file space is allocated on first write.
     */
    page_id_t AllocatePage() {
        return next_page_id_.fetch_add(1);
    }

    /**
     * @brief Deallocate a page (mark as free)
     * @param page_id The page to deallocate
     * 
     * Note: This implementation does not reclaim space immediately.
     * A more sophisticated version would maintain a free list.
     */
    void DeallocatePage(page_id_t page_id) {
        std::lock_guard<std::mutex> lock(latch_);
        free_pages_.push_back(page_id);
    }

    /**
     * @brief Get number of pages in the database file
     */
    page_id_t GetNumPages() const {
        return next_page_id_.load();
    }

    /**
     * @brief Get the database file path
     */
    const std::string& GetFilePath() const {
        return db_file_path_;
    }

    /**
     * @brief Flush all pending writes to disk
     */
    void Flush() {
        std::lock_guard<std::mutex> lock(latch_);
        if (db_io_.is_open()) {
            db_io_.flush();
        }
    }

private:
    std::string db_file_path_;
    std::fstream db_io_;
    std::atomic<page_id_t> next_page_id_{0};
    std::vector<page_id_t> free_pages_;  // Simple free list
    std::mutex latch_;
};

}  // namespace mydb
