/**
 * @file buffer_pool.hpp
 * @brief Buffer Pool Manager for page caching
 * 
 * The Buffer Pool Manager is the heart of memory management in a DBMS.
 * It caches pages from disk in memory to minimize I/O operations.
 * 
 * Key responsibilities:
 * - Fetch pages from disk into memory frames
 * - Track page usage via pinning/unpinning
 * - Write dirty pages back to disk
 * - Evict pages when buffer is full (using LRU-K)
 * 
 * This component demonstrates understanding of:
 * - Memory hierarchy and caching
 * - Page replacement algorithms
 * - Thread-safe resource management
 * 
 * @see UGent Courses: Besturingssystemen, Parallel Computer Systems
 */

#pragma once

#include <mydb/storage/page.hpp>
#include <mydb/storage/disk_manager.hpp>
#include <mydb/storage/lru_k_replacer.hpp>

#include <memory>
#include <unordered_map>
#include <list>
#include <mutex>
#include <condition_variable>

namespace mydb {

/**
 * @brief Configuration for the buffer pool
 */
struct BufferPoolConfig {
    size_t pool_size{1024};     ///< Number of pages in the pool
    size_t replacer_k{2};       ///< K value for LRU-K replacer
    std::string db_file;        ///< Path to database file
};

/**
 * @brief Manages a pool of in-memory page frames
 * 
 * The BufferPoolManager sits between the execution engine and disk:
 * 
 * ```
 * Execution Engine
 *        |
 *        v
 * Buffer Pool Manager  <-- You are here
 *        |
 *        v
 *   Disk Manager
 *        |
 *        v
 *    Disk (SSD/HDD)
 * ```
 * 
 * Pages in the pool are either:
 * - Pinned: Being used by a query, cannot be evicted
 * - Unpinned: Available for eviction if needed
 * 
 * Thread Safety: All operations are thread-safe.
 */
class BufferPoolManager {
public:
    /**
     * @brief Construct buffer pool with specified configuration
     * @param config Buffer pool configuration
     */
    explicit BufferPoolManager(const BufferPoolConfig& config)
        : pool_size_(config.pool_size),
          disk_manager_(std::make_unique<DiskManager>(config.db_file)),
          replacer_(std::make_unique<LRUKReplacer>(config.pool_size, config.replacer_k)) {
        
        // Allocate page frames
        pages_.resize(pool_size_);
        
        // Initialize free list with all frames
        for (frame_id_t i = 0; i < static_cast<frame_id_t>(pool_size_); ++i) {
            free_list_.push_back(i);
        }
    }

    /**
     * @brief Construct buffer pool with an existing disk manager
     * @param pool_size Number of frames in the pool
     * @param disk_manager Existing disk manager to use
     */
    BufferPoolManager(size_t pool_size, std::unique_ptr<DiskManager> disk_manager)
        : pool_size_(pool_size),
          disk_manager_(std::move(disk_manager)),
          replacer_(std::make_unique<LRUKReplacer>(pool_size, 2)) {
        
        pages_.resize(pool_size_);
        for (frame_id_t i = 0; i < static_cast<frame_id_t>(pool_size_); ++i) {
            free_list_.push_back(i);
        }
    }

    ~BufferPoolManager() {
        // Flush all dirty pages before destruction
        FlushAllPages();
    }

    // Non-copyable
    BufferPoolManager(const BufferPoolManager&) = delete;
    BufferPoolManager& operator=(const BufferPoolManager&) = delete;

    /**
     * @brief Fetch a page from the buffer pool
     * @param page_id The page to fetch
     * @return Pointer to the page, or nullptr if failed
     * 
     * If the page is already in the pool, returns it and increments pin count.
     * If not in pool, loads from disk into a free frame (evicting if necessary).
     * 
     * The returned page has pin_count >= 1 and must be unpinned when done.
     */
    Page* FetchPage(page_id_t page_id) {
        std::lock_guard<std::mutex> lock(latch_);
        
        if (page_id == INVALID_PAGE_ID) {
            return nullptr;
        }
        
        // Check if page is already in buffer pool
        auto it = page_table_.find(page_id);
        if (it != page_table_.end()) {
            frame_id_t frame_id = it->second;
            Page& page = pages_[frame_id];
            page.IncrementPinCount();
            replacer_->SetEvictable(frame_id, false);
            replacer_->RecordAccess(frame_id);
            return &page;
        }
        
        // Need to load from disk - find a frame
        frame_id_t frame_id = INVALID_FRAME_ID;
        
        // Try free list first
        if (!free_list_.empty()) {
            frame_id = free_list_.front();
            free_list_.pop_front();
        } else {
            // Must evict a page
            if (!replacer_->Evict(&frame_id)) {
                // No evictable pages - buffer pool is full
                return nullptr;
            }
            
            // Write back if dirty
            Page& old_page = pages_[frame_id];
            if (old_page.IsDirty()) {
                disk_manager_->WritePage(old_page.GetPageId(), old_page.GetData());
            }
            
            // Remove old page from table
            page_table_.erase(old_page.GetPageId());
        }
        
        // Load new page into frame
        Page& page = pages_[frame_id];
        page.ResetMemory();
        page.SetPageId(page_id);
        disk_manager_->ReadPage(page_id, page.GetData());
        
        // Update tracking structures
        page_table_[page_id] = frame_id;
        page.IncrementPinCount();
        replacer_->RecordAccess(frame_id);
        replacer_->SetEvictable(frame_id, false);
        
        return &page;
    }

    /**
     * @brief Unpin a page in the buffer pool
     * @param page_id The page to unpin
     * @param is_dirty Whether the page was modified
     * @return True if the page was found and unpinned
     * 
     * Decrements pin count. When pin count reaches 0, the page becomes
     * eligible for eviction by the replacer.
     */
    bool UnpinPage(page_id_t page_id, bool is_dirty) {
        std::lock_guard<std::mutex> lock(latch_);
        
        auto it = page_table_.find(page_id);
        if (it == page_table_.end()) {
            return false;
        }
        
        frame_id_t frame_id = it->second;
        Page& page = pages_[frame_id];
        
        if (page.GetPinCount() <= 0) {
            return false;  // Already unpinned
        }
        
        page.DecrementPinCount();
        if (is_dirty) {
            page.SetDirty(true);
        }
        
        if (page.GetPinCount() == 0) {
            replacer_->SetEvictable(frame_id, true);
        }
        
        return true;
    }

    /**
     * @brief Allocate a new page in the buffer pool
     * @param[out] page_id Will contain the new page's ID
     * @return Pointer to the new page, or nullptr if failed
     * 
     * Allocates a new page ID from the disk manager and pins it.
     */
    Page* NewPage(page_id_t* page_id) {
        std::lock_guard<std::mutex> lock(latch_);
        
        // Find a frame for the new page
        frame_id_t frame_id = INVALID_FRAME_ID;
        
        if (!free_list_.empty()) {
            frame_id = free_list_.front();
            free_list_.pop_front();
        } else {
            if (!replacer_->Evict(&frame_id)) {
                return nullptr;
            }
            
            // Write back if dirty
            Page& old_page = pages_[frame_id];
            if (old_page.IsDirty()) {
                disk_manager_->WritePage(old_page.GetPageId(), old_page.GetData());
            }
            page_table_.erase(old_page.GetPageId());
        }
        
        // Allocate new page ID
        page_id_t new_page_id = disk_manager_->AllocatePage();
        
        // Initialize the page
        Page& page = pages_[frame_id];
        page.ResetMemory();
        page.SetPageId(new_page_id);
        page.IncrementPinCount();
        page.SetDirty(true);  // New pages are dirty by default
        
        // Update tracking
        page_table_[new_page_id] = frame_id;
        replacer_->RecordAccess(frame_id);
        replacer_->SetEvictable(frame_id, false);
        
        *page_id = new_page_id;
        return &page;
    }

    /**
     * @brief Flush a page to disk
     * @param page_id The page to flush
     * @return True if the page was flushed
     */
    bool FlushPage(page_id_t page_id) {
        std::lock_guard<std::mutex> lock(latch_);
        
        auto it = page_table_.find(page_id);
        if (it == page_table_.end()) {
            return false;
        }
        
        frame_id_t frame_id = it->second;
        Page& page = pages_[frame_id];
        
        disk_manager_->WritePage(page_id, page.GetData());
        page.SetDirty(false);
        
        return true;
    }

    /**
     * @brief Flush all pages to disk
     */
    void FlushAllPages() {
        std::lock_guard<std::mutex> lock(latch_);
        
        for (auto& [page_id, frame_id] : page_table_) {
            Page& page = pages_[frame_id];
            if (page.IsDirty()) {
                disk_manager_->WritePage(page_id, page.GetData());
                page.SetDirty(false);
            }
        }
    }

    /**
     * @brief Delete a page from the buffer pool
     * @param page_id The page to delete
     * @return True if the page was deleted
     * 
     * The page must have pin_count == 0 to be deleted.
     */
    bool DeletePage(page_id_t page_id) {
        std::lock_guard<std::mutex> lock(latch_);
        
        auto it = page_table_.find(page_id);
        if (it == page_table_.end()) {
            // Not in pool - just deallocate from disk manager
            disk_manager_->DeallocatePage(page_id);
            return true;
        }
        
        frame_id_t frame_id = it->second;
        Page& page = pages_[frame_id];
        
        if (page.GetPinCount() > 0) {
            return false;  // Cannot delete pinned page
        }
        
        // Remove from tracking
        page_table_.erase(it);
        replacer_->Remove(frame_id);
        
        // Reset page and add frame to free list
        page.ResetMemory();
        free_list_.push_back(frame_id);
        
        disk_manager_->DeallocatePage(page_id);
        return true;
    }

    /**
     * @brief Get the size of the buffer pool
     */
    size_t GetPoolSize() const { return pool_size_; }

    /**
     * @brief Get the disk manager
     */
    DiskManager* GetDiskManager() { return disk_manager_.get(); }

private:
    size_t pool_size_;
    std::vector<Page> pages_;                        ///< The actual page frames
    std::unordered_map<page_id_t, frame_id_t> page_table_;  ///< Maps page_id -> frame_id
    std::list<frame_id_t> free_list_;                ///< Free frames available
    std::unique_ptr<DiskManager> disk_manager_;
    std::unique_ptr<LRUKReplacer> replacer_;
    std::mutex latch_;
};

}  // namespace mydb
