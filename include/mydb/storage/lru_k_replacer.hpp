/**
 * @file lru_k_replacer.hpp
 * @brief LRU-K page replacement policy
 * 
 * Implements the LRU-K algorithm as described in:
 * "The LRU-K Page Replacement Algorithm for Database Disk Buffering"
 * by O'Neil, O'Neil, and Weikum.
 * 
 * Key advantages over simple LRU:
 * - Resistant to sequential flooding (large sequential scans)
 * - Better handling of varying access patterns
 * - Distinguishes between frequently and infrequently accessed pages
 * 
 * @see UGent Courses: Algoritmen en Datastructuren, Besturingssystemen
 */

#pragma once

#include <mydb/storage/page.hpp>

#include <list>
#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include <limits>
#include <optional>
#include <chrono>

namespace mydb {

/**
 * @brief LRU-K page replacement algorithm
 * 
 * Tracks the last K accesses for each frame and uses the "backward K-distance"
 * to determine which frame to evict. The backward K-distance is the difference
 * between the current timestamp and the Kth most recent access.
 * 
 * A frame with fewer than K accesses has infinite backward K-distance
 * and is prioritized for eviction (cold start behavior).
 * 
 * Thread Safety: All operations are protected by a mutex.
 * 
 * @tparam K Number of accesses to track (default: 2)
 */
class LRUKReplacer {
public:
    using timestamp_t = uint64_t;

    /**
     * @brief Construct LRU-K replacer
     * @param num_frames Maximum number of frames to track
     * @param k Number of accesses to track (default: 2)
     */
    explicit LRUKReplacer(size_t num_frames, size_t k = 2)
        : num_frames_(num_frames), k_(k), current_timestamp_(0) {}

    /**
     * @brief Record an access to a frame
     * @param frame_id The frame that was accessed
     * 
     * Adds the current timestamp to the access history for this frame.
     * If the history exceeds K entries, the oldest entry is removed.
     */
    void RecordAccess(frame_id_t frame_id) {
        std::lock_guard<std::mutex> lock(latch_);
        
        current_timestamp_++;
        
        auto& history = access_history_[frame_id];
        history.push_back(current_timestamp_);
        
        // Keep only the last K accesses
        while (history.size() > k_) {
            history.pop_front();
        }
    }

    /**
     * @brief Set whether a frame is evictable
     * @param frame_id The frame to modify
     * @param evictable True if the frame can be evicted
     * 
     * A frame is evictable when it has no pins (pin_count == 0).
     */
    void SetEvictable(frame_id_t frame_id, bool evictable) {
        std::lock_guard<std::mutex> lock(latch_);
        
        if (evictable) {
            evictable_frames_.insert(frame_id);
        } else {
            evictable_frames_.erase(frame_id);
        }
    }

    /**
     * @brief Evict a frame using LRU-K policy
     * @param[out] frame_id The frame that was evicted
     * @return True if a frame was evicted, false if no evictable frames
     * 
     * Selects the frame with the largest backward K-distance.
     * Frames with fewer than K accesses have infinite distance.
     * Among frames with infinite distance, uses FIFO based on first access.
     */
    bool Evict(frame_id_t* frame_id) {
        std::lock_guard<std::mutex> lock(latch_);
        
        if (evictable_frames_.empty()) {
            return false;
        }
        
        frame_id_t victim = INVALID_FRAME_ID;
        timestamp_t max_k_distance = 0;
        timestamp_t earliest_first_access = std::numeric_limits<timestamp_t>::max();
        bool found_inf = false;
        
        for (frame_id_t fid : evictable_frames_) {
            auto it = access_history_.find(fid);
            
            if (it == access_history_.end() || it->second.empty()) {
                // No access history - highest priority for eviction
                if (!found_inf || fid < victim) {
                    victim = fid;
                    found_inf = true;
                }
                continue;
            }
            
            const auto& history = it->second;
            
            if (history.size() < k_) {
                // Fewer than K accesses - infinite backward K-distance
                // Use FIFO among these (earliest first access wins)
                timestamp_t first_access = history.front();
                if (!found_inf || first_access < earliest_first_access) {
                    victim = fid;
                    earliest_first_access = first_access;
                    found_inf = true;
                }
            } else if (!found_inf) {
                // Exactly K or more accesses - use backward K-distance
                // K-distance = current_timestamp - Kth_oldest_access
                timestamp_t kth_access = history.front();  // Oldest of last K
                timestamp_t k_distance = current_timestamp_ - kth_access;
                
                if (k_distance > max_k_distance) {
                    max_k_distance = k_distance;
                    victim = fid;
                }
            }
        }
        
        if (victim == INVALID_FRAME_ID) {
            return false;
        }
        
        // Remove victim from tracking
        evictable_frames_.erase(victim);
        access_history_.erase(victim);
        
        *frame_id = victim;
        return true;
    }

    /**
     * @brief Remove a frame from the replacer entirely
     * @param frame_id The frame to remove
     * 
     * Called when a page is deleted or explicitly unpinned.
     */
    void Remove(frame_id_t frame_id) {
        std::lock_guard<std::mutex> lock(latch_);
        
        evictable_frames_.erase(frame_id);
        access_history_.erase(frame_id);
    }

    /**
     * @brief Get current number of evictable frames
     */
    size_t Size() const {
        std::lock_guard<std::mutex> lock(latch_);
        return evictable_frames_.size();
    }

    /**
     * @brief Get the K value
     */
    size_t GetK() const { return k_; }

private:
    size_t num_frames_;
    size_t k_;
    timestamp_t current_timestamp_;
    
    // Maps frame_id -> list of last K access timestamps
    std::unordered_map<frame_id_t, std::list<timestamp_t>> access_history_;
    
    // Set of frames that can be evicted (pin_count == 0)
    std::unordered_set<frame_id_t> evictable_frames_;
    
    mutable std::mutex latch_;
};

}  // namespace mydb
