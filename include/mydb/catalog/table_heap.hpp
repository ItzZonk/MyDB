/**
 * @file table_heap.hpp
 * @brief Table storage as a heap of pages
 * 
 * Implements a heap file organization for storing table data.
 * Tuples are stored in pages without any particular order.
 * 
 * @see UGent Courses: Databases
 */

#pragma once

#include <mydb/catalog/tuple.hpp>
#include <mydb/catalog/schema.hpp>
#include <mydb/storage/buffer_pool.hpp>

#include <memory>

namespace mydb {

/**
 * @brief Iterator for scanning table tuples
 */
class TableIterator {
public:
    TableIterator() = default;
    
    TableIterator(BufferPoolManager* bpm, const Schema* schema,
                  page_id_t page_id, uint32_t slot_num)
        : bpm_(bpm), schema_(schema), rid_(page_id, slot_num) {
        if (rid_.page_id != INVALID_PAGE_ID) {
            LoadCurrentTuple();
        }
    }

    /**
     * @brief Dereference to get current tuple
     */
    const Tuple& operator*() const { return current_tuple_; }
    const Tuple* operator->() const { return &current_tuple_; }

    /**
     * @brief Get current RID
     */
    const RID& GetRID() const { return rid_; }

    /**
     * @brief Move to next tuple
     */
    TableIterator& operator++() {
        if (rid_.page_id == INVALID_PAGE_ID) {
            return *this;
        }
        
        // Try next slot in current page
        rid_.slot_num++;
        
        Page* page = bpm_->FetchPage(rid_.page_id);
        if (page == nullptr) {
            rid_.page_id = INVALID_PAGE_ID;
            return *this;
        }
        
        // Check if slot is valid
        if (rid_.slot_num >= page->GetTupleCount()) {
            // Move to next page
            // For now, assume contiguous page IDs (simplified)
            bpm_->UnpinPage(rid_.page_id, false);
            
            // Try next page
            page_id_t next_page = rid_.page_id + 1;
            Page* next = bpm_->FetchPage(next_page);
            if (next == nullptr || next->GetTupleCount() == 0) {
                if (next != nullptr) {
                    bpm_->UnpinPage(next_page, false);
                }
                rid_.page_id = INVALID_PAGE_ID;
                return *this;
            }
            
            rid_.page_id = next_page;
            rid_.slot_num = 0;
            bpm_->UnpinPage(next_page, false);
        } else {
            bpm_->UnpinPage(rid_.page_id, false);
        }
        
        LoadCurrentTuple();
        return *this;
    }

    /**
     * @brief Check if at end
     */
    bool IsEnd() const {
        return rid_.page_id == INVALID_PAGE_ID;
    }

    /**
     * @brief Equality comparison
     */
    bool operator==(const TableIterator& other) const {
        return rid_ == other.rid_;
    }

    bool operator!=(const TableIterator& other) const {
        return !(*this == other);
    }

private:
    void LoadCurrentTuple() {
        if (rid_.page_id == INVALID_PAGE_ID) return;
        
        Page* page = bpm_->FetchPage(rid_.page_id);
        if (page == nullptr) {
            rid_.page_id = INVALID_PAGE_ID;
            return;
        }
        
        size_t size;
        const char* data = page->GetTuple(rid_.slot_num, &size);
        if (data == nullptr) {
            bpm_->UnpinPage(rid_.page_id, false);
            rid_.page_id = INVALID_PAGE_ID;
            return;
        }
        
        current_tuple_ = Tuple::DeserializeFrom(data, schema_);
        current_tuple_.SetRID(rid_);
        bpm_->UnpinPage(rid_.page_id, false);
    }

    BufferPoolManager* bpm_{nullptr};
    const Schema* schema_{nullptr};
    RID rid_;
    Tuple current_tuple_;
};

/**
 * @brief Heap-organized table storage
 * 
 * A TableHeap stores tuples in a collection of pages.
 * Tuples are stored wherever there is space (heap organization).
 * 
 * Operations:
 * - InsertTuple: Add a new tuple to the table
 * - DeleteTuple: Mark a tuple as deleted
 * - UpdateTuple: Replace a tuple's data
 * - Scan: Iterate over all tuples
 */
class TableHeap {
public:
    /**
     * @brief Construct table heap
     * @param bpm Buffer pool manager
     * @param schema Table schema
     * @param first_page_id First page of the table
     */
    TableHeap(BufferPoolManager* bpm, const Schema* schema, page_id_t first_page_id)
        : bpm_(bpm), schema_(schema), first_page_id_(first_page_id) {}

    /**
     * @brief Insert a tuple into the table
     * @param tuple The tuple to insert
     * @param[out] rid The RID of the inserted tuple
     * @return True if successful
     */
    bool InsertTuple(const Tuple& tuple, RID* rid) {
        // Serialize tuple
        std::vector<char> buffer(tuple.GetSerializedSize(schema_) + sizeof(TupleHeader));
        tuple.SerializeTo(buffer.data(), schema_);
        
        // Find a page with space
        page_id_t current_page_id = first_page_id_;
        
        while (current_page_id != INVALID_PAGE_ID) {
            Page* page = bpm_->FetchPage(current_page_id);
            if (page == nullptr) break;
            
            // Try to insert
            int slot = page->InsertTuple(buffer.data(), buffer.size());
            if (slot >= 0) {
                *rid = RID(current_page_id, static_cast<uint32_t>(slot));
                bpm_->UnpinPage(current_page_id, true);
                return true;
            }
            
            bpm_->UnpinPage(current_page_id, false);
            current_page_id++;  // Try next page (simplified)
        }
        
        // Need a new page
        page_id_t new_page_id;
        Page* new_page = bpm_->NewPage(&new_page_id);
        if (new_page == nullptr) {
            return false;
        }
        
        int slot = new_page->InsertTuple(buffer.data(), buffer.size());
        if (slot < 0) {
            bpm_->UnpinPage(new_page_id, false);
            return false;
        }
        
        *rid = RID(new_page_id, static_cast<uint32_t>(slot));
        bpm_->UnpinPage(new_page_id, true);
        return true;
    }

    /**
     * @brief Delete a tuple by RID
     * @param rid The RID of the tuple to delete
     * @return True if successful
     */
    bool DeleteTuple(const RID& rid) {
        Page* page = bpm_->FetchPage(rid.page_id);
        if (page == nullptr) {
            return false;
        }
        
        bool success = page->DeleteTuple(rid.slot_num);
        bpm_->UnpinPage(rid.page_id, success);
        return success;
    }

    /**
     * @brief Get a tuple by RID
     * @param rid The RID of the tuple
     * @param[out] tuple The retrieved tuple
     * @return True if found
     */
    bool GetTuple(const RID& rid, Tuple* tuple) {
        Page* page = bpm_->FetchPage(rid.page_id);
        if (page == nullptr) {
            return false;
        }
        
        size_t size;
        const char* data = page->GetTuple(rid.slot_num, &size);
        if (data == nullptr) {
            bpm_->UnpinPage(rid.page_id, false);
            return false;
        }
        
        *tuple = Tuple::DeserializeFrom(data, schema_);
        tuple->SetRID(rid);
        bpm_->UnpinPage(rid.page_id, false);
        return true;
    }

    /**
     * @brief Get iterator to first tuple
     */
    TableIterator Begin() {
        return TableIterator(bpm_, schema_, first_page_id_, 0);
    }

    /**
     * @brief Get end iterator
     */
    TableIterator End() {
        return TableIterator(bpm_, schema_, INVALID_PAGE_ID, 0);
    }

    /**
     * @brief Get the schema
     */
    const Schema* GetSchema() const { return schema_; }

    /**
     * @brief Get first page ID
     */
    page_id_t GetFirstPageId() const { return first_page_id_; }

private:
    BufferPoolManager* bpm_;
    const Schema* schema_;
    page_id_t first_page_id_;
};

}  // namespace mydb
