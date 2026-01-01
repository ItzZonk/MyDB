/**
 * @file catalog.hpp
 * @brief Database catalog for table and index metadata
 * 
 * The catalog is the metadata store for the database, tracking:
 * - Table definitions (schemas)
 * - Index definitions
 * - System tables
 * 
 * @see UGent Courses: Databases
 */

#pragma once

#include <mydb/catalog/schema.hpp>
#include <mydb/catalog/tuple.hpp>
#include <mydb/storage/buffer_pool.hpp>

#include <string>
#include <memory>
#include <unordered_map>
#include <mutex>

namespace mydb {

// Forward declarations
class TableHeap;
class BPlusTreeIndex;

/**
 * @brief Metadata about a table
 */
struct TableInfo {
    std::string name;
    Schema schema;
    page_id_t first_page_id{INVALID_PAGE_ID};
    uint64_t tuple_count{0};
    
    TableInfo(std::string n, Schema s) 
        : name(std::move(n)), schema(std::move(s)) {}
};

/**
 * @brief Metadata about an index
 */
struct IndexInfo {
    std::string name;
    std::string table_name;
    std::vector<uint32_t> key_columns;  ///< Column indices that form the key
    page_id_t root_page_id{INVALID_PAGE_ID};
    
    IndexInfo(std::string n, std::string tn, std::vector<uint32_t> cols)
        : name(std::move(n)), table_name(std::move(tn)), key_columns(std::move(cols)) {}
};

/**
 * @brief Database catalog managing table and index metadata
 * 
 * The catalog provides a central registry for all database objects.
 * It supports creating, looking up, and dropping tables and indexes.
 * 
 * Thread Safety: All operations are protected by a mutex.
 */
class Catalog {
public:
    /**
     * @brief Construct catalog with buffer pool
     * @param bpm Buffer pool manager for storage
     */
    explicit Catalog(BufferPoolManager* bpm) : bpm_(bpm) {}

    /**
     * @brief Create a new table
     * @param table_name Name of the table
     * @param schema Table schema
     * @return Pointer to table info, or nullptr if table exists
     */
    TableInfo* CreateTable(const std::string& table_name, const Schema& schema) {
        std::lock_guard<std::mutex> lock(latch_);
        
        if (tables_.find(table_name) != tables_.end()) {
            return nullptr;  // Table already exists
        }
        
        auto info = std::make_unique<TableInfo>(table_name, schema);
        
        // Allocate first page for the table
        page_id_t first_page;
        Page* page = bpm_->NewPage(&first_page);
        if (page == nullptr) {
            return nullptr;
        }
        info->first_page_id = first_page;
        bpm_->UnpinPage(first_page, true);
        
        TableInfo* result = info.get();
        tables_[table_name] = std::move(info);
        table_names_.push_back(table_name);
        
        return result;
    }

    /**
     * @brief Get table info by name
     * @param table_name Name of the table
     * @return Pointer to table info, or nullptr if not found
     */
    TableInfo* GetTable(const std::string& table_name) {
        std::lock_guard<std::mutex> lock(latch_);
        
        auto it = tables_.find(table_name);
        if (it == tables_.end()) {
            return nullptr;
        }
        return it->second.get();
    }

    /**
     * @brief Check if a table exists
     */
    bool TableExists(const std::string& table_name) {
        std::lock_guard<std::mutex> lock(latch_);
        return tables_.find(table_name) != tables_.end();
    }

    /**
     * @brief Drop a table
     * @param table_name Name of the table to drop
     * @return True if table was dropped
     */
    bool DropTable(const std::string& table_name) {
        std::lock_guard<std::mutex> lock(latch_);
        
        auto it = tables_.find(table_name);
        if (it == tables_.end()) {
            return false;
        }
        
        // Remove all indexes on this table
        auto idx_it = indexes_.begin();
        while (idx_it != indexes_.end()) {
            if (idx_it->second->table_name == table_name) {
                idx_it = indexes_.erase(idx_it);
            } else {
                ++idx_it;
            }
        }
        
        tables_.erase(it);
        table_names_.erase(
            std::remove(table_names_.begin(), table_names_.end(), table_name),
            table_names_.end()
        );
        
        return true;
    }

    /**
     * @brief Get all table names
     */
    const std::vector<std::string>& GetTableNames() const {
        return table_names_;
    }

    /**
     * @brief Create an index on a table
     * @param index_name Name of the index
     * @param table_name Name of the table
     * @param key_columns Column indices to index
     * @return Pointer to index info, or nullptr if failed
     */
    IndexInfo* CreateIndex(const std::string& index_name, 
                           const std::string& table_name,
                           const std::vector<uint32_t>& key_columns) {
        std::lock_guard<std::mutex> lock(latch_);
        
        // Check if table exists
        if (tables_.find(table_name) == tables_.end()) {
            return nullptr;
        }
        
        // Check if index already exists
        if (indexes_.find(index_name) != indexes_.end()) {
            return nullptr;
        }
        
        auto info = std::make_unique<IndexInfo>(index_name, table_name, key_columns);
        
        // Allocate root page for B+ tree
        page_id_t root_page;
        Page* page = bpm_->NewPage(&root_page);
        if (page == nullptr) {
            return nullptr;
        }
        info->root_page_id = root_page;
        bpm_->UnpinPage(root_page, true);
        
        IndexInfo* result = info.get();
        indexes_[index_name] = std::move(info);
        
        return result;
    }

    /**
     * @brief Get index info by name
     */
    IndexInfo* GetIndex(const std::string& index_name) {
        std::lock_guard<std::mutex> lock(latch_);
        
        auto it = indexes_.find(index_name);
        if (it == indexes_.end()) {
            return nullptr;
        }
        return it->second.get();
    }

    /**
     * @brief Get all indexes for a table
     */
    std::vector<IndexInfo*> GetTableIndexes(const std::string& table_name) {
        std::lock_guard<std::mutex> lock(latch_);
        
        std::vector<IndexInfo*> result;
        for (auto& [name, info] : indexes_) {
            if (info->table_name == table_name) {
                result.push_back(info.get());
            }
        }
        return result;
    }

    /**
     * @brief Drop an index
     */
    bool DropIndex(const std::string& index_name) {
        std::lock_guard<std::mutex> lock(latch_);
        return indexes_.erase(index_name) > 0;
    }

private:
    BufferPoolManager* bpm_;
    std::unordered_map<std::string, std::unique_ptr<TableInfo>> tables_;
    std::unordered_map<std::string, std::unique_ptr<IndexInfo>> indexes_;
    std::vector<std::string> table_names_;  // Ordered list for iteration
    std::mutex latch_;
};

}  // namespace mydb
