/**
 * @file seq_scan_executor.hpp
 * @brief Sequential scan executor
 * 
 * Scans all tuples in a table sequentially.
 * This is the fundamental access method for tables without indexes.
 * 
 * @see UGent Courses: Databases
 */

#pragma once

#include <mydb/execution/executor.hpp>
#include <mydb/catalog/table_heap.hpp>

namespace mydb {

/**
 * @brief Sequential scan over a table
 * 
 * Iterates through all tuples in a table from the first page
 * to the last, returning each tuple that passes any filter.
 * 
 * Complexity: O(n) where n is the number of tuples
 */
class SeqScanExecutor : public AbstractExecutor {
public:
    /**
     * @brief Construct sequential scan executor
     * @param ctx Execution context
     * @param table_name Name of the table to scan
     */
    SeqScanExecutor(ExecutorContext* ctx, const std::string& table_name)
        : AbstractExecutor(ctx), table_name_(table_name) {}

    /**
     * @brief Initialize the scan
     * 
     * Looks up the table in the catalog and positions the iterator
     * at the first tuple.
     */
    void Init() override {
        // Get table info from catalog
        TableInfo* table_info = ctx_->GetCatalog()->GetTable(table_name_);
        if (table_info == nullptr) {
            throw std::runtime_error("Table not found: " + table_name_);
        }
        
        schema_ = &table_info->schema;
        
        // Create table heap for iteration
        table_heap_ = std::make_unique<TableHeap>(
            ctx_->GetBufferPoolManager(),
            schema_,
            table_info->first_page_id
        );
        
        // Position at first tuple
        iterator_ = table_heap_->Begin();
    }

    /**
     * @brief Get the next tuple
     * 
     * Returns the next tuple from the table.
     */
    bool Next(Tuple* tuple, RID* rid) override {
        while (!iterator_.IsEnd()) {
            *tuple = *iterator_;
            *rid = iterator_.GetRID();
            ++iterator_;
            return true;
        }
        return false;
    }

    /**
     * @brief Get the output schema
     */
    const Schema* GetOutputSchema() const override {
        return schema_;
    }

private:
    std::string table_name_;
    const Schema* schema_{nullptr};
    std::unique_ptr<TableHeap> table_heap_;
    TableIterator iterator_;
};

}  // namespace mydb
