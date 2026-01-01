/**
 * @file insert_executor.hpp
 * @brief Insert executor for adding tuples with veracity support
 */

#pragma once

#include <mydb/execution/executor.hpp>
#include <mydb/catalog/table_heap.hpp>

namespace mydb {

/**
 * @brief Insert executor
 * 
 * Inserts tuples into a table. Supports optional veracity scores
 * for data quality tracking.
 * 
 * Example:
 *   INSERT INTO students (name, age) VALUES ('Alice', 22) VERACITY 0.95
 */
class InsertExecutor : public AbstractExecutor {
public:
    /**
     * @brief Construct insert executor
     * @param ctx Execution context
     * @param table_name Target table
     * @param values Values to insert
     * @param veracity Data quality score [0.0, 1.0]
     */
    InsertExecutor(ExecutorContext* ctx,
                   const std::string& table_name,
                   std::vector<Value> values,
                   float veracity = 1.0f)
        : AbstractExecutor(ctx),
          table_name_(table_name),
          values_(std::move(values)),
          veracity_(veracity) {}

    /**
     * @brief Initialize the insert
     */
    void Init() override {
        inserted_ = false;
        
        // Get table info
        table_info_ = ctx_->GetCatalog()->GetTable(table_name_);
        if (table_info_ == nullptr) {
            throw std::runtime_error("Table not found: " + table_name_);
        }
        
        schema_ = &table_info_->schema;
    }

    /**
     * @brief Execute the insert
     * 
     * Inserts one tuple and returns the count (1).
     */
    bool Next(Tuple* tuple, RID* rid) override {
        if (inserted_) {
            return false;
        }
        
        // Create tuple with veracity
        Tuple new_tuple(values_, schema_, veracity_);
        
        // Insert into table heap
        TableHeap heap(ctx_->GetBufferPoolManager(), schema_, 
                       table_info_->first_page_id);
        
        RID inserted_rid;
        bool success = heap.InsertTuple(new_tuple, &inserted_rid);
        
        if (success) {
            table_info_->tuple_count++;
        }
        
        // Return count tuple (1 row inserted)
        *tuple = Tuple({Value(success ? 1 : 0)}, nullptr);
        *rid = inserted_rid;
        
        inserted_ = true;
        return true;
    }

    /**
     * @brief Get output schema (count)
     */
    const Schema* GetOutputSchema() const override {
        return nullptr;  // Returns count, not structured data
    }

private:
    std::string table_name_;
    std::vector<Value> values_;
    float veracity_;
    TableInfo* table_info_{nullptr};
    const Schema* schema_{nullptr};
    bool inserted_{false};
};

/**
 * @brief Insert from child executor (INSERT ... SELECT)
 */
class InsertSelectExecutor : public AbstractExecutor {
public:
    InsertSelectExecutor(ExecutorContext* ctx,
                         const std::string& table_name,
                         ExecutorPtr child,
                         float veracity = 1.0f)
        : AbstractExecutor(ctx),
          table_name_(table_name),
          child_(std::move(child)),
          veracity_(veracity) {}

    void Init() override {
        child_->Init();
        count_ = 0;
        done_ = false;
        
        table_info_ = ctx_->GetCatalog()->GetTable(table_name_);
        if (table_info_ == nullptr) {
            throw std::runtime_error("Table not found: " + table_name_);
        }
    }

    bool Next(Tuple* tuple, RID* rid) override {
        if (done_) {
            return false;
        }
        
        TableHeap heap(ctx_->GetBufferPoolManager(), 
                       &table_info_->schema, 
                       table_info_->first_page_id);
        
        Tuple child_tuple;
        RID child_rid;
        
        while (child_->Next(&child_tuple, &child_rid)) {
            // Create new tuple with veracity
            Tuple new_tuple(child_tuple.GetValues(), 
                           &table_info_->schema, 
                           veracity_);
            
            RID inserted_rid;
            if (heap.InsertTuple(new_tuple, &inserted_rid)) {
                count_++;
                table_info_->tuple_count++;
            }
        }
        
        // Return count
        *tuple = Tuple({Value(static_cast<int32_t>(count_))}, nullptr);
        done_ = true;
        return true;
    }

    const Schema* GetOutputSchema() const override {
        return nullptr;
    }

private:
    std::string table_name_;
    ExecutorPtr child_;
    float veracity_;
    TableInfo* table_info_{nullptr};
    size_t count_{0};
    bool done_{false};
};

}  // namespace mydb
