/**
 * @file executor_context.hpp
 * @brief Context for query execution
 * 
 * Provides access to database resources during query execution.
 */

#pragma once

#include <mydb/catalog/catalog.hpp>
#include <mydb/storage/buffer_pool.hpp>

namespace mydb {

/**
 * @brief Execution context for queries
 * 
 * Provides executors with access to:
 * - Buffer pool for page access
 * - Catalog for table/schema lookups
 * - Transaction context (future)
 */
class ExecutorContext {
public:
    ExecutorContext(BufferPoolManager* bpm, Catalog* catalog)
        : bpm_(bpm), catalog_(catalog) {}

    BufferPoolManager* GetBufferPoolManager() { return bpm_; }
    Catalog* GetCatalog() { return catalog_; }

private:
    BufferPoolManager* bpm_;
    Catalog* catalog_;
};

}  // namespace mydb
