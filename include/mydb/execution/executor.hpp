/**
 * @file executor.hpp
 * @brief Abstract executor interface (Volcano Model)
 * 
 * Implements the Iterator (Volcano) execution model used in modern DBMS.
 * Each operator processes tuples one at a time through Next() calls,
 * enabling pipelined execution without materializing intermediate results.
 * 
 * The Volcano model was introduced by Graefe and decouples operators,
 * allowing modular query execution.
 * 
 * @see UGent Courses: Databases
 */

#pragma once

#include <mydb/catalog/tuple.hpp>
#include <mydb/catalog/schema.hpp>
#include <mydb/execution/executor_context.hpp>

#include <memory>

namespace mydb {

/**
 * @brief Abstract base class for all executors
 * 
 * The Volcano (Iterator) model interface. Each executor implements:
 * - Init(): Initialize/reset the executor
 * - Next(): Get the next tuple (returns false when done)
 * - GetOutputSchema(): Describe output tuples
 * 
 * Executors can be composed: a Filter wraps a SeqScan, etc.
 * 
 * ```
 * Query: SELECT name FROM students WHERE age > 20
 * 
 * Projection (name)
 *      |
 *   Filter (age > 20)
 *      |
 *   SeqScan (students)
 * ```
 */
class AbstractExecutor {
public:
    /**
     * @brief Construct executor with context
     */
    explicit AbstractExecutor(ExecutorContext* ctx) : ctx_(ctx) {}

    /**
     * @brief Virtual destructor
     */
    virtual ~AbstractExecutor() = default;

    /**
     * @brief Initialize the executor
     * 
     * Called before the first Next(). Resets state for re-execution.
     */
    virtual void Init() = 0;

    /**
     * @brief Get the next tuple
     * @param[out] tuple The output tuple
     * @param[out] rid The tuple's RID
     * @return True if a tuple was produced, false if no more tuples
     * 
     * This is the core of the Volcano model. Each call produces one tuple.
     */
    virtual bool Next(Tuple* tuple, RID* rid) = 0;

    /**
     * @brief Get the output schema
     * @return Schema describing output tuples
     */
    virtual const Schema* GetOutputSchema() const = 0;

protected:
    ExecutorContext* ctx_;
};

/**
 * @brief Convenience using for executor pointers
 */
using ExecutorPtr = std::unique_ptr<AbstractExecutor>;

}  // namespace mydb
