/**
 * @file filter_executor.hpp
 * @brief Filter executor with fuzzy matching support
 * 
 * Filters tuples based on a predicate expression.
 * Supports standard comparisons and fuzzy string matching.
 * 
 * @see UGent Research: DDCM Group (fuzzy databases)
 */

#pragma once

#include <mydb/execution/executor.hpp>
#include <mydb/execution/expression.hpp>

namespace mydb {

/**
 * @brief Filter executor (Selection operator)
 * 
 * Wraps a child executor and only returns tuples that satisfy
 * the filter predicate. Supports fuzzy matching predicates.
 * 
 * Example Query:
 *   SELECT * FROM students WHERE name FUZZY LIKE 'Jon' WITH THRESHOLD 0.8
 * 
 * Execution Plan:
 *   Filter (FuzzyLikeExpression)
 *      |
 *   SeqScan (students)
 */
class FilterExecutor : public AbstractExecutor {
public:
    /**
     * @brief Construct filter executor
     * @param ctx Execution context
     * @param child Child executor to filter
     * @param predicate Filter predicate expression
     */
    FilterExecutor(ExecutorContext* ctx, 
                   ExecutorPtr child,
                   AbstractExpression* predicate)
        : AbstractExecutor(ctx), 
          child_(std::move(child)),
          predicate_(predicate) {}

    /**
     * @brief Initialize the filter
     */
    void Init() override {
        child_->Init();
    }

    /**
     * @brief Get next tuple that satisfies the predicate
     * 
     * Repeatedly calls child->Next() until finding a tuple
     * that passes the filter predicate.
     */
    bool Next(Tuple* tuple, RID* rid) override {
        while (child_->Next(tuple, rid)) {
            // Evaluate predicate
            if (predicate_ == nullptr) {
                return true;  // No predicate = accept all
            }
            
            if (predicate_->EvaluateAsBool(tuple, GetOutputSchema())) {
                return true;
            }
        }
        return false;
    }

    /**
     * @brief Get the output schema (same as child)
     */
    const Schema* GetOutputSchema() const override {
        return child_->GetOutputSchema();
    }

private:
    ExecutorPtr child_;
    AbstractExpression* predicate_;
};

}  // namespace mydb
