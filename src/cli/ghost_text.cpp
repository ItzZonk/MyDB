/**
 * @file ghost_text.cpp
 * @brief Async ghost text prediction implementation
 */

#include <mydb/cli/ghost_text.hpp>
#include <mydb/ai/rag_pipeline.hpp>

#include <spdlog/spdlog.h>

#include <chrono>
#include <thread>

namespace mydb::ai {

// ============================================================================
// GhostTextRenderer Implementation
// ============================================================================

GhostTextRenderer::GhostTextRenderer(RAGPipeline* rag, const GhostTextConfig& config)
    : rag_(rag)
    , config_(config) {
    spdlog::debug("GhostTextRenderer initialized");
}

GhostTextRenderer::~GhostTextRenderer() {
    Cancel();
    
    // Wait for pending future
    if (pending_future_.valid()) {
        pending_future_.wait();
    }
}

void GhostTextRenderer::RequestPrediction(const std::string& partial_input) {
    if (!enabled_) return;
    
    predictions_requested_++;
    
    // Cancel any pending prediction
    Cancel();
    
    // Update state
    {
        std::lock_guard<std::mutex> lock(mutex_);
        last_input_ = partial_input;
        current_prediction_.reset();
    }
    
    // Start new prediction
    uint64_t request_id = ++current_request_id_;
    cancelled_ = false;
    
    // Launch async prediction
    pending_future_ = std::async(std::launch::async, 
        &GhostTextRenderer::PredictionWorker, this, partial_input, request_id);
}

void GhostTextRenderer::PredictionWorker(std::string input, uint64_t request_id) {
    auto start = std::chrono::steady_clock::now();
    
    // Debounce
    std::this_thread::sleep_for(std::chrono::milliseconds(config_.debounce_ms));
    
    // Check if cancelled or stale
    if (cancelled_ || request_id != current_request_id_) {
        predictions_cancelled_++;
        return;
    }
    
    if (!rag_) {
        return;
    }
    
    // Get completion from RAG
    auto result = rag_->AutoComplete(input, 1);
    
    // Check again if cancelled
    if (cancelled_ || request_id != current_request_id_) {
        predictions_cancelled_++;
        return;
    }
    
    if (!result.ok() || result.value().empty()) {
        return;
    }
    
    std::string completion = result.value()[0];
    
    // Extract just the continuation (remove the input prefix)
    std::string continuation;
    if (completion.size() > input.size() && completion.substr(0, input.size()) == input) {
        continuation = completion.substr(input.size());
    } else {
        continuation = completion;  // Full suggestion
    }
    
    // Truncate if too long
    if (continuation.size() > config_.max_prediction_length) {
        continuation = continuation.substr(0, config_.max_prediction_length);
    }
    
    auto end = std::chrono::steady_clock::now();
    double time_ms = std::chrono::duration<double, std::milli>(end - start).count();
    
    // Store prediction
    {
        std::lock_guard<std::mutex> lock(mutex_);
        
        // Check if input changed while we were computing
        bool is_stale = (last_input_ != input);
        
        current_prediction_ = Prediction{
            .text = continuation,
            .confidence = 0.8f,  // TODO: Get from RAG
            .timestamp = std::chrono::steady_clock::now(),
            .is_stale = is_stale
        };
    }
    
    predictions_completed_++;
    total_prediction_time_ms_ = total_prediction_time_ms_.load() + time_ms;
    
    spdlog::debug("Ghost text prediction: '{}' -> '{}' ({:.1f}ms)", 
                  input, continuation, time_ms);
}

void GhostTextRenderer::Cancel() {
    cancelled_ = true;
}

bool GhostTextRenderer::HasPrediction() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return current_prediction_.has_value() && !current_prediction_->is_stale;
}

std::optional<Prediction> GhostTextRenderer::GetPrediction() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return current_prediction_;
}

std::string GhostTextRenderer::GetPredictionText() const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (current_prediction_ && !current_prediction_->is_stale) {
        return current_prediction_->text;
    }
    return "";
}

bool GhostTextRenderer::WasAccepted(const std::string& full_input) const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (!current_prediction_) {
        return false;
    }
    
    // Check if full_input = last_input + prediction
    std::string expected = last_input_ + current_prediction_->text;
    return full_input == expected;
}

std::string GhostTextRenderer::Accept() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (!current_prediction_) {
        return "";
    }
    
    predictions_accepted_++;
    std::string text = current_prediction_->text;
    current_prediction_.reset();
    
    return text;
}

void GhostTextRenderer::Clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    current_prediction_.reset();
    last_input_.clear();
}

void GhostTextRenderer::SetConfig(const GhostTextConfig& config) {
    config_ = config;
}

GhostTextConfig GhostTextRenderer::GetConfig() const {
    return config_;
}

GhostTextRenderer::Stats GhostTextRenderer::GetStats() const {
    Stats stats;
    stats.predictions_requested = predictions_requested_;
    stats.predictions_completed = predictions_completed_;
    stats.predictions_accepted = predictions_accepted_;
    stats.predictions_cancelled = predictions_cancelled_;
    
    if (predictions_completed_ > 0) {
        stats.avg_prediction_time_ms = total_prediction_time_ms_ / predictions_completed_;
    } else {
        stats.avg_prediction_time_ms = 0;
    }
    
    return stats;
}

void GhostTextRenderer::SetEnabled(bool enabled) {
    enabled_ = enabled;
    if (!enabled) {
        Cancel();
        Clear();
    }
}

bool GhostTextRenderer::IsEnabled() const {
    return enabled_;
}

// ============================================================================
// Utility Functions
// ============================================================================

std::string FormatGhostText(const std::string& text, bool show_hint) {
    if (text.empty()) {
        return "";
    }
    
    std::string result = style::kGhostText;
    result += text;
    
    if (show_hint) {
        result += " [Tab]";
    }
    
    result += style::kReset;
    
    return result;
}

} // namespace mydb::ai
