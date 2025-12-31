/**
 * @file compactor.cpp
 * @brief Background compaction implementation
 */

#include <mydb/engine/compactor.hpp>

#include <spdlog/spdlog.h>

#include <filesystem>
#include <algorithm>

namespace mydb {

// ============================================================================
// FileMetaData Implementation
// ============================================================================

bool FileMetaData::Overlaps(const std::string& min_key, const std::string& max_key) const {
    return !(largest_key < min_key || smallest_key > max_key);
}

// ============================================================================
// VersionSet Implementation
// ============================================================================

VersionSet::VersionSet(const std::string& db_path)
    : db_path_(db_path)
    , levels_(kMaxLevels) {
}

std::vector<FileMetaData> VersionSet::GetFilesAtLevel(int level) const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (level < 0 || level >= static_cast<int>(levels_.size())) {
        return {};
    }
    return levels_[level];
}

void VersionSet::AddFile(int level, const FileMetaData& file) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (level >= 0 && level < static_cast<int>(levels_.size())) {
        levels_[level].push_back(file);
        spdlog::debug("Added file {} to level {}", file.filename, level);
    }
}

void VersionSet::RemoveFiles(int level, const std::vector<uint64_t>& file_numbers) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (level < 0 || level >= static_cast<int>(levels_.size())) {
        return;
    }
    
    auto& files = levels_[level];
    files.erase(
        std::remove_if(files.begin(), files.end(),
            [&file_numbers](const FileMetaData& f) {
                return std::find(file_numbers.begin(), file_numbers.end(), 
                                f.file_number) != file_numbers.end();
            }),
        files.end()
    );
}

size_t VersionSet::NumFilesAtLevel0() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return levels_.empty() ? 0 : levels_[0].size();
}

uint64_t VersionSet::LevelSize(int level) const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (level < 0 || level >= static_cast<int>(levels_.size())) {
        return 0;
    }
    
    uint64_t total = 0;
    for (const auto& f : levels_[level]) {
        total += f.file_size;
    }
    return total;
}

bool VersionSet::NeedsCompaction(int level) const {
    if (level == 0) {
        return NumFilesAtLevel0() >= kLevel0CompactionTrigger;
    }
    
    // For level L > 0, compact if size exceeds threshold
    // Level 1 target: 10 MB, Level 2: 100 MB, etc.
    uint64_t target_size = 10 * 1024 * 1024;  // 10 MB for level 1
    for (int i = 1; i < level; ++i) {
        target_size *= kLevelSizeMultiplier;
    }
    
    return LevelSize(level) > target_size;
}

uint64_t VersionSet::NextFileNumber() {
    return next_file_number_.fetch_add(1, std::memory_order_relaxed);
}

Status VersionSet::WriteManifest() {
    std::string manifest_path = db_path_ + "/MANIFEST";
    std::ofstream file(manifest_path, std::ios::binary | std::ios::trunc);
    
    if (!file.is_open()) {
        return Status::IOError("Failed to write MANIFEST");
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Write next file number
    uint64_t next_num = next_file_number_.load();
    file.write(reinterpret_cast<const char*>(&next_num), sizeof(next_num));
    
    // Write number of levels
    uint32_t num_levels = static_cast<uint32_t>(levels_.size());
    file.write(reinterpret_cast<const char*>(&num_levels), sizeof(num_levels));
    
    // Write each level
    for (const auto& level : levels_) {
        uint32_t num_files = static_cast<uint32_t>(level.size());
        file.write(reinterpret_cast<const char*>(&num_files), sizeof(num_files));
        
        for (const auto& f : level) {
            // Write file metadata
            file.write(reinterpret_cast<const char*>(&f.file_number), sizeof(f.file_number));
            file.write(reinterpret_cast<const char*>(&f.file_size), sizeof(f.file_size));
            
            uint32_t fn_len = static_cast<uint32_t>(f.filename.size());
            file.write(reinterpret_cast<const char*>(&fn_len), sizeof(fn_len));
            file.write(f.filename.data(), fn_len);
            
            uint32_t sk_len = static_cast<uint32_t>(f.smallest_key.size());
            file.write(reinterpret_cast<const char*>(&sk_len), sizeof(sk_len));
            file.write(f.smallest_key.data(), sk_len);
            
            uint32_t lk_len = static_cast<uint32_t>(f.largest_key.size());
            file.write(reinterpret_cast<const char*>(&lk_len), sizeof(lk_len));
            file.write(f.largest_key.data(), lk_len);
            
            file.write(reinterpret_cast<const char*>(&f.num_entries), sizeof(f.num_entries));
        }
    }
    
    return file.good() ? Status::Ok() : Status::IOError("Failed to write MANIFEST");
}

Status VersionSet::LoadManifest() {
    std::string manifest_path = db_path_ + "/MANIFEST";
    
    if (!std::filesystem::exists(manifest_path)) {
        return Status::NotFound("No MANIFEST file");
    }
    
    std::ifstream file(manifest_path, std::ios::binary);
    if (!file.is_open()) {
        return Status::IOError("Failed to read MANIFEST");
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Read next file number
    uint64_t next_num;
    file.read(reinterpret_cast<char*>(&next_num), sizeof(next_num));
    next_file_number_.store(next_num);
    
    // Read number of levels
    uint32_t num_levels;
    file.read(reinterpret_cast<char*>(&num_levels), sizeof(num_levels));
    
    levels_.resize(num_levels);
    
    // Read each level
    for (uint32_t l = 0; l < num_levels; ++l) {
        uint32_t num_files;
        file.read(reinterpret_cast<char*>(&num_files), sizeof(num_files));
        
        for (uint32_t i = 0; i < num_files; ++i) {
            FileMetaData f;
            f.level = static_cast<int>(l);
            
            file.read(reinterpret_cast<char*>(&f.file_number), sizeof(f.file_number));
            file.read(reinterpret_cast<char*>(&f.file_size), sizeof(f.file_size));
            
            uint32_t fn_len;
            file.read(reinterpret_cast<char*>(&fn_len), sizeof(fn_len));
            f.filename.resize(fn_len);
            file.read(f.filename.data(), fn_len);
            
            uint32_t sk_len;
            file.read(reinterpret_cast<char*>(&sk_len), sizeof(sk_len));
            f.smallest_key.resize(sk_len);
            file.read(f.smallest_key.data(), sk_len);
            
            uint32_t lk_len;
            file.read(reinterpret_cast<char*>(&lk_len), sizeof(lk_len));
            f.largest_key.resize(lk_len);
            file.read(f.largest_key.data(), lk_len);
            
            file.read(reinterpret_cast<char*>(&f.num_entries), sizeof(f.num_entries));
            
            levels_[l].push_back(f);
        }
    }
    
    return file.good() ? Status::Ok() : Status::IOError("Error reading MANIFEST");
}

// ============================================================================
// Compactor Implementation
// ============================================================================

Compactor::Compactor(VersionSet* versions, const std::string& db_path)
    : versions_(versions)
    , db_path_(db_path) {
}

Compactor::~Compactor() {
    Stop();
}

void Compactor::Start() {
    if (running_.load()) return;
    
    running_.store(true);
    background_thread_ = std::jthread([this](std::stop_token stop_token) {
        spdlog::info("Compactor thread started");
        BackgroundThread();
        spdlog::info("Compactor thread stopped");
    });
}

void Compactor::Stop() {
    if (!running_.load()) return;
    
    running_.store(false);
    cv_.notify_all();
    
    if (background_thread_.joinable()) {
        background_thread_.request_stop();
        background_thread_.join();
    }
}

void Compactor::MaybeScheduleCompaction() {
    pending_compaction_.store(true);
    cv_.notify_one();
}

void Compactor::WaitForCompaction() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this] { 
        return !pending_compaction_.load(); 
    });
}

Status Compactor::CompactLevel(int level) {
    auto job_opt = PickCompaction();
    if (!job_opt) {
        return Status::Ok();  // Nothing to compact
    }
    
    return DoCompaction(*job_opt);
}

Compactor::Stats Compactor::GetStats() const {
    return stats_;
}

void Compactor::BackgroundThread() {
    while (running_.load()) {
        std::unique_lock<std::mutex> lock(mutex_);
        
        cv_.wait_for(lock, std::chrono::seconds(1), [this] {
            return !running_.load() || pending_compaction_.load();
        });
        
        if (!running_.load()) break;
        
        pending_compaction_.store(false);
        
        // Check if any level needs compaction
        for (int level = 0; level < static_cast<int>(kMaxLevels - 1); ++level) {
            if (versions_->NeedsCompaction(level)) {
                lock.unlock();
                
                auto job_opt = PickCompaction();
                if (job_opt) {
                    Status s = DoCompaction(*job_opt);
                    if (!s.ok()) {
                        spdlog::error("Compaction failed: {}", s.ToString());
                    }
                }
                
                break;
            }
        }
    }
}

std::optional<CompactionJob> Compactor::PickCompaction() {
    // Simple strategy: pick level 0 if it needs compaction
    for (int level = 0; level < static_cast<int>(kMaxLevels - 1); ++level) {
        if (versions_->NeedsCompaction(level)) {
            CompactionJob job;
            job.level = level;
            job.inputs = versions_->GetFilesAtLevel(level);
            
            if (job.inputs.empty()) {
                continue;
            }
            
            return job;
        }
    }
    
    return std::nullopt;
}

std::vector<FileMetaData> Compactor::GetOverlappingFiles(
    int level,
    const std::string& min_key,
    const std::string& max_key
) {
    std::vector<FileMetaData> result;
    auto files = versions_->GetFilesAtLevel(level);
    
    for (const auto& f : files) {
        if (f.Overlaps(min_key, max_key)) {
            result.push_back(f);
        }
    }
    
    return result;
}

Status Compactor::DoCompaction(CompactionJob& job) {
    spdlog::info("Starting compaction at level {}, {} input files", 
                 job.level, job.inputs.size());
    
    if (job.inputs.empty()) {
        return Status::Ok();
    }
    
    // Open all input SSTables
    std::vector<std::unique_ptr<SSTableReader>> readers;
    std::vector<SSTableReader*> reader_ptrs;
    
    for (const auto& file : job.inputs) {
        auto result = SSTableReader::Open(file.filename);
        if (!result.ok()) {
            spdlog::error("Failed to open SSTable for compaction: {}", file.filename);
            return result.status();
        }
        reader_ptrs.push_back(result.value().get());
        readers.push_back(std::move(result.value()));
    }
    
    // Generate output filename
    uint64_t output_number = versions_->NextFileNumber();
    std::string output_filename = db_path_ + "/" + std::to_string(output_number) + ".sst";
    
    // Merge into new SSTable
    Status s = SSTableMerger::Merge(reader_ptrs, output_filename);
    if (!s.ok()) {
        spdlog::error("Merge failed: {}", s.ToString());
        return s;
    }
    
    // Open the new SSTable to get metadata
    auto new_reader = SSTableReader::Open(output_filename);
    if (!new_reader.ok()) {
        return new_reader.status();
    }
    
    FileMetaData output_meta;
    output_meta.file_number = output_number;
    output_meta.filename = output_filename;
    output_meta.file_size = new_reader.value()->FileSize();
    output_meta.smallest_key = new_reader.value()->SmallestKey();
    output_meta.largest_key = new_reader.value()->LargestKey();
    output_meta.num_entries = new_reader.value()->NumEntries();
    output_meta.level = job.level + 1;  // Output goes to next level
    
    // Update version set
    std::vector<uint64_t> deleted_numbers;
    for (const auto& f : job.inputs) {
        deleted_numbers.push_back(f.file_number);
    }
    
    versions_->RemoveFiles(job.level, deleted_numbers);
    versions_->AddFile(job.level + 1, output_meta);
    MYDB_RETURN_IF_ERROR(versions_->WriteManifest());
    
    // Delete old files
    for (const auto& f : job.inputs) {
        std::filesystem::remove(f.filename);
        spdlog::debug("Deleted old SSTable: {}", f.filename);
    }
    
    // Update stats
    for (const auto& r : readers) {
        stats_.bytes_read += r->FileSize();
    }
    stats_.bytes_written += output_meta.file_size;
    stats_.files_compacted += job.inputs.size();
    stats_.compactions_completed++;
    
    spdlog::info("Compaction complete: {} files merged into {}", 
                 job.inputs.size(), output_filename);
    
    return Status::Ok();
}

} // namespace mydb
