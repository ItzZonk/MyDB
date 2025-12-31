/**
 * @file db.cpp
 * @brief Main database implementation
 */

#include <mydb/db.hpp>
#include <mydb/scripting/python_vm.hpp>

#include <spdlog/spdlog.h>

#include <filesystem>
#include <algorithm>

namespace mydb {

// ============================================================================
// Database Iterator Implementation
// ============================================================================

class DatabaseIteratorImpl : public Database::Iterator {
public:
    explicit DatabaseIteratorImpl(Database* db, const ReadOptions& options)
        : db_(db), options_(options) {}
    
    bool Valid() const override { return valid_; }
    
    void SeekToFirst() override {
        // TODO: Merge iterators from MemTable and all SSTables
        valid_ = false;
    }
    
    void SeekToLast() override {
        valid_ = false;
    }
    
    void Seek(const Slice& key) override {
        valid_ = false;
    }
    
    void Next() override {
        valid_ = false;
    }
    
    void Prev() override {
        valid_ = false;
    }
    
    Slice key() const override { return current_key_; }
    Slice value() const override { return current_value_; }
    Status status() const override { return status_; }
    
private:
    Database* db_;
    ReadOptions options_;
    bool valid_{false};
    std::string current_key_;
    std::string current_value_;
    Status status_;
};

// ============================================================================
// Database Implementation
// ============================================================================

Database::Database(const Options& options)
    : options_(options)
    , db_path_(options.db_path) {
}

Database::~Database() {
    // Stop compactor first
    if (compactor_) {
        compactor_->Stop();
    }
    
    // Flush any remaining data
    if (memtable_ && memtable_->Count() > 0) {
        Flush();
    }
    
    spdlog::info("Database closed: {}", db_path_);
}

Result<std::unique_ptr<Database>> Database::Open(const Options& options) {
    auto db = std::unique_ptr<Database>(new Database(options));
    
    Status s = db->Initialize();
    if (!s.ok()) {
        return s;
    }
    
    return std::move(db);
}

Status Database::Initialize() {
    spdlog::info("Opening database: {}", db_path_);
    
    // Create directory if needed
    if (options_.create_if_missing) {
        std::filesystem::create_directories(db_path_);
    }
    
    if (!std::filesystem::exists(db_path_)) {
        return Status::IOError("Database path does not exist: " + db_path_);
    }
    
    // Check for existing database
    std::string manifest_path = db_path_ + "/MANIFEST";
    bool is_new = !std::filesystem::exists(manifest_path);
    
    if (!is_new && options_.error_if_exists) {
        return Status::AlreadyExists("Database already exists: " + db_path_);
    }
    
    // Initialize components
    memtable_ = std::make_unique<MemTable>();
    
    // WAL Manager
    wal_manager_ = std::make_unique<WALManager>(db_path_);
    
    // Version set
    versions_ = std::make_unique<VersionSet>(db_path_);
    
    // Load existing state or start fresh
    if (!is_new) {
        MYDB_RETURN_IF_ERROR(versions_->LoadManifest());
        MYDB_RETURN_IF_ERROR(Recover());
    }
    
    // Create new WAL
    auto wal_result = wal_manager_->CreateWriter(sequence_.load());
    if (!wal_result.ok()) {
        return wal_result.status();
    }
    wal_ = std::move(wal_result.value());
    
    // Start compactor
    compactor_ = std::make_unique<Compactor>(versions_.get(), db_path_);
    compactor_->Start();
    
#ifdef MYDB_ENABLE_PYTHON
    if (options_.enable_python) {
        python_vm_ = std::make_unique<PythonVM>(this);
        spdlog::info("Python scripting enabled");
    }
#endif
    
    spdlog::info("Database opened successfully");
    return Status::Ok();
}

Status Database::Recover() {
    spdlog::info("Recovering from WAL files...");
    
    auto wal_files = wal_manager_->GetWALFiles();
    
    for (const auto& wal_file : wal_files) {
        WALReader reader(wal_file);
        
        Status s = reader.ForEach([this](const WALRecord& record) {
            memtable_->Add(record.key, record.value, record.sequence, record.type);
            
            if (record.sequence > sequence_.load()) {
                sequence_.store(record.sequence);
            }
            
            return Status::Ok();
        });
        
        if (!s.ok()) {
            spdlog::warn("Error reading WAL {}: {}", wal_file, s.ToString());
        }
    }
    
    spdlog::info("Recovery complete, sequence: {}", sequence_.load());
    return Status::Ok();
}

Status Database::WriteInternal(const Slice& key, const Slice& value,
                                OperationType type, const WriteOptions& options) {
    std::lock_guard<std::mutex> write_lock(write_mutex_);
    
    SequenceNumber seq = sequence_.fetch_add(1, std::memory_order_relaxed) + 1;
    
    // Write to WAL first (for durability)
    if (options_.enable_wal && !options.disable_wal) {
        MYDB_RETURN_IF_ERROR(wal_->Append(type, key, value, seq));
        
        if (options.sync) {
            MYDB_RETURN_IF_ERROR(wal_->Sync());
        }
    }
    
    // Write to MemTable
    memtable_->Add(key, value, seq, type);
    
    // Update stats
    if (type == OperationType::kPut) {
        stats_.writes++;
    } else {
        stats_.deletes++;
    }
    
    // Check if we need to flush
    if (memtable_->ShouldFlush()) {
        MYDB_RETURN_IF_ERROR(RotateMemTable());
    }
    
    return Status::Ok();
}

Status Database::RotateMemTable() {
    spdlog::debug("Rotating MemTable");
    
    // Wait for any previous immutable memtable to be flushed
    if (immutable_memtable_) {
        MYDB_RETURN_IF_ERROR(FlushImmutableMemTable());
    }
    
    // Swap memtables
    immutable_memtable_ = std::move(memtable_);
    memtable_ = std::make_unique<MemTable>();
    
    // Create new WAL
    wal_->Close();
    auto wal_result = wal_manager_->CreateWriter(sequence_.load());
    if (!wal_result.ok()) {
        return wal_result.status();
    }
    wal_ = std::move(wal_result.value());
    
    // Flush immutable memtable in background
    return FlushImmutableMemTable();
}

Status Database::FlushImmutableMemTable() {
    if (!immutable_memtable_) {
        return Status::Ok();
    }
    
    spdlog::info("Flushing MemTable to SSTable");
    
    // Generate new SSTable filename
    uint64_t file_number = versions_->NextFileNumber();
    std::string filename = db_path_ + "/" + std::to_string(file_number) + ".sst";
    
    // Build SSTable
    SSTableBuilder builder(filename);
    
    auto iter = immutable_memtable_->NewIterator();
    iter->SeekToFirst();
    
    std::string smallest_key, largest_key;
    uint64_t entry_count = 0;
    
    while (iter->Valid()) {
        if (entry_count == 0) {
            smallest_key = iter->key().ToString();
        }
        largest_key = iter->key().ToString();
        
        MYDB_RETURN_IF_ERROR(builder.Add(iter->key(), iter->value()));
        entry_count++;
        iter->Next();
    }
    
    MYDB_RETURN_IF_ERROR(builder.Finish());
    
    // Add to version set
    FileMetaData meta;
    meta.file_number = file_number;
    meta.filename = filename;
    meta.file_size = builder.FileSize();
    meta.smallest_key = smallest_key;
    meta.largest_key = largest_key;
    meta.num_entries = entry_count;
    meta.level = 0;
    
    versions_->AddFile(0, meta);
    MYDB_RETURN_IF_ERROR(versions_->WriteManifest());
    
    // Clear immutable memtable
    immutable_memtable_.reset();
    
    // Cleanup old WALs
    wal_manager_->CleanupOldWALs(sequence_.load());
    
    // Maybe trigger compaction
    compactor_->MaybeScheduleCompaction();
    
    spdlog::info("Flush complete: {}, {} entries", filename, entry_count);
    
    return Status::Ok();
}

Result<std::string> Database::Get(const Slice& key, const ReadOptions& options) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    stats_.reads++;
    SequenceNumber snapshot = options.snapshot ? options.snapshot : sequence_.load();
    
    // Check MemTable first
    auto result = memtable_->Get(key, snapshot);
    if (result.ok()) {
        stats_.cache_hits++;
        return result;
    }
    
    // Check immutable MemTable
    if (immutable_memtable_) {
        result = immutable_memtable_->Get(key, snapshot);
        if (result.ok()) {
            stats_.cache_hits++;
            return result;
        }
    }
    
    stats_.cache_misses++;
    
    // Check SSTables
    return GetFromLevels(key, snapshot);
}

Result<std::string> Database::GetFromLevels(const Slice& key, SequenceNumber seq) {
    // Search through all levels
    for (int level = 0; level < static_cast<int>(kMaxLevels); ++level) {
        auto files = versions_->GetFilesAtLevel(level);
        
        // For level 0, files may overlap, check all
        // For level > 0, files are sorted and non-overlapping
        for (const auto& file : files) {
            // Quick key range check
            if (key.ToString() < file.smallest_key || key.ToString() > file.largest_key) {
                continue;
            }
            
            auto reader_result = SSTableReader::Open(file.filename);
            if (!reader_result.ok()) {
                continue;
            }
            
            auto& reader = reader_result.value();
            
            // Bloom filter check
            if (!reader->MayContain(key)) {
                continue;
            }
            
            auto result = reader->Get(key);
            if (result.ok()) {
                return result;
            }
        }
    }
    
    return Status::NotFound("Key not found");
}

Status Database::Put(const Slice& key, const Slice& value, const WriteOptions& options) {
    return WriteInternal(key, value, OperationType::kPut, options);
}

Status Database::Delete(const Slice& key, const WriteOptions& options) {
    return WriteInternal(key, Slice(), OperationType::kDelete, options);
}

bool Database::Exists(const Slice& key) {
    auto result = Get(key);
    return result.ok();
}

// WriteBatch
void Database::WriteBatch::Put(const Slice& key, const Slice& value) {
    operations_.push_back({OperationType::kPut, key.ToString(), value.ToString()});
}

void Database::WriteBatch::Delete(const Slice& key) {
    operations_.push_back({OperationType::kDelete, key.ToString(), ""});
}

void Database::WriteBatch::Clear() {
    operations_.clear();
}

Status Database::Write(const WriteBatch& batch, const WriteOptions& options) {
    std::lock_guard<std::mutex> write_lock(write_mutex_);
    
    for (const auto& op : batch.operations_) {
        SequenceNumber seq = sequence_.fetch_add(1, std::memory_order_relaxed) + 1;
        
        if (options_.enable_wal && !options.disable_wal) {
            MYDB_RETURN_IF_ERROR(wal_->Append(op.type, op.key, op.value, seq));
        }
        
        memtable_->Add(op.key, op.value, seq, op.type);
    }
    
    if (options.sync && options_.enable_wal && !options.disable_wal) {
        MYDB_RETURN_IF_ERROR(wal_->Sync());
    }
    
    if (memtable_->ShouldFlush()) {
        MYDB_RETURN_IF_ERROR(RotateMemTable());
    }
    
    return Status::Ok();
}

std::unique_ptr<Database::Iterator> Database::NewIterator(const ReadOptions& options) {
    return std::make_unique<DatabaseIteratorImpl>(this, options);
}

Status Database::Flush() {
    std::lock_guard<std::mutex> write_lock(write_mutex_);
    return RotateMemTable();
}

Status Database::CompactLevel(int level) {
    return compactor_->CompactLevel(level);
}

SequenceNumber Database::GetSnapshot() {
    return sequence_.load();
}

void Database::ReleaseSnapshot(SequenceNumber) {
    // Snapshots are just sequence numbers, no cleanup needed
}

#ifdef MYDB_ENABLE_PYTHON
Result<std::string> Database::ExecutePython(const std::string& script) {
    if (!python_vm_ || !python_vm_->IsInitialized()) {
        return Status::NotSupported("Python scripting not available");
    }
    return python_vm_->Execute(script);
}
#endif

Database::Stats Database::GetStats() const {
    stats_.num_entries = memtable_->Count();
    stats_.memtable_size = memtable_->ApproximateMemoryUsage();
    stats_.sequence = sequence_.load();
    
    // Count SSTables
    stats_.num_sstables = 0;
    stats_.disk_usage = 0;
    for (int level = 0; level < static_cast<int>(kMaxLevels); ++level) {
        auto files = versions_->GetFilesAtLevel(level);
        stats_.num_sstables += files.size();
        for (const auto& f : files) {
            stats_.disk_usage += f.file_size;
        }
    }
    
    return stats_;
}

std::string Database::GetVersion() const {
    return std::string(kVersion);
}

} // namespace mydb
