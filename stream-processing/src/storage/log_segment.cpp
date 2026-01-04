/**
 * @file log_segment.cpp
 * @brief Append-only log segment implementation
 */

#include "log_segment.h"

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <iostream>

namespace fs = std::filesystem;

namespace stream {

// =============================================================================
// Helper Functions
// =============================================================================

namespace {

template<typename T>
bool write_value(std::fstream& file, T value) {
    return file.write(reinterpret_cast<const char*>(&value), sizeof(T)).good();
}

template<typename T>
bool read_value(std::fstream& file, T& value) {
    return file.read(reinterpret_cast<char*>(&value), sizeof(T)).good();
}

} // anonymous namespace

// =============================================================================
// LogSegment Implementation
// =============================================================================

std::unique_ptr<LogSegment> LogSegment::create(const std::string& path, uint64_t base_offset) {
    auto segment = std::unique_ptr<LogSegment>(new LogSegment());
    segment->path_ = path;
    segment->base_offset_ = base_offset;
    segment->next_offset_ = base_offset;
    
    // Create parent directory if needed
    fs::path file_path(path);
    if (file_path.has_parent_path()) {
        std::error_code ec;
        fs::create_directories(file_path.parent_path(), ec);
        if (ec) {
            return nullptr;
        }
    }
    
    // Open file for writing (create new)
    segment->file_.open(path, std::ios::binary | std::ios::in | std::ios::out | std::ios::trunc);
    if (!segment->file_.is_open()) {
        return nullptr;
    }
    
    // Write header
    if (!segment->write_header()) {
        return nullptr;
    }
    
    segment->file_size_ = HEADER_SIZE;
    return segment;
}

std::unique_ptr<LogSegment> LogSegment::open(const std::string& path) {
    if (!fs::exists(path)) {
        return nullptr;
    }
    
    auto segment = std::unique_ptr<LogSegment>(new LogSegment());
    segment->path_ = path;
    
    // Open file for reading and appending
    segment->file_.open(path, std::ios::binary | std::ios::in | std::ios::out);
    if (!segment->file_.is_open()) {
        return nullptr;
    }
    
    // Read and validate header
    if (!segment->read_header()) {
        return nullptr;
    }
    
    // Build index by scanning file
    if (!segment->build_index()) {
        return nullptr;
    }
    
    // Get file size
    segment->file_.seekg(0, std::ios::end);
    segment->file_size_ = segment->file_.tellg();
    
    return segment;
}

LogSegment::~LogSegment() {
    if (!closed_) {
        close();
    }
}

bool LogSegment::write_header() {
    file_.seekp(0);
    
    if (!write_value(file_, MAGIC)) return false;
    if (!write_value(file_, VERSION)) return false;
    if (!write_value(file_, base_offset_)) return false;
    if (!write_value(file_, uint64_t{0})) return false; // message count placeholder
    
    return file_.good();
}

bool LogSegment::read_header() {
    file_.seekg(0);
    
    uint32_t magic, version;
    uint64_t count;
    
    if (!read_value(file_, magic)) return false;
    if (!read_value(file_, version)) return false;
    if (!read_value(file_, base_offset_)) return false;
    if (!read_value(file_, count)) return false;
    
    if (magic != MAGIC) {
        return false; // Invalid file
    }
    
    if (version != VERSION) {
        return false; // Unsupported version
    }
    
    return true;
}

bool LogSegment::update_header_count() {
    auto current_pos = file_.tellp();
    
    // Seek to count field (after magic + version + base_offset)
    file_.seekp(sizeof(uint32_t) + sizeof(uint32_t) + sizeof(uint64_t));
    
    uint64_t count = index_.size();
    if (!write_value(file_, count)) {
        return false;
    }
    
    // Restore position
    file_.seekp(current_pos);
    return true;
}

bool LogSegment::build_index() {
    index_.clear();
    
    // Start after header
    file_.seekg(HEADER_SIZE);
    uint64_t position = HEADER_SIZE;
    uint64_t current_offset = base_offset_;
    
    while (file_.good() && !file_.eof()) {
        uint32_t msg_size;
        if (!read_value(file_, msg_size)) {
            break; // End of file or error
        }
        
        if (msg_size == 0) {
            break; // Invalid
        }
        
        // Add to index
        IndexEntry entry;
        entry.offset = current_offset;
        entry.file_position = position;
        entry.message_size = msg_size;
        index_.push_back(entry);
        
        // Skip message data
        file_.seekg(msg_size, std::ios::cur);
        position = file_.tellg();
        current_offset++;
    }
    
    next_offset_ = current_offset;
    file_.clear(); // Clear EOF flag
    
    return true;
}

const LogSegment::IndexEntry* LogSegment::find_entry(uint64_t offset) const {
    if (offset < base_offset_ || offset >= next_offset_) {
        return nullptr;
    }
    
    // Binary search in index
    auto it = std::lower_bound(index_.begin(), index_.end(), offset,
        [](const IndexEntry& entry, uint64_t off) {
            return entry.offset < off;
        });
    
    if (it != index_.end() && it->offset == offset) {
        return &(*it);
    }
    
    return nullptr;
}

uint64_t LogSegment::append(Message& msg) {
    std::lock_guard<std::mutex> lock(write_mutex_);
    
    if (closed_) {
        return 0;
    }
    
    // Assign offset to message
    msg.offset = next_offset_;
    msg.compute_checksum();
    
    // Serialize message
    auto data = msg.serialize();
    uint32_t msg_size = static_cast<uint32_t>(data.size());
    
    // Seek to end
    file_.seekp(0, std::ios::end);
    uint64_t position = file_.tellp();
    
    // Write size + data
    if (!write_value(file_, msg_size)) {
        return 0;
    }
    
    if (!file_.write(reinterpret_cast<const char*>(data.data()), data.size())) {
        return 0;
    }
    
    // Add to index
    IndexEntry entry;
    entry.offset = next_offset_;
    entry.file_position = position;
    entry.message_size = msg_size;
    index_.push_back(entry);
    
    // Update state
    uint64_t assigned_offset = next_offset_;
    next_offset_++;
    file_size_ = file_.tellp();
    
    return assigned_offset;
}

MessagePtr LogSegment::read(uint64_t offset) const {
    const IndexEntry* entry = find_entry(offset);
    if (!entry) {
        return nullptr;
    }
    
    // If segment is closed, we need to reopen for reading
    if (closed_) {
        // Open temporarily for reading
        std::ifstream read_file(path_, std::ios::binary);
        if (!read_file.is_open()) {
            return nullptr;
        }
        
        // Seek to message (skip the size field position, then read size)
        read_file.seekg(entry->file_position);
        
        // Read size
        uint32_t msg_size;
        if (!read_file.read(reinterpret_cast<char*>(&msg_size), sizeof(msg_size))) {
            return nullptr;
        }
        
        if (msg_size != entry->message_size) {
            return nullptr;
        }
        
        // Read message data
        std::vector<uint8_t> data(msg_size);
        if (!read_file.read(reinterpret_cast<char*>(data.data()), msg_size)) {
            return nullptr;
        }
        
        auto msg = Message::deserialize(data.data(), data.size());
        if (msg && !msg->verify_checksum()) {
            return nullptr;
        }
        return msg;
    }
    
    // Seek to message
    file_.seekg(entry->file_position);
    
    // Read size
    uint32_t msg_size;
    if (!read_value(file_, msg_size)) {
        return nullptr;
    }
    
    if (msg_size != entry->message_size) {
        return nullptr; // Corruption detected
    }
    
    // Read message data
    std::vector<uint8_t> data(msg_size);
    if (!file_.read(reinterpret_cast<char*>(data.data()), msg_size)) {
        return nullptr;
    }
    
    // Deserialize
    auto msg = Message::deserialize(data.data(), data.size());
    if (msg && !msg->verify_checksum()) {
        return nullptr; // Checksum mismatch
    }
    
    return msg;
}

std::vector<MessagePtr> LogSegment::read_batch(uint64_t start_offset, size_t max_messages) const {
    std::vector<MessagePtr> messages;
    messages.reserve(max_messages);
    
    // Find starting entry
    auto it = std::lower_bound(index_.begin(), index_.end(), start_offset,
        [](const IndexEntry& entry, uint64_t off) {
            return entry.offset < off;
        });
    
    // Read messages
    while (it != index_.end() && messages.size() < max_messages) {
        auto msg = read(it->offset);
        if (msg) {
            messages.push_back(std::move(msg));
        }
        ++it;
    }
    
    return messages;
}

bool LogSegment::flush() {
    std::lock_guard<std::mutex> lock(write_mutex_);
    
    if (closed_) {
        return false;
    }
    
    file_.flush();
    return file_.good();
}

void LogSegment::close() {
    std::lock_guard<std::mutex> lock(write_mutex_);
    
    if (closed_) {
        return;
    }
    
    // Update message count in header
    update_header_count();
    
    // Flush and close
    file_.flush();
    file_.close();
    closed_ = true;
}

// =============================================================================
// LogManager Implementation
// =============================================================================

LogManager::LogManager(const Config& config)
    : config_(config) {
    // Create base directory if needed
    std::error_code ec;
    fs::create_directories(config_.base_path, ec);
    
    // Load existing segments
    load_segments();
    
    // Create first segment if none exist
    if (segments_.empty()) {
        rotate_segment();
    }
}

LogManager::~LogManager() {
    flush();
    // Segments are closed by their destructors
}

bool LogManager::load_segments() {
    if (!fs::exists(config_.base_path)) {
        return true; // Empty directory
    }
    
    std::vector<std::string> segment_files;
    
    // Find all .log files
    for (const auto& entry : fs::directory_iterator(config_.base_path)) {
        if (entry.path().extension() == ".log") {
            segment_files.push_back(entry.path().string());
        }
    }
    
    // Sort by filename (which encodes base offset)
    std::sort(segment_files.begin(), segment_files.end());
    
    // Open each segment
    for (const auto& file : segment_files) {
        auto segment = LogSegment::open(file);
        if (segment) {
            segments_.push_back(std::move(segment));
        }
    }
    
    // Set active segment
    if (!segments_.empty()) {
        active_segment_ = segments_.back().get();
    }
    
    return true;
}

bool LogManager::rotate_segment() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Close current active segment
    if (active_segment_) {
        active_segment_->close();
    }
    
    // Determine base offset for new segment
    uint64_t new_base_offset = 0;
    if (active_segment_) {
        new_base_offset = active_segment_->next_offset();
    }
    
    // Create new segment
    std::string filename = segment_filename(new_base_offset);
    auto segment = LogSegment::create(filename, new_base_offset);
    if (!segment) {
        return false;
    }
    
    active_segment_ = segment.get();
    segments_.push_back(std::move(segment));
    
    return true;
}

std::string LogManager::segment_filename(uint64_t base_offset) const {
    char filename[64];
    std::snprintf(filename, sizeof(filename), "%020llu.log", 
                  static_cast<unsigned long long>(base_offset));
    return (fs::path(config_.base_path) / filename).string();
}

uint64_t LogManager::append(Message& msg) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (!active_segment_) {
        return 0;
    }
    
    // Check if rotation needed
    if (active_segment_->should_rotate(config_.max_segment_bytes)) {
        // Close current active segment
        active_segment_->close();
        
        // Determine base offset for new segment
        uint64_t new_base_offset = active_segment_->next_offset();
        
        // Create new segment
        std::string filename = segment_filename(new_base_offset);
        auto segment = LogSegment::create(filename, new_base_offset);
        if (!segment) {
            return 0;
        }
        
        active_segment_ = segment.get();
        segments_.push_back(std::move(segment));
    }
    
    return active_segment_->append(msg);
}

LogSegment* LogManager::find_segment(uint64_t offset) const {
    // Search from newest to oldest (most common case is recent data)
    for (auto it = segments_.rbegin(); it != segments_.rend(); ++it) {
        auto& seg = *it;
        // Check if this segment contains the offset
        // A segment contains offset if: base_offset <= offset < next_offset
        if (offset >= seg->base_offset() && 
            (seg->is_closed() ? offset < seg->next_offset() : offset < seg->next_offset())) {
            return seg.get();
        }
    }
    return nullptr;
}

MessagePtr LogManager::read(uint64_t offset) const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    LogSegment* segment = find_segment(offset);
    if (!segment) {
        return nullptr;
    }
    
    return segment->read(offset);
}

std::vector<MessagePtr> LogManager::read_batch(uint64_t start_offset, size_t max_messages) const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::vector<MessagePtr> messages;
    messages.reserve(max_messages);
    
    uint64_t current_offset = start_offset;
    
    while (messages.size() < max_messages) {
        LogSegment* segment = find_segment(current_offset);
        if (!segment) {
            break;
        }
        
        size_t remaining = max_messages - messages.size();
        auto batch = segment->read_batch(current_offset, remaining);
        
        if (batch.empty()) {
            break;
        }
        
        for (auto& msg : batch) {
            messages.push_back(std::move(msg));
        }
        
        current_offset = segment->next_offset();
    }
    
    return messages;
}

bool LogManager::flush() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    bool success = true;
    for (auto& segment : segments_) {
        if (!segment->is_closed()) {
            success = segment->flush() && success;
        }
    }
    
    return success;
}

uint64_t LogManager::earliest_offset() const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (segments_.empty()) {
        return 0;
    }
    
    return segments_.front()->base_offset();
}

uint64_t LogManager::latest_offset() const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (!active_segment_) {
        return 0;
    }
    
    return active_segment_->next_offset();
}

void LogManager::apply_retention() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (segments_.size() <= 1) {
        return; // Keep at least one segment
    }
    
    auto now = std::chrono::system_clock::now();
    auto cutoff = now - std::chrono::milliseconds(config_.retention_ms);
    auto cutoff_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        cutoff.time_since_epoch()).count();
    
    // Find segments to delete (all messages older than cutoff)
    auto it = segments_.begin();
    while (it != segments_.end() && segments_.size() > 1) {
        // Check if entire segment is old
        // For simplicity, we check based on segment file modification time
        std::error_code ec;
        auto mtime = fs::last_write_time((*it)->path(), ec);
        if (ec) {
            ++it;
            continue;
        }
        
        // Convert to system_clock (C++20 would make this easier)
        // For now, skip time-based deletion and just keep recent segments
        // TODO: Implement proper time-based retention
        ++it;
    }
}

} // namespace stream
