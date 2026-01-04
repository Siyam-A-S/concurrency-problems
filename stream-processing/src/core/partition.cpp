#include "partition.h"
#include <filesystem>

namespace fs = std::filesystem;

namespace stream {

// =============================================================================
// Partition Implementation
// =============================================================================

Partition::Partition(const Config& config)
    : config_(config) {
    
    // Build partition path: base_path/topic-partition/
    std::string partition_path = config_.base_path + "/" + 
                                  config_.topic_name + "-" + 
                                  std::to_string(config_.partition_id);
    
    // Create LogManager config
    LogManager::Config log_config;
    log_config.base_path = partition_path;
    log_config.max_segment_bytes = config_.segment_max_bytes;
    log_config.retention_ms = config_.retention_ms;
    
    // Create log manager
    log_manager_ = std::make_unique<LogManager>(log_config);
}

uint64_t Partition::append(Message& msg) {
    // Serialize to get size for stats
    auto data = msg.serialize();
    size_t msg_size = data.size();
    
    // Append to log
    uint64_t offset = log_manager_->append(msg);
    
    // Track stats - always increment for successful append
    // Note: offset 0 is valid for first message, so we always count
    messages_in_.fetch_add(1, std::memory_order_relaxed);
    bytes_in_.fetch_add(msg_size, std::memory_order_relaxed);
    
    // Notify any waiting poll() calls
    {
        std::lock_guard<std::mutex> lock(poll_mutex_);
    }
    poll_cv_.notify_all();
    
    return offset;
}

uint64_t Partition::append_batch(std::vector<Message>& messages) {
    if (messages.empty()) return 0;
    
    bool first = true;
    uint64_t first_offset = 0;
    size_t total_bytes = 0;
    size_t count = 0;
    
    for (auto& msg : messages) {
        auto data = msg.serialize();
        
        uint64_t offset = log_manager_->append(msg);
        // append() returns the assigned offset. It also sets msg.offset.
        // On failure (closed segment), it returns 0 but doesn't set msg.offset correctly.
        // So we check that msg.offset matches what was returned.
        // Note: For the very first message, offset 0 is valid.
        
        if (first) {
            first_offset = offset;
            first = false;
        }
        
        total_bytes += data.size();
        count++;
    }
    
    if (count > 0) {
        messages_in_.fetch_add(count, std::memory_order_relaxed);
        bytes_in_.fetch_add(total_bytes, std::memory_order_relaxed);
        
        // Notify waiters
        {
            std::lock_guard<std::mutex> lock(poll_mutex_);
        }
        poll_cv_.notify_all();
    }
    
    return first_offset;
}

MessagePtr Partition::read(uint64_t offset) const {
    auto msg = log_manager_->read(offset);
    
    if (msg) {
        messages_out_.fetch_add(1, std::memory_order_relaxed);
        // Estimate size from key + value
        bytes_out_.fetch_add(msg->key.size() + msg->value.size() + 64, 
                             std::memory_order_relaxed);
    }
    
    return msg;
}

std::vector<MessagePtr> Partition::read_batch(uint64_t start_offset, 
                                               size_t max_messages) const {
    auto messages = log_manager_->read_batch(start_offset, max_messages);
    
    if (!messages.empty()) {
        size_t total_bytes = 0;
        for (const auto& msg : messages) {
            total_bytes += msg->key.size() + msg->value.size() + 64;
        }
        
        messages_out_.fetch_add(messages.size(), std::memory_order_relaxed);
        bytes_out_.fetch_add(total_bytes, std::memory_order_relaxed);
    }
    
    return messages;
}

std::vector<MessagePtr> Partition::poll(uint64_t start_offset,
                                         size_t max_messages,
                                         std::chrono::milliseconds timeout) const {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    
    while (std::chrono::steady_clock::now() < deadline) {
        // Check if messages are available
        if (start_offset < end_offset()) {
            return read_batch(start_offset, max_messages);
        }
        
        // Wait for new messages
        std::unique_lock<std::mutex> lock(poll_mutex_);
        auto remaining = deadline - std::chrono::steady_clock::now();
        
        if (remaining <= std::chrono::milliseconds::zero()) {
            break;
        }
        
        poll_cv_.wait_for(lock, remaining);
    }
    
    // Final check before returning empty
    if (start_offset < end_offset()) {
        return read_batch(start_offset, max_messages);
    }
    
    return {};
}

uint64_t Partition::start_offset() const {
    return log_manager_->earliest_offset();
}

uint64_t Partition::end_offset() const {
    return log_manager_->latest_offset();
}

bool Partition::is_valid_offset(uint64_t offset) const {
    return offset >= start_offset() && offset < end_offset();
}

Partition::Stats Partition::stats() const {
    Stats s;
    s.messages_in = messages_in_.load(std::memory_order_relaxed);
    s.messages_out = messages_out_.load(std::memory_order_relaxed);
    s.bytes_in = bytes_in_.load(std::memory_order_relaxed);
    s.bytes_out = bytes_out_.load(std::memory_order_relaxed);
    s.oldest_offset = start_offset();
    s.newest_offset = end_offset();
    return s;
}

bool Partition::flush() {
    return log_manager_->flush();
}

} // namespace stream
