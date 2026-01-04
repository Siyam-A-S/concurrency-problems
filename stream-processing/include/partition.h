#pragma once

#include "message.h"
#include "log_segment.h"
#include <string>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <atomic>
#include <functional>
#include <condition_variable>

namespace stream {

// =============================================================================
// Partition - A single ordered message stream
// =============================================================================
// Each partition is an independent, ordered sequence of messages.
// Messages within a partition have monotonically increasing offsets.
// Backed by LogManager for persistent storage.

class Partition {
public:
    struct Config {
        std::string topic_name;
        uint32_t partition_id = 0;
        std::string base_path;
        
        // Storage settings
        size_t segment_max_bytes = 100 * 1024 * 1024;  // 100MB per segment
        size_t segment_max_messages = 1000000;         // 1M messages per segment
        
        // Retention settings
        size_t retention_bytes = 1024 * 1024 * 1024;   // 1GB total retention
        uint64_t retention_ms = 7 * 24 * 3600 * 1000;  // 7 days
    };
    
    // Statistics
    struct Stats {
        uint64_t messages_in = 0;       // Total messages appended
        uint64_t messages_out = 0;      // Total messages read
        uint64_t bytes_in = 0;          // Total bytes written
        uint64_t bytes_out = 0;         // Total bytes read
        uint64_t oldest_offset = 0;     // Oldest available offset
        uint64_t newest_offset = 0;     // Latest offset (exclusive)
    };

    explicit Partition(const Config& config);
    ~Partition() = default;
    
    // Non-copyable, movable
    Partition(const Partition&) = delete;
    Partition& operator=(const Partition&) = delete;
    Partition(Partition&&) = default;
    Partition& operator=(Partition&&) = default;
    
    // --- Write Operations ---
    
    // Append a single message
    // Returns the assigned offset, or 0 on failure
    uint64_t append(Message& msg);
    
    // Append a batch of messages
    // Returns the first offset assigned, or 0 on failure
    uint64_t append_batch(std::vector<Message>& messages);
    
    // --- Read Operations ---
    
    // Read a single message at offset
    MessagePtr read(uint64_t offset) const;
    
    // Read a batch of messages starting at offset
    std::vector<MessagePtr> read_batch(uint64_t start_offset, size_t max_messages) const;
    
    // Read messages from offset, blocking until at least one is available
    // Returns empty if timeout expires
    std::vector<MessagePtr> poll(uint64_t start_offset, 
                                  size_t max_messages,
                                  std::chrono::milliseconds timeout) const;
    
    // --- Offset Management ---
    
    // Get the first available offset (oldest message)
    uint64_t start_offset() const;
    
    // Get the next offset to be assigned (one past the last message)
    uint64_t end_offset() const;
    
    // Check if offset is valid (in range [start, end))
    bool is_valid_offset(uint64_t offset) const;
    
    // --- Metadata ---
    
    const std::string& topic_name() const { return config_.topic_name; }
    uint32_t partition_id() const { return config_.partition_id; }
    std::string path() const { return config_.base_path; }
    
    // Get current statistics
    Stats stats() const;
    
    // --- Maintenance ---
    
    // Flush pending writes to disk
    bool flush();

private:
    Config config_;
    std::unique_ptr<LogManager> log_manager_;
    
    // Statistics (atomic for lock-free reads)
    mutable std::atomic<uint64_t> messages_in_{0};
    mutable std::atomic<uint64_t> messages_out_{0};
    mutable std::atomic<uint64_t> bytes_in_{0};
    mutable std::atomic<uint64_t> bytes_out_{0};
    
    // Condition variable for poll() blocking
    mutable std::mutex poll_mutex_;
    mutable std::condition_variable poll_cv_;
};

// =============================================================================
// PartitionIterator - Iterate through messages in a partition
// =============================================================================

class PartitionIterator {
public:
    PartitionIterator(const Partition& partition, uint64_t start_offset)
        : partition_(partition)
        , current_offset_(start_offset) {}
    
    // Check if more messages are available
    bool has_next() const {
        return current_offset_ < partition_.end_offset();
    }
    
    // Get the next message
    MessagePtr next() {
        if (!has_next()) return nullptr;
        auto msg = partition_.read(current_offset_);
        if (msg) {
            current_offset_++;
        }
        return msg;
    }
    
    // Get current offset
    uint64_t offset() const { return current_offset_; }
    
    // Seek to a specific offset
    void seek(uint64_t offset) { current_offset_ = offset; }
    
    // Reset to start of partition
    void reset() { current_offset_ = partition_.start_offset(); }

private:
    const Partition& partition_;
    uint64_t current_offset_;
};

} // namespace stream
