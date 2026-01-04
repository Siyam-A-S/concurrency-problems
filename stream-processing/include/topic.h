#pragma once

#include "partition.h"
#include "hash.h"
#include <string>
#include <vector>
#include <memory>
#include <shared_mutex>
#include <unordered_map>
#include <functional>
#include <optional>

namespace stream {

// =============================================================================
// Topic - A named collection of partitions
// =============================================================================
// Topics provide logical grouping of related messages.
// Messages are distributed across partitions based on their key.
// Each partition maintains ordering; no ordering across partitions.

class Topic {
public:
    struct Config {
        std::string name;
        uint32_t num_partitions = 4;
        std::string base_path;
        
        // Per-partition settings (inherited)
        size_t segment_max_bytes = 100 * 1024 * 1024;
        size_t segment_max_messages = 1000000;
        size_t retention_bytes = 1024 * 1024 * 1024;
        uint64_t retention_ms = 7 * 24 * 3600 * 1000;
    };
    
    // Aggregated statistics
    struct Stats {
        std::string name;
        uint32_t num_partitions = 0;
        uint64_t total_messages = 0;
        uint64_t total_bytes = 0;
        std::vector<Partition::Stats> partition_stats;
    };
    
    // Result of a produce operation
    struct ProduceResult {
        bool success = false;
        uint32_t partition_id = 0;
        uint64_t offset = 0;
        std::string error;
    };
    
    // Result of a fetch operation
    struct FetchResult {
        bool success = false;
        std::vector<MessagePtr> messages;
        uint64_t next_offset = 0;
        std::string error;
    };

    explicit Topic(const Config& config);
    ~Topic() = default;
    
    // Non-copyable
    Topic(const Topic&) = delete;
    Topic& operator=(const Topic&) = delete;
    
    // --- Produce Operations ---
    
    // Produce a message to the topic
    // Partition is selected based on key (or round-robin if empty)
    ProduceResult produce(Message& msg);
    
    // Produce a message to a specific partition
    ProduceResult produce(Message& msg, uint32_t partition_id);
    
    // Produce a batch of messages with the same key
    std::vector<ProduceResult> produce_batch(std::vector<Message>& messages);
    
    // --- Fetch Operations ---
    
    // Fetch messages from a specific partition starting at offset
    FetchResult fetch(uint32_t partition_id, 
                      uint64_t offset, 
                      size_t max_messages) const;
    
    // Fetch messages from all partitions (fan-out)
    std::vector<FetchResult> fetch_all(
        const std::unordered_map<uint32_t, uint64_t>& offsets,
        size_t max_messages_per_partition) const;
    
    // --- Partition Management ---
    
    // Get partition count
    uint32_t num_partitions() const { return static_cast<uint32_t>(partitions_.size()); }
    
    // Get a specific partition
    Partition* partition(uint32_t id);
    const Partition* partition(uint32_t id) const;
    
    // Get partition for a key
    uint32_t partition_for_key(const std::string& key) const;
    
    // --- Metadata ---
    
    const std::string& name() const { return config_.name; }
    std::string path() const { return config_.base_path; }
    
    // Get aggregated statistics
    Stats stats() const;
    
    // Get start/end offsets for each partition
    std::unordered_map<uint32_t, uint64_t> start_offsets() const;
    std::unordered_map<uint32_t, uint64_t> end_offsets() const;
    
    // --- Maintenance ---
    
    bool flush();

private:
    // Select partition for a message
    uint32_t select_partition(const Message& msg) const;
    
    Config config_;
    std::vector<std::unique_ptr<Partition>> partitions_;
    
    // Round-robin counter for messages without keys
    mutable std::atomic<uint32_t> round_robin_counter_{0};
    
    // Protect partition access during initialization
    mutable std::shared_mutex mutex_;
};

// =============================================================================
// TopicManager - Manages multiple topics
// =============================================================================

class TopicManager {
public:
    struct Config {
        std::string base_path;
        
        // Default topic settings
        uint32_t default_num_partitions = 4;
        size_t default_segment_max_bytes = 100 * 1024 * 1024;
        size_t default_retention_bytes = 1024 * 1024 * 1024;
        uint64_t default_retention_ms = 7 * 24 * 3600 * 1000;
    };

    explicit TopicManager(const Config& config);
    ~TopicManager() = default;
    
    // Non-copyable
    TopicManager(const TopicManager&) = delete;
    TopicManager& operator=(const TopicManager&) = delete;
    
    // --- Topic Management ---
    
    // Create a new topic
    Topic* create_topic(const std::string& name, 
                        uint32_t num_partitions = 0);  // 0 = use default
    
    // Create topic with full config
    Topic* create_topic(const Topic::Config& config);
    
    // Get an existing topic
    Topic* get_topic(const std::string& name);
    const Topic* get_topic(const std::string& name) const;
    
    // Check if topic exists
    bool topic_exists(const std::string& name) const;
    
    // Delete a topic (removes files!)
    bool delete_topic(const std::string& name);
    
    // List all topic names
    std::vector<std::string> list_topics() const;
    
    // --- Convenience Methods ---
    
    // Get or create topic
    Topic* get_or_create_topic(const std::string& name, 
                               uint32_t num_partitions = 0);
    
    // --- Maintenance ---
    
    void flush_all();
    
    // Load existing topics from disk
    void load_topics();

private:
    std::string topic_path(const std::string& name) const;
    
    Config config_;
    std::unordered_map<std::string, std::unique_ptr<Topic>> topics_;
    mutable std::shared_mutex mutex_;
};

} // namespace stream
