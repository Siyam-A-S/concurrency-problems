#pragma once

#include "topic.h"
#include "message.h"
#include <string>
#include <memory>
#include <vector>
#include <map>
#include <mutex>
#include <atomic>
#include <thread>
#include <condition_variable>
#include <functional>
#include <chrono>
#include <filesystem>

namespace stream {

// ==================== Broker Stats ====================

struct BrokerStats {
    size_t topic_count{0};
    size_t partition_count{0};
    uint64_t total_messages{0};
    uint64_t total_bytes{0};
};

// ==================== Produce Result (Broker level) ====================

struct ProduceResult {
    bool success{false};
    std::string topic;
    uint32_t partition{0};
    uint64_t offset{0};
    std::chrono::system_clock::time_point timestamp;
    std::string error;
};

// ==================== Broker ====================

/**
 * Central broker that manages topics and handles produce/consume operations.
 * Thread-safe for concurrent access.
 */
class Broker {
public:
    explicit Broker(const std::filesystem::path& data_dir);
    ~Broker();
    
    // Disable copy
    Broker(const Broker&) = delete;
    Broker& operator=(const Broker&) = delete;
    
    // Topic management
    bool create_topic(const std::string& name, uint32_t num_partitions = 1);
    bool delete_topic(const std::string& name);
    bool topic_exists(const std::string& name) const;
    std::vector<std::string> list_topics() const;
    
    // Produce
    ProduceResult produce(const std::string& topic_name,
                          const std::string& key,
                          const std::string& value,
                          MessagePriority priority = MessagePriority::NORMAL);
    
    std::vector<ProduceResult> produce_batch(
        const std::string& topic_name,
        const std::vector<std::tuple<std::string, std::string, MessagePriority>>& messages);
    
    // Consume
    MessagePtr consume(const std::string& topic_name,
                       uint32_t partition,
                       uint64_t offset);
    
    std::vector<MessagePtr> consume_batch(const std::string& topic_name,
                                          uint32_t partition,
                                          uint64_t start_offset,
                                          size_t max_messages);
    
    // Offset management
    uint64_t get_latest_offset(const std::string& topic_name, uint32_t partition) const;
    uint32_t get_partition_count(const std::string& topic_name) const;
    
    // Consumer group offset management
    void commit_offset(const std::string& group_id,
                       const std::string& topic,
                       uint32_t partition,
                       uint64_t offset);
    
    uint64_t get_committed_offset(const std::string& group_id,
                                  const std::string& topic,
                                  uint32_t partition) const;
    
    // Stats
    BrokerStats get_stats() const;
    
    // Lifecycle
    void flush();
    void shutdown();

private:
    std::filesystem::path data_dir_;
    TopicManager topic_manager_;
    
    // Committed offsets: group_id -> topic -> partition -> offset
    mutable std::mutex offset_mutex_;
    std::map<std::string, std::map<std::string, std::map<uint32_t, uint64_t>>> committed_offsets_;
    
    std::atomic<bool> running_;
    
    void load_offsets();
    void save_offsets();
};

// ==================== Producer ====================

using ProduceCallback = std::function<void(const ProduceResult&)>;

/**
 * High-level producer client for sending messages to a topic.
 * Supports sync, async, and batched sends.
 */
class Producer {
public:
    Producer(std::shared_ptr<Broker> broker, const std::string& topic);
    ~Producer();
    
    // Disable copy
    Producer(const Producer&) = delete;
    Producer& operator=(const Producer&) = delete;
    
    // Synchronous send
    ProduceResult send(const std::string& key, const std::string& value,
                       MessagePriority priority = MessagePriority::NORMAL);
    
    // Batch send
    std::vector<ProduceResult> send_batch(
        const std::vector<std::pair<std::string, std::string>>& messages,
        MessagePriority priority = MessagePriority::NORMAL);
    
    // Asynchronous send with callback
    void send_async(const std::string& key, const std::string& value,
                    ProduceCallback callback,
                    MessagePriority priority = MessagePriority::NORMAL);
    
    // Flush pending messages
    void flush();
    
    // Configuration
    void set_batch_size(size_t size);
    void set_linger_ms(uint32_t ms);

private:
    std::shared_ptr<Broker> broker_;
    std::string topic_;
    
    // Batching
    struct PendingMessage {
        std::string key;
        std::string value;
        MessagePriority priority;
        ProduceCallback callback;
    };
    
    std::mutex buffer_mutex_;
    std::condition_variable buffer_cv_;
    std::vector<PendingMessage> buffer_;
    size_t batch_size_;
    uint32_t linger_ms_;
    
    std::thread flush_thread_;
    std::atomic<bool> running_;
    
    void flush_loop();
};

// ==================== Consumer ====================

/**
 * Consumer client for reading messages from a single partition.
 * Tracks offsets and supports auto-commit.
 */
class Consumer {
public:
    Consumer(std::shared_ptr<Broker> broker,
             const std::string& topic,
             uint32_t partition,
             const std::string& group_id = "");
    ~Consumer();
    
    // Disable copy
    Consumer(const Consumer&) = delete;
    Consumer& operator=(const Consumer&) = delete;
    
    // Poll for messages
    MessagePtr poll(std::chrono::milliseconds timeout);
    std::vector<MessagePtr> poll_batch(size_t max_messages,
                                       std::chrono::milliseconds timeout);
    
    // Offset management
    void commit();
    void commit(uint64_t offset);
    void seek(uint64_t offset);
    void seek_to_beginning();
    void seek_to_end();
    void seek_to_committed();
    uint64_t position() const;
    
    // Auto-commit
    void enable_auto_commit(std::chrono::milliseconds interval = std::chrono::milliseconds(5000));
    void disable_auto_commit();

private:
    std::shared_ptr<Broker> broker_;
    std::string topic_;
    uint32_t partition_;
    std::string group_id_;
    
    std::atomic<uint64_t> current_offset_;
    
    // Auto-commit
    std::atomic<bool> auto_commit_enabled_;
    uint64_t auto_commit_interval_ms_;
    std::thread auto_commit_thread_;
    std::atomic<bool> running_;
    
    void auto_commit_loop();
};

// ==================== Consumer Group ====================

/**
 * Consumer group for coordinated consumption across multiple consumers.
 * Automatically assigns partitions to consumers and handles rebalancing.
 */
class ConsumerGroup {
public:
    ConsumerGroup(std::shared_ptr<Broker> broker,
                  const std::string& group_id,
                  const std::string& topic);
    ~ConsumerGroup();
    
    // Disable copy
    ConsumerGroup(const ConsumerGroup&) = delete;
    ConsumerGroup& operator=(const ConsumerGroup&) = delete;
    
    // Membership
    std::string join();  // Returns consumer_id
    void leave(const std::string& consumer_id);
    
    // Get assigned partitions for a consumer
    std::vector<uint32_t> get_assigned_partitions(const std::string& consumer_id) const;
    
    // Poll messages from assigned partitions
    std::vector<MessagePtr> poll(const std::string& consumer_id,
                                 size_t max_messages,
                                 std::chrono::milliseconds timeout);
    
    // Commit offsets
    void commit(const std::string& consumer_id);
    void commit_all();

private:
    std::shared_ptr<Broker> broker_;
    std::string group_id_;
    std::string topic_;
    uint32_t partition_count_;
    
    mutable std::mutex mutex_;
    std::vector<std::string> members_;
    std::map<std::string, std::vector<uint32_t>> assignments_;
    std::map<uint32_t, uint64_t> partition_offsets_;
    
    void rebalance();
    std::string generate_consumer_id();
};

} // namespace stream
