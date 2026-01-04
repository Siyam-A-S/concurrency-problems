#include "topic.h"
#include "hash.h"
#include <iostream>
#include <cassert>
#include <filesystem>
#include <thread>
#include <atomic>
#include <set>

namespace fs = std::filesystem;

// =============================================================================
// Test Utilities
// =============================================================================

#define TEST(name) void test_##name()
#define RUN_TEST(name) do { \
    std::cout << "Running " #name "... "; \
    std::cout.flush(); \
    test_##name(); \
    std::cout << "PASSED" << std::endl; \
} while(0)

class TempDir {
public:
    TempDir() {
        path_ = fs::temp_directory_path() / ("topic_test_" + std::to_string(counter_++));
        fs::create_directories(path_);
    }
    
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path_, ec);
    }
    
    std::string path() const { return path_.string(); }
    
private:
    fs::path path_;
    static std::atomic<int> counter_;
};

std::atomic<int> TempDir::counter_{0};

// =============================================================================
// Hash Tests
// =============================================================================

TEST(hash_basic) {
    // Hash should be deterministic
    std::string key = "test-key";
    uint32_t h1 = stream::Hasher::hash(key);
    uint32_t h2 = stream::Hasher::hash(key);
    assert(h1 == h2);
}

TEST(hash_different_keys) {
    // Different keys should (usually) produce different hashes
    uint32_t h1 = stream::Hasher::hash(std::string("key1"));
    uint32_t h2 = stream::Hasher::hash(std::string("key2"));
    assert(h1 != h2);
}

TEST(hash_partition_distribution) {
    // Test partition distribution
    const uint32_t num_partitions = 4;
    std::vector<int> counts(num_partitions, 0);
    
    for (int i = 0; i < 1000; i++) {
        std::string key = "key-" + std::to_string(i);
        uint32_t partition = stream::Hasher::partition_for_key(key, num_partitions);
        assert(partition < num_partitions);
        counts[partition]++;
    }
    
    // Each partition should get at least some messages (rough distribution)
    for (int count : counts) {
        assert(count > 100);  // At least 10% each (250 expected)
    }
}

TEST(hash_empty_key) {
    // Empty key should go to partition 0
    uint32_t partition = stream::Hasher::partition_for_key(std::string(""), 4);
    assert(partition == 0);
}

TEST(hash_consistency) {
    // Same key should always map to same partition
    const std::string key = "consistent-key";
    uint32_t partition1 = stream::Hasher::partition_for_key(key, 8);
    uint32_t partition2 = stream::Hasher::partition_for_key(key, 8);
    assert(partition1 == partition2);
}

// =============================================================================
// Partition Tests
// =============================================================================

TEST(partition_create) {
    TempDir temp;
    
    stream::Partition::Config config;
    config.topic_name = "test-topic";
    config.partition_id = 0;
    config.base_path = temp.path();
    
    stream::Partition partition(config);
    
    assert(partition.topic_name() == "test-topic");
    assert(partition.partition_id() == 0);
    assert(partition.start_offset() == 0);
    // Partition was created successfully
}

TEST(partition_append_read) {
    TempDir temp;
    
    stream::Partition::Config config;
    config.topic_name = "test-topic";
    config.partition_id = 0;
    config.base_path = temp.path();
    
    stream::Partition partition(config);
    
    // Append a message
    auto msg = stream::make_message("key1", "value1");
    uint64_t offset = partition.append(*msg);
    assert(offset == 0);  // First message gets offset 0
    
    // Read it back
    auto read_msg = partition.read(offset);
    assert(read_msg != nullptr);
    assert(read_msg->key == "key1");
    assert(read_msg->value == "value1");
}

TEST(partition_append_batch) {
    TempDir temp;
    
    stream::Partition::Config config;
    config.topic_name = "test-topic";
    config.partition_id = 0;
    config.base_path = temp.path();
    
    stream::Partition partition(config);
    
    // Create batch
    std::vector<stream::Message> messages;
    for (int i = 0; i < 10; i++) {
        stream::Message msg;
        msg.key = "key-" + std::to_string(i);
        msg.value = "value-" + std::to_string(i);
        messages.push_back(std::move(msg));
    }
    
    uint64_t first_offset = partition.append_batch(messages);
    // First offset is 0 for a fresh partition - batch append succeeded
    assert(first_offset == 0);
    
    // Read all back
    auto read_msgs = partition.read_batch(first_offset, 10);
    assert(read_msgs.size() == 10);
    
    for (size_t i = 0; i < read_msgs.size(); i++) {
        assert(read_msgs[i]->key == "key-" + std::to_string(i));
    }
}

TEST(partition_stats) {
    TempDir temp;
    
    stream::Partition::Config config;
    config.topic_name = "test-topic";
    config.partition_id = 0;
    config.base_path = temp.path();
    
    stream::Partition partition(config);
    
    // Append some messages
    for (int i = 0; i < 5; i++) {
        auto msg = stream::make_message("key", "value");
        partition.append(*msg);
    }
    
    auto stats = partition.stats();
    assert(stats.messages_in == 5);
    assert(stats.bytes_in > 0);
}

TEST(partition_poll_immediate) {
    TempDir temp;
    
    stream::Partition::Config config;
    config.topic_name = "test-topic";
    config.partition_id = 0;
    config.base_path = temp.path();
    
    stream::Partition partition(config);
    
    // Append a message
    auto msg = stream::make_message("key", "value");
    uint64_t offset = partition.append(*msg);
    
    // Poll should return immediately
    auto messages = partition.poll(offset, 10, std::chrono::milliseconds(100));
    assert(messages.size() == 1);
}

TEST(partition_poll_timeout) {
    TempDir temp;
    
    stream::Partition::Config config;
    config.topic_name = "test-topic";
    config.partition_id = 0;
    config.base_path = temp.path();
    
    stream::Partition partition(config);
    
    // Poll with no messages should timeout
    auto start = std::chrono::steady_clock::now();
    auto messages = partition.poll(100, 10, std::chrono::milliseconds(50));
    auto elapsed = std::chrono::steady_clock::now() - start;
    
    assert(messages.empty());
    assert(elapsed >= std::chrono::milliseconds(40));  // Should have waited
}

// =============================================================================
// Topic Tests
// =============================================================================

TEST(topic_create) {
    TempDir temp;
    
    stream::Topic::Config config;
    config.name = "my-topic";
    config.num_partitions = 4;
    config.base_path = temp.path();
    
    stream::Topic topic(config);
    
    assert(topic.name() == "my-topic");
    assert(topic.num_partitions() == 4);
}

TEST(topic_produce_consume) {
    TempDir temp;
    
    stream::Topic::Config config;
    config.name = "my-topic";
    config.num_partitions = 4;
    config.base_path = temp.path();
    
    stream::Topic topic(config);
    
    // Produce a message
    auto msg = stream::make_message("key1", "value1");
    auto result = topic.produce(*msg);
    
    assert(result.success);
    // Note: offset 0 is valid for first message
    
    // Fetch it back
    auto fetch_result = topic.fetch(result.partition_id, result.offset, 10);
    assert(fetch_result.success);
    assert(fetch_result.messages.size() == 1);
    assert(fetch_result.messages[0]->key == "key1");
}

TEST(topic_key_routing) {
    TempDir temp;
    
    stream::Topic::Config config;
    config.name = "my-topic";
    config.num_partitions = 4;
    config.base_path = temp.path();
    
    stream::Topic topic(config);
    
    // Messages with same key should go to same partition
    std::string key = "consistent-key";
    
    stream::Message msg1;
    msg1.key = key;
    msg1.value = "value1";
    auto result1 = topic.produce(msg1);
    
    stream::Message msg2;
    msg2.key = key;
    msg2.value = "value2";
    auto result2 = topic.produce(msg2);
    
    assert(result1.success);
    assert(result2.success);
    assert(result1.partition_id == result2.partition_id);
}

TEST(topic_round_robin) {
    TempDir temp;
    
    stream::Topic::Config config;
    config.name = "my-topic";
    config.num_partitions = 4;
    config.base_path = temp.path();
    
    stream::Topic topic(config);
    
    // Messages without keys should use round-robin
    std::vector<uint32_t> partitions;
    
    for (int i = 0; i < 8; i++) {
        stream::Message msg;
        msg.key = "";  // Empty key
        msg.value = "value-" + std::to_string(i);
        auto result = topic.produce(msg);
        assert(result.success);
        partitions.push_back(result.partition_id);
    }
    
    // Should have distributed across partitions
    std::set<uint32_t> unique_partitions(partitions.begin(), partitions.end());
    assert(unique_partitions.size() > 1);  // At least 2 different partitions
}

TEST(topic_produce_batch) {
    TempDir temp;
    
    stream::Topic::Config config;
    config.name = "my-topic";
    config.num_partitions = 4;
    config.base_path = temp.path();
    
    stream::Topic topic(config);
    
    // Create batch with same key (should go to same partition)
    std::vector<stream::Message> messages;
    for (int i = 0; i < 10; i++) {
        stream::Message msg;
        msg.key = "batch-key";
        msg.value = "value-" + std::to_string(i);
        messages.push_back(std::move(msg));
    }
    
    auto results = topic.produce_batch(messages);
    assert(results.size() == 10);
    
    // All should succeed and go to same partition
    uint32_t partition = results[0].partition_id;
    for (const auto& r : results) {
        assert(r.success);
        assert(r.partition_id == partition);
    }
}

TEST(topic_fetch_all) {
    TempDir temp;
    
    stream::Topic::Config config;
    config.name = "my-topic";
    config.num_partitions = 2;
    config.base_path = temp.path();
    
    stream::Topic topic(config);
    
    // Produce to each partition explicitly
    stream::Message msg0;
    msg0.value = "partition-0";
    topic.produce(msg0, 0);
    
    stream::Message msg1;
    msg1.value = "partition-1";
    topic.produce(msg1, 1);
    
    // Fetch from all partitions
    auto start_offsets = topic.start_offsets();
    auto results = topic.fetch_all(start_offsets, 10);
    
    assert(results.size() == 2);
    // Each partition should have messages
    size_t total = 0;
    for (const auto& r : results) {
        assert(r.success);
        total += r.messages.size();
    }
    assert(total >= 2);
}

TEST(topic_stats) {
    TempDir temp;
    
    stream::Topic::Config config;
    config.name = "my-topic";
    config.num_partitions = 4;
    config.base_path = temp.path();
    
    stream::Topic topic(config);
    
    // Produce some messages
    for (int i = 0; i < 20; i++) {
        auto msg = stream::make_message("key-" + std::to_string(i), "value");
        topic.produce(*msg);
    }
    
    auto stats = topic.stats();
    assert(stats.name == "my-topic");
    assert(stats.num_partitions == 4);
    assert(stats.total_messages == 20);
    assert(stats.partition_stats.size() == 4);
}

// =============================================================================
// TopicManager Tests
// =============================================================================

TEST(manager_create_topic) {
    TempDir temp;
    
    stream::TopicManager::Config config;
    config.base_path = temp.path();
    config.default_num_partitions = 4;
    
    stream::TopicManager manager(config);
    
    auto* topic = manager.create_topic("test-topic");
    assert(topic != nullptr);
    assert(topic->name() == "test-topic");
    assert(topic->num_partitions() == 4);
}

TEST(manager_get_topic) {
    TempDir temp;
    
    stream::TopicManager::Config config;
    config.base_path = temp.path();
    
    stream::TopicManager manager(config);
    
    manager.create_topic("topic1");
    
    auto* topic = manager.get_topic("topic1");
    assert(topic != nullptr);
    assert(topic->name() == "topic1");
    
    auto* missing = manager.get_topic("missing");
    assert(missing == nullptr);
}

TEST(manager_list_topics) {
    TempDir temp;
    
    stream::TopicManager::Config config;
    config.base_path = temp.path();
    
    stream::TopicManager manager(config);
    
    manager.create_topic("topic-b");
    manager.create_topic("topic-a");
    manager.create_topic("topic-c");
    
    auto topics = manager.list_topics();
    assert(topics.size() == 3);
    assert(topics[0] == "topic-a");  // Should be sorted
    assert(topics[1] == "topic-b");
    assert(topics[2] == "topic-c");
}

TEST(manager_delete_topic) {
    TempDir temp;
    
    stream::TopicManager::Config config;
    config.base_path = temp.path();
    
    stream::TopicManager manager(config);
    
    manager.create_topic("to-delete");
    assert(manager.topic_exists("to-delete"));
    
    bool deleted = manager.delete_topic("to-delete");
    assert(deleted);
    assert(!manager.topic_exists("to-delete"));
}

TEST(manager_get_or_create) {
    TempDir temp;
    
    stream::TopicManager::Config config;
    config.base_path = temp.path();
    
    stream::TopicManager manager(config);
    
    // First call creates
    auto* topic1 = manager.get_or_create_topic("test", 2);
    assert(topic1 != nullptr);
    assert(topic1->num_partitions() == 2);
    
    // Second call returns existing
    auto* topic2 = manager.get_or_create_topic("test", 8);  // Different partition count ignored
    assert(topic2 == topic1);
}

TEST(manager_produce_consume) {
    TempDir temp;
    
    stream::TopicManager::Config config;
    config.base_path = temp.path();
    
    stream::TopicManager manager(config);
    
    auto* topic = manager.create_topic("events", 2);
    
    // Produce
    auto msg = stream::make_message("user-123", R"({"event":"click"})");
    auto result = topic->produce(*msg);
    assert(result.success);
    
    // Consume
    auto fetch = topic->fetch(result.partition_id, result.offset, 10);
    assert(fetch.success);
    assert(fetch.messages.size() == 1);
    assert(fetch.messages[0]->key == "user-123");
}

// =============================================================================
// Concurrent Tests
// =============================================================================

TEST(concurrent_produce) {
    TempDir temp;
    
    stream::Topic::Config config;
    config.name = "concurrent-topic";
    config.num_partitions = 4;
    config.base_path = temp.path();
    
    stream::Topic topic(config);
    
    const int num_threads = 4;
    const int messages_per_thread = 100;
    std::atomic<int> success_count{0};
    
    std::vector<std::thread> threads;
    for (int t = 0; t < num_threads; t++) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < messages_per_thread; i++) {
                auto msg = stream::make_message(
                    "key-" + std::to_string(t) + "-" + std::to_string(i),
                    "value"
                );
                auto result = topic.produce(*msg);
                if (result.success) {
                    success_count.fetch_add(1);
                }
            }
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    assert(success_count == num_threads * messages_per_thread);
    
    auto stats = topic.stats();
    assert(stats.total_messages == num_threads * messages_per_thread);
}

// =============================================================================
// Main
// =============================================================================

int main() {
    std::cout << "\n=== Topic & Partition Tests ===\n" << std::endl;
    
    std::cout << "--- Hash Tests ---" << std::endl;
    RUN_TEST(hash_basic);
    RUN_TEST(hash_different_keys);
    RUN_TEST(hash_partition_distribution);
    RUN_TEST(hash_empty_key);
    RUN_TEST(hash_consistency);
    
    std::cout << "\n--- Partition Tests ---" << std::endl;
    RUN_TEST(partition_create);
    RUN_TEST(partition_append_read);
    RUN_TEST(partition_append_batch);
    RUN_TEST(partition_stats);
    RUN_TEST(partition_poll_immediate);
    RUN_TEST(partition_poll_timeout);
    
    std::cout << "\n--- Topic Tests ---" << std::endl;
    RUN_TEST(topic_create);
    RUN_TEST(topic_produce_consume);
    RUN_TEST(topic_key_routing);
    RUN_TEST(topic_round_robin);
    RUN_TEST(topic_produce_batch);
    RUN_TEST(topic_fetch_all);
    RUN_TEST(topic_stats);
    
    std::cout << "\n--- TopicManager Tests ---" << std::endl;
    RUN_TEST(manager_create_topic);
    RUN_TEST(manager_get_topic);
    RUN_TEST(manager_list_topics);
    RUN_TEST(manager_delete_topic);
    RUN_TEST(manager_get_or_create);
    RUN_TEST(manager_produce_consume);
    
    std::cout << "\n--- Concurrent Tests ---" << std::endl;
    RUN_TEST(concurrent_produce);
    
    std::cout << "\n=== Results ===" << std::endl;
    std::cout << "All tests PASSED!" << std::endl;
    
    return 0;
}
