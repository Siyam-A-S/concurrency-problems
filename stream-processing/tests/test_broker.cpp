#include "broker.h"
#include <iostream>
#include <thread>
#include <atomic>
#include <cassert>
#include <set>
#include <filesystem>

using namespace stream;

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
        path_ = fs::temp_directory_path() / ("broker_test_" + std::to_string(counter_++));
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
// Broker Tests
// =============================================================================

TEST(broker_create_topic) {
    TempDir temp;
    
    Broker broker(temp.path());
    assert(broker.create_topic("test-topic", 3));
    assert(broker.topic_exists("test-topic"));
    
    auto topics = broker.list_topics();
    assert(topics.size() == 1);
    assert(topics[0] == "test-topic");
}

TEST(broker_delete_topic) {
    TempDir temp;
    
    Broker broker(temp.path());
    broker.create_topic("to-delete", 2);
    assert(broker.topic_exists("to-delete"));
    
    assert(broker.delete_topic("to-delete"));
    assert(!broker.topic_exists("to-delete"));
}

TEST(broker_produce_consume) {
    TempDir temp;
    
    Broker broker(temp.path());
    broker.create_topic("events", 2);
    
    // Produce
    auto result = broker.produce("events", "key1", "value1");
    assert(result.success);
    assert(result.topic == "events");
    
    // Consume
    auto msg = broker.consume("events", result.partition, result.offset);
    assert(msg != nullptr);
    assert(msg->key == "key1");
    assert(msg->value == "value1");
}

TEST(broker_consume_batch) {
    TempDir temp;
    
    Broker broker(temp.path());
    broker.create_topic("batch-topic", 1);
    
    // Produce multiple messages to partition 0
    for (int i = 0; i < 10; i++) {
        broker.produce("batch-topic", "", "msg-" + std::to_string(i));
    }
    
    // Consume batch
    auto messages = broker.consume_batch("batch-topic", 0, 0, 5);
    assert(messages.size() == 5);
}

TEST(broker_get_stats) {
    TempDir temp;
    
    Broker broker(temp.path());
    broker.create_topic("stats-topic", 2);
    
    broker.produce("stats-topic", "k1", "v1");
    broker.produce("stats-topic", "k2", "v2");
    broker.produce("stats-topic", "k3", "v3");
    
    auto stats = broker.get_stats();
    assert(stats.topic_count == 1);
    assert(stats.total_messages == 3);
}

// =============================================================================
// Producer Tests
// =============================================================================

TEST(producer_send_single) {
    TempDir temp;
    
    auto broker = std::make_shared<Broker>(temp.path());
    broker->create_topic("prod-topic", 2);
    
    Producer producer(broker, "prod-topic");
    
    auto result = producer.send("my-key", "my-value");
    assert(result.success);
    assert(result.topic == "prod-topic");
}

TEST(producer_send_batch) {
    TempDir temp;
    
    auto broker = std::make_shared<Broker>(temp.path());
    broker->create_topic("batch-prod", 2);
    
    Producer producer(broker, "batch-prod");
    
    std::vector<std::pair<std::string, std::string>> messages = {
        {"k1", "v1"},
        {"k2", "v2"},
        {"k3", "v3"}
    };
    
    auto results = producer.send_batch(messages);
    assert(results.size() == 3);
    for (const auto& r : results) {
        assert(r.success);
    }
}

TEST(producer_async_send) {
    TempDir temp;
    
    auto broker = std::make_shared<Broker>(temp.path());
    broker->create_topic("async-topic", 2);
    
    Producer producer(broker, "async-topic");
    
    std::atomic<bool> callback_called{false};
    producer.send_async("key", "value", [&](const ProduceResult& result) {
        assert(result.success);
        callback_called = true;
    });
    
    producer.flush();
    assert(callback_called.load());
}

TEST(producer_buffering) {
    TempDir temp;
    
    auto broker = std::make_shared<Broker>(temp.path());
    broker->create_topic("buffer-topic", 1);
    
    Producer producer(broker, "buffer-topic");
    producer.set_batch_size(5);
    
    // Send 3 messages (below batch size)
    for (int i = 0; i < 3; i++) {
        producer.send_async("k", "v" + std::to_string(i), nullptr);
    }
    
    // Flush to ensure delivery
    producer.flush();
    
    auto stats = broker->get_stats();
    assert(stats.total_messages == 3);
}

// =============================================================================
// Consumer Tests
// =============================================================================

TEST(consumer_poll_single) {
    TempDir temp;
    
    auto broker = std::make_shared<Broker>(temp.path());
    broker->create_topic("poll-topic", 1);
    
    // Produce a message
    broker->produce("poll-topic", "k", "v");
    
    Consumer consumer(broker, "poll-topic", 0);
    
    auto msg = consumer.poll(std::chrono::milliseconds(100));
    assert(msg != nullptr);
    assert(msg->key == "k");
    assert(msg->value == "v");
}

TEST(consumer_poll_batch) {
    TempDir temp;
    
    auto broker = std::make_shared<Broker>(temp.path());
    broker->create_topic("poll-batch", 1);
    
    // Produce messages
    for (int i = 0; i < 10; i++) {
        broker->produce("poll-batch", "", "msg-" + std::to_string(i));
    }
    
    Consumer consumer(broker, "poll-batch", 0);
    
    auto messages = consumer.poll_batch(5, std::chrono::milliseconds(100));
    assert(messages.size() == 5);
    
    // Next batch
    messages = consumer.poll_batch(10, std::chrono::milliseconds(100));
    assert(messages.size() == 5);
}

TEST(consumer_commit_offset) {
    TempDir temp;
    
    auto broker = std::make_shared<Broker>(temp.path());
    broker->create_topic("commit-topic", 1);
    
    broker->produce("commit-topic", "", "msg1");
    broker->produce("commit-topic", "", "msg2");
    broker->produce("commit-topic", "", "msg3");
    
    {
        Consumer consumer(broker, "commit-topic", 0, "test-group");
        consumer.poll(std::chrono::milliseconds(100));  // msg1
        consumer.poll(std::chrono::milliseconds(100));  // msg2
        consumer.commit();  // Commit offset 2
    }
    
    // New consumer should start from committed offset
    {
        Consumer consumer(broker, "commit-topic", 0, "test-group");
        consumer.seek_to_committed();
        auto msg = consumer.poll(std::chrono::milliseconds(100));
        assert(msg != nullptr);
        assert(msg->value == "msg3");
    }
}

TEST(consumer_seek) {
    TempDir temp;
    
    auto broker = std::make_shared<Broker>(temp.path());
    broker->create_topic("seek-topic", 1);
    
    for (int i = 0; i < 5; i++) {
        broker->produce("seek-topic", "", "msg-" + std::to_string(i));
    }
    
    Consumer consumer(broker, "seek-topic", 0);
    
    // Seek to offset 3
    consumer.seek(3);
    auto msg = consumer.poll(std::chrono::milliseconds(100));
    assert(msg != nullptr);
    assert(msg->value == "msg-3");
    
    // Seek to beginning
    consumer.seek_to_beginning();
    msg = consumer.poll(std::chrono::milliseconds(100));
    assert(msg != nullptr);
    assert(msg->value == "msg-0");
}

TEST(consumer_auto_commit) {
    TempDir temp;
    
    auto broker = std::make_shared<Broker>(temp.path());
    broker->create_topic("auto-commit", 1);
    
    broker->produce("auto-commit", "", "m1");
    broker->produce("auto-commit", "", "m2");
    
    {
        Consumer consumer(broker, "auto-commit", 0, "auto-group");
        consumer.enable_auto_commit(std::chrono::milliseconds(10));
        
        consumer.poll(std::chrono::milliseconds(100));
        consumer.poll(std::chrono::milliseconds(100));
        
        // Wait for auto-commit
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    
    // Verify committed
    Consumer consumer(broker, "auto-commit", 0, "auto-group");
    consumer.seek_to_committed();
    assert(consumer.position() == 2);
}

// =============================================================================
// Consumer Group Tests
// =============================================================================

TEST(consumer_group_single_consumer) {
    TempDir temp;
    
    auto broker = std::make_shared<Broker>(temp.path());
    broker->create_topic("cg-topic", 4);
    
    ConsumerGroup group(broker, "my-group", "cg-topic");
    auto consumer_id = group.join();
    
    assert(!consumer_id.empty());
    
    auto partitions = group.get_assigned_partitions(consumer_id);
    assert(partitions.size() == 4);  // Single consumer gets all partitions
}

TEST(consumer_group_rebalance) {
    TempDir temp;
    
    auto broker = std::make_shared<Broker>(temp.path());
    broker->create_topic("rebalance-topic", 4);
    
    ConsumerGroup group(broker, "rebalance-group", "rebalance-topic");
    
    auto c1 = group.join();
    auto p1 = group.get_assigned_partitions(c1);
    assert(p1.size() == 4);
    
    auto c2 = group.join();
    auto p1_after = group.get_assigned_partitions(c1);
    auto p2 = group.get_assigned_partitions(c2);
    
    // Partitions should be split
    assert(p1_after.size() == 2);
    assert(p2.size() == 2);
    
    // No overlap
    std::set<uint32_t> all_partitions;
    all_partitions.insert(p1_after.begin(), p1_after.end());
    all_partitions.insert(p2.begin(), p2.end());
    assert(all_partitions.size() == 4);
}

TEST(consumer_group_leave) {
    TempDir temp;
    
    auto broker = std::make_shared<Broker>(temp.path());
    broker->create_topic("leave-topic", 4);
    
    ConsumerGroup group(broker, "leave-group", "leave-topic");
    
    auto c1 = group.join();
    auto c2 = group.join();
    
    // Both have 2 partitions
    assert(group.get_assigned_partitions(c1).size() == 2);
    assert(group.get_assigned_partitions(c2).size() == 2);
    
    // c2 leaves
    group.leave(c2);
    
    // c1 should now have all 4
    assert(group.get_assigned_partitions(c1).size() == 4);
    assert(group.get_assigned_partitions(c2).empty());
}

TEST(consumer_group_poll) {
    TempDir temp;
    
    auto broker = std::make_shared<Broker>(temp.path());
    broker->create_topic("cg-poll-topic", 2);
    
    // Produce to both partitions
    broker->produce("cg-poll-topic", "key-a", "val-a");
    broker->produce("cg-poll-topic", "key-b", "val-b");
    broker->produce("cg-poll-topic", "", "val-c");
    broker->produce("cg-poll-topic", "", "val-d");
    
    ConsumerGroup group(broker, "poll-group", "cg-poll-topic");
    auto consumer_id = group.join();
    
    // Poll all messages
    std::vector<MessagePtr> all_messages;
    for (int i = 0; i < 10; i++) {
        auto msgs = group.poll(consumer_id, 10, std::chrono::milliseconds(50));
        all_messages.insert(all_messages.end(), msgs.begin(), msgs.end());
        if (all_messages.size() >= 4) break;
    }
    
    assert(all_messages.size() == 4);
}

TEST(consumer_group_commit) {
    TempDir temp;
    
    auto broker = std::make_shared<Broker>(temp.path());
    broker->create_topic("cg-commit-topic", 1);
    
    broker->produce("cg-commit-topic", "", "m1");
    broker->produce("cg-commit-topic", "", "m2");
    broker->produce("cg-commit-topic", "", "m3");
    
    {
        ConsumerGroup group(broker, "commit-group", "cg-commit-topic");
        auto cid = group.join();
        
        group.poll(cid, 2, std::chrono::milliseconds(100));
        group.commit(cid);
    }
    
    // New group instance should resume
    {
        ConsumerGroup group(broker, "commit-group", "cg-commit-topic");
        auto cid = group.join();
        
        auto msgs = group.poll(cid, 10, std::chrono::milliseconds(100));
        assert(msgs.size() == 1);
        assert(msgs[0]->value == "m3");
    }
}

TEST(consumer_group_many_consumers) {
    TempDir temp;
    
    auto broker = std::make_shared<Broker>(temp.path());
    broker->create_topic("many-topic", 4);
    
    ConsumerGroup group(broker, "many-group", "many-topic");
    
    std::vector<std::string> consumers;
    for (int i = 0; i < 6; i++) {
        consumers.push_back(group.join());
    }
    
    // With 4 partitions and 6 consumers, 4 get 1 partition each, 2 get none
    int with_partitions = 0;
    int without_partitions = 0;
    
    for (const auto& c : consumers) {
        auto parts = group.get_assigned_partitions(c);
        if (parts.empty()) {
            without_partitions++;
        } else {
            with_partitions++;
            assert(parts.size() == 1);
        }
    }
    
    assert(with_partitions == 4);
    assert(without_partitions == 2);
}

// =============================================================================
// Integration Tests
// =============================================================================

TEST(integration_producer_consumer) {
    TempDir temp;
    
    auto broker = std::make_shared<Broker>(temp.path());
    broker->create_topic("integration", 2);
    
    Producer producer(broker, "integration");
    Consumer consumer0(broker, "integration", 0);
    Consumer consumer1(broker, "integration", 1);
    
    // Produce messages
    std::vector<ProduceResult> results;
    for (int i = 0; i < 20; i++) {
        results.push_back(producer.send("key-" + std::to_string(i), "value-" + std::to_string(i)));
    }
    
    // Consume from both partitions
    size_t total_consumed = 0;
    auto msgs0 = consumer0.poll_batch(20, std::chrono::milliseconds(100));
    auto msgs1 = consumer1.poll_batch(20, std::chrono::milliseconds(100));
    
    total_consumed = msgs0.size() + msgs1.size();
    assert(total_consumed == 20);
}

TEST(integration_multi_threaded_produce) {
    TempDir temp;
    
    auto broker = std::make_shared<Broker>(temp.path());
    broker->create_topic("mt-topic", 4);
    
    const int num_threads = 4;
    const int msgs_per_thread = 100;
    std::atomic<int> success_count{0};
    
    std::vector<std::thread> threads;
    for (int t = 0; t < num_threads; t++) {
        threads.emplace_back([&, t]() {
            Producer producer(broker, "mt-topic");
            for (int i = 0; i < msgs_per_thread; i++) {
                auto result = producer.send(
                    "thread-" + std::to_string(t),
                    "msg-" + std::to_string(i)
                );
                if (result.success) {
                    success_count++;
                }
            }
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    assert(success_count == num_threads * msgs_per_thread);
    
    auto stats = broker->get_stats();
    assert(stats.total_messages == static_cast<uint64_t>(num_threads * msgs_per_thread));
}

// =============================================================================
// Main
// =============================================================================

int main() {
    std::cout << "\n=== Broker Tests ===\n" << std::endl;
    
    std::cout << "--- Broker Core Tests ---" << std::endl;
    RUN_TEST(broker_create_topic);
    RUN_TEST(broker_delete_topic);
    RUN_TEST(broker_produce_consume);
    RUN_TEST(broker_consume_batch);
    RUN_TEST(broker_get_stats);
    
    std::cout << "\n--- Producer Tests ---" << std::endl;
    RUN_TEST(producer_send_single);
    RUN_TEST(producer_send_batch);
    RUN_TEST(producer_async_send);
    RUN_TEST(producer_buffering);
    
    std::cout << "\n--- Consumer Tests ---" << std::endl;
    RUN_TEST(consumer_poll_single);
    RUN_TEST(consumer_poll_batch);
    RUN_TEST(consumer_commit_offset);
    RUN_TEST(consumer_seek);
    RUN_TEST(consumer_auto_commit);
    
    std::cout << "\n--- Consumer Group Tests ---" << std::endl;
    RUN_TEST(consumer_group_single_consumer);
    RUN_TEST(consumer_group_rebalance);
    RUN_TEST(consumer_group_leave);
    RUN_TEST(consumer_group_poll);
    RUN_TEST(consumer_group_commit);
    RUN_TEST(consumer_group_many_consumers);
    
    std::cout << "\n--- Integration Tests ---" << std::endl;
    RUN_TEST(integration_producer_consumer);
    RUN_TEST(integration_multi_threaded_produce);
    
    std::cout << "\n=== Results ===" << std::endl;
    std::cout << "All tests PASSED!" << std::endl;
    
    return 0;
}
