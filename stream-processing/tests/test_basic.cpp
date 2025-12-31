/**
 * @file test_basic.cpp
 * @brief Basic tests for the stream processing MVP
 * 
 * Run with: ./test_basic
 * 
 * These tests verify the core components work correctly:
 * - Message creation and manipulation
 * - Thread-safe queue operations
 * - Object pool functionality
 * - Configuration management
 */

#include <iostream>
#include <thread>
#include <vector>
#include <cassert>
#include <chrono>
#include <atomic>

#include "message.h"
#include "lock_free_queue.h"
#include "object_pool.h"
#include "config.h"

using namespace stream;

// Simple test framework
#define TEST(name) void test_##name()
#define RUN_TEST(name) do { \
    std::cout << "Running " #name "... "; \
    try { \
        test_##name(); \
        std::cout << "PASSED" << std::endl; \
    } catch (const std::exception& e) { \
        std::cout << "FAILED: " << e.what() << std::endl; \
        failures++; \
    } \
} while(0)

#define ASSERT(condition) do { \
    if (!(condition)) { \
        throw std::runtime_error("Assertion failed: " #condition); \
    } \
} while(0)

int failures = 0;

// =============================================================================
// Message Tests
// =============================================================================

TEST(message_creation) {
    auto msg = make_message("test-key", "test-value");
    
    ASSERT(msg != nullptr);
    ASSERT(msg->key == "test-key");
    ASSERT(msg->value == "test-value");
    ASSERT(msg->priority == MessagePriority::NORMAL);
    ASSERT(msg->total_size() == 18); // "test-key" + "test-value"
    ASSERT(msg->timestamp_ms > 0);
}

TEST(message_with_priority) {
    auto msg = make_message("key", "value", MessagePriority::CRITICAL);
    
    ASSERT(msg->priority == MessagePriority::CRITICAL);
}

TEST(message_batch) {
    MessageBatch batch(3, 1024);
    
    ASSERT(batch.empty());
    ASSERT(!batch.is_full());
    
    batch.add(make_message("k1", "v1"));
    batch.add(make_message("k2", "v2"));
    
    ASSERT(batch.size() == 2);
    ASSERT(!batch.is_full());
    
    batch.add(make_message("k3", "v3"));
    
    ASSERT(batch.is_full()); // Reached max_size of 3
    
    auto messages = batch.flush();
    ASSERT(messages.size() == 3);
    ASSERT(batch.empty());
}

// =============================================================================
// Queue Tests
// =============================================================================

TEST(queue_basic_operations) {
    ThreadSafeQueue<int> queue(10);
    
    ASSERT(queue.empty());
    ASSERT(queue.try_enqueue(42));
    ASSERT(!queue.empty());
    ASSERT(queue.size() == 1);
    
    int value;
    ASSERT(queue.dequeue(value));
    ASSERT(value == 42);
    ASSERT(queue.empty());
}

TEST(queue_capacity) {
    ThreadSafeQueue<int> queue(3);
    
    ASSERT(queue.try_enqueue(1));
    ASSERT(queue.try_enqueue(2));
    ASSERT(queue.try_enqueue(3));
    ASSERT(!queue.try_enqueue(4)); // Queue full
    
    int value;
    ASSERT(queue.dequeue(value));
    ASSERT(queue.try_enqueue(4)); // Now has space
}

TEST(queue_with_messages) {
    ThreadSafeQueue<MessagePtr> queue(100);
    
    auto msg1 = make_message("key1", "value1");
    auto msg2 = make_message("key2", "value2");
    
    ASSERT(queue.try_enqueue(msg1));
    ASSERT(queue.try_enqueue(msg2));
    
    MessagePtr received;
    ASSERT(queue.dequeue(received));
    ASSERT(received->key == "key1");
    
    ASSERT(queue.dequeue(received));
    ASSERT(received->key == "key2");
}

TEST(queue_multithreaded) {
    ThreadSafeQueue<int> queue(1000);
    const int num_items = 100;
    std::atomic<int> sum{0};
    
    // Producer thread
    std::thread producer([&]() {
        for (int i = 1; i <= num_items; ++i) {
            queue.enqueue(i);
        }
    });
    
    // Consumer thread
    std::thread consumer([&]() {
        int received = 0;
        while (received < num_items) {
            int value;
            if (queue.dequeue_for(value, std::chrono::milliseconds(100))) {
                sum += value;
                received++;
            }
        }
    });
    
    producer.join();
    consumer.join();
    
    // Sum of 1 to 100 = 5050
    ASSERT(sum == 5050);
}

// =============================================================================
// Object Pool Tests
// =============================================================================

TEST(pool_basic_operations) {
    auto pool = make_object_pool<Message>(10);
    
    auto msg = pool->acquire("pooled-key", "pooled-value");
    ASSERT(msg != nullptr);
    ASSERT(msg->key == "pooled-key");
    
    auto stats = pool->get_stats();
    ASSERT(stats.total_acquisitions == 1);
}

TEST(pool_reuse) {
    auto pool = make_object_pool<Message>(10);
    
    // Acquire and release several times
    for (int i = 0; i < 5; ++i) {
        auto msg = pool->acquire("key", "value");
        // msg goes out of scope, returned to pool
    }
    
    auto stats = pool->get_stats();
    // First was new, rest should be reused
    ASSERT(stats.hit_rate() > 0.5);
}

// =============================================================================
// Config Tests
// =============================================================================

TEST(config_singleton) {
    auto& manager1 = ConfigManager::instance();
    auto& manager2 = ConfigManager::instance();
    
    // Same instance
    ASSERT(&manager1 == &manager2);
}

TEST(config_read_write) {
    auto& manager = ConfigManager::instance();
    
    // Read default config
    auto config = manager.get_config();
    ASSERT(config.port == 9092);
    
    // Update config
    manager.update([](BrokerConfig& cfg) {
        cfg.port = 9999;
    });
    
    // Read updated config
    config = manager.get_config();
    ASSERT(config.port == 9999);
    
    // Restore default
    manager.update([](BrokerConfig& cfg) {
        cfg.port = 9092;
    });
}

TEST(config_concurrent_access) {
    auto& manager = ConfigManager::instance();
    std::atomic<int> reads{0};
    std::atomic<int> writes{0};
    
    // Multiple reader threads
    std::vector<std::thread> readers;
    for (int i = 0; i < 4; ++i) {
        readers.emplace_back([&]() {
            for (int j = 0; j < 100; ++j) {
                auto config = manager.get_config();
                (void)config.port; // Use the value
                reads++;
            }
        });
    }
    
    // Single writer thread
    std::thread writer([&]() {
        for (int j = 0; j < 10; ++j) {
            manager.update([j](BrokerConfig& cfg) {
                cfg.max_connections = 10000 + j;
            });
            writes++;
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    });
    
    for (auto& t : readers) t.join();
    writer.join();
    
    ASSERT(reads == 400);
    ASSERT(writes == 10);
}

// =============================================================================
// Main
// =============================================================================

int main() {
    std::cout << "\n=== Stream Processing MVP Tests ===\n\n";
    
    std::cout << "--- Message Tests ---\n";
    RUN_TEST(message_creation);
    RUN_TEST(message_with_priority);
    RUN_TEST(message_batch);
    
    std::cout << "\n--- Queue Tests ---\n";
    RUN_TEST(queue_basic_operations);
    RUN_TEST(queue_capacity);
    RUN_TEST(queue_with_messages);
    RUN_TEST(queue_multithreaded);
    
    std::cout << "\n--- Object Pool Tests ---\n";
    RUN_TEST(pool_basic_operations);
    RUN_TEST(pool_reuse);
    
    std::cout << "\n--- Config Tests ---\n";
    RUN_TEST(config_singleton);
    RUN_TEST(config_read_write);
    RUN_TEST(config_concurrent_access);
    
    std::cout << "\n=== Results ===\n";
    if (failures == 0) {
        std::cout << "All tests PASSED!\n\n";
        return 0;
    } else {
        std::cout << failures << " test(s) FAILED!\n\n";
        return 1;
    }
}
