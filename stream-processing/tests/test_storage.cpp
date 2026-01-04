/**
 * @file test_storage.cpp
 * @brief Tests for log segment and storage functionality
 */

#include <iostream>
#include <filesystem>
#include <cstring>
#include <cassert>

#include "message.h"
#include "log_segment.h"

namespace fs = std::filesystem;
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

#define ASSERT_EQ(a, b) do { \
    if ((a) != (b)) { \
        throw std::runtime_error("Assertion failed: " #a " == " #b); \
    } \
} while(0)

int failures = 0;

// Test directory
const std::string TEST_DIR = "/tmp/stream_test_storage";

// Cleanup helper
void cleanup_test_dir() {
    std::error_code ec;
    fs::remove_all(TEST_DIR, ec);
}

// RAII cleanup
struct TestGuard {
    TestGuard() { cleanup_test_dir(); }
    ~TestGuard() { cleanup_test_dir(); }
};

// =============================================================================
// LogSegment Tests
// =============================================================================

TEST(segment_create) {
    TestGuard guard;
    
    std::string path = TEST_DIR + "/test.log";
    auto segment = LogSegment::create(path, 0);
    
    ASSERT(segment != nullptr);
    ASSERT(fs::exists(path));
    ASSERT_EQ(segment->base_offset(), 0u);
    ASSERT_EQ(segment->next_offset(), 0u);
    ASSERT_EQ(segment->message_count(), 0u);
}

TEST(segment_create_with_base_offset) {
    TestGuard guard;
    
    std::string path = TEST_DIR + "/test.log";
    auto segment = LogSegment::create(path, 1000);
    
    ASSERT(segment != nullptr);
    ASSERT_EQ(segment->base_offset(), 1000u);
    ASSERT_EQ(segment->next_offset(), 1000u);
}

TEST(segment_append_single) {
    TestGuard guard;
    
    std::string path = TEST_DIR + "/test.log";
    auto segment = LogSegment::create(path, 0);
    ASSERT(segment != nullptr);
    
    Message msg("key1", "value1");
    uint64_t offset = segment->append(msg);
    
    ASSERT_EQ(offset, 0u);
    ASSERT_EQ(msg.offset, 0u);
    ASSERT_EQ(segment->next_offset(), 1u);
    ASSERT_EQ(segment->message_count(), 1u);
}

TEST(segment_append_multiple) {
    TestGuard guard;
    
    std::string path = TEST_DIR + "/test.log";
    auto segment = LogSegment::create(path, 100);
    ASSERT(segment != nullptr);
    
    for (int i = 0; i < 10; ++i) {
        Message msg("key" + std::to_string(i), "value" + std::to_string(i));
        uint64_t offset = segment->append(msg);
        ASSERT_EQ(offset, 100u + i);
    }
    
    ASSERT_EQ(segment->next_offset(), 110u);
    ASSERT_EQ(segment->message_count(), 10u);
}

TEST(segment_read_single) {
    TestGuard guard;
    
    std::string path = TEST_DIR + "/test.log";
    auto segment = LogSegment::create(path, 0);
    ASSERT(segment != nullptr);
    
    Message msg("test-key", "test-value");
    msg.priority = MessagePriority::HIGH;
    segment->append(msg);
    
    auto read_msg = segment->read(0);
    ASSERT(read_msg != nullptr);
    ASSERT_EQ(read_msg->key, "test-key");
    ASSERT_EQ(read_msg->value, "test-value");
    ASSERT(read_msg->priority == MessagePriority::HIGH);
    ASSERT(read_msg->verify_checksum());
}

TEST(segment_read_multiple) {
    TestGuard guard;
    
    std::string path = TEST_DIR + "/test.log";
    auto segment = LogSegment::create(path, 0);
    ASSERT(segment != nullptr);
    
    // Append messages
    for (int i = 0; i < 100; ++i) {
        Message msg("key" + std::to_string(i), "value" + std::to_string(i));
        segment->append(msg);
    }
    
    // Read and verify each
    for (int i = 0; i < 100; ++i) {
        auto msg = segment->read(i);
        ASSERT(msg != nullptr);
        ASSERT_EQ(msg->key, "key" + std::to_string(i));
        ASSERT_EQ(msg->value, "value" + std::to_string(i));
        ASSERT(msg->verify_checksum());
    }
}

TEST(segment_read_batch) {
    TestGuard guard;
    
    std::string path = TEST_DIR + "/test.log";
    auto segment = LogSegment::create(path, 0);
    ASSERT(segment != nullptr);
    
    // Append 50 messages
    for (int i = 0; i < 50; ++i) {
        Message msg("k" + std::to_string(i), "v" + std::to_string(i));
        segment->append(msg);
    }
    
    // Read batch from start
    auto batch = segment->read_batch(0, 10);
    ASSERT_EQ(batch.size(), 10u);
    ASSERT_EQ(batch[0]->key, "k0");
    ASSERT_EQ(batch[9]->key, "k9");
    
    // Read batch from middle
    batch = segment->read_batch(25, 10);
    ASSERT_EQ(batch.size(), 10u);
    ASSERT_EQ(batch[0]->key, "k25");
    ASSERT_EQ(batch[9]->key, "k34");
    
    // Read batch at end (partial)
    batch = segment->read_batch(45, 10);
    ASSERT_EQ(batch.size(), 5u);
}

TEST(segment_read_nonexistent) {
    TestGuard guard;
    
    std::string path = TEST_DIR + "/test.log";
    auto segment = LogSegment::create(path, 100);
    
    Message msg("key", "value");
    segment->append(msg);
    
    // Offset before segment
    ASSERT(segment->read(50) == nullptr);
    
    // Offset after segment
    ASSERT(segment->read(200) == nullptr);
}

TEST(segment_contains_offset) {
    TestGuard guard;
    
    std::string path = TEST_DIR + "/test.log";
    auto segment = LogSegment::create(path, 100);
    
    // Append 10 messages (offsets 100-109)
    for (int i = 0; i < 10; ++i) {
        Message msg("k", "v");
        segment->append(msg);
    }
    
    ASSERT(!segment->contains_offset(50));
    ASSERT(!segment->contains_offset(99));
    ASSERT(segment->contains_offset(100));
    ASSERT(segment->contains_offset(105));
    ASSERT(segment->contains_offset(109));
    ASSERT(!segment->contains_offset(110));
    ASSERT(!segment->contains_offset(200));
}

TEST(segment_reopen) {
    TestGuard guard;
    
    std::string path = TEST_DIR + "/test.log";
    
    // Create and write
    {
        auto segment = LogSegment::create(path, 50);
        ASSERT(segment != nullptr);
        
        for (int i = 0; i < 20; ++i) {
            Message msg("key" + std::to_string(i), "value" + std::to_string(i));
            segment->append(msg);
        }
        
        segment->close();
    }
    
    // Reopen and verify
    {
        auto segment = LogSegment::open(path);
        ASSERT(segment != nullptr);
        ASSERT_EQ(segment->base_offset(), 50u);
        ASSERT_EQ(segment->next_offset(), 70u);
        ASSERT_EQ(segment->message_count(), 20u);
        
        // Read messages
        for (int i = 0; i < 20; ++i) {
            auto msg = segment->read(50 + i);
            ASSERT(msg != nullptr);
            ASSERT_EQ(msg->key, "key" + std::to_string(i));
            ASSERT(msg->verify_checksum());
        }
    }
}

TEST(segment_large_messages) {
    TestGuard guard;
    
    std::string path = TEST_DIR + "/test.log";
    auto segment = LogSegment::create(path, 0);
    
    // Write large message (1MB)
    std::string large_value(1024 * 1024, 'X');
    Message msg("large", large_value);
    uint64_t offset = segment->append(msg);
    ASSERT_EQ(offset, 0u);
    
    // Read back
    auto read_msg = segment->read(0);
    ASSERT(read_msg != nullptr);
    ASSERT_EQ(read_msg->value.size(), large_value.size());
    ASSERT_EQ(read_msg->value, large_value);
    ASSERT(read_msg->verify_checksum());
}

TEST(segment_should_rotate) {
    TestGuard guard;
    
    std::string path = TEST_DIR + "/test.log";
    auto segment = LogSegment::create(path, 0);
    
    // Small segment limit for testing
    const size_t max_size = 1000;
    
    ASSERT(!segment->should_rotate(max_size));
    
    // Add messages until we exceed limit
    while (!segment->should_rotate(max_size)) {
        Message msg("key", "value with some content to increase size");
        segment->append(msg);
    }
    
    ASSERT(segment->should_rotate(max_size));
    ASSERT(segment->file_size() >= max_size);
}

// =============================================================================
// LogManager Tests
// =============================================================================

TEST(manager_create_empty) {
    TestGuard guard;
    
    LogManager::Config config;
    config.base_path = TEST_DIR + "/logs";
    
    LogManager manager(config);
    
    ASSERT_EQ(manager.earliest_offset(), 0u);
    ASSERT_EQ(manager.latest_offset(), 0u);
}

TEST(manager_append_read) {
    TestGuard guard;
    
    LogManager::Config config;
    config.base_path = TEST_DIR + "/logs";
    
    LogManager manager(config);
    
    // Append
    Message msg("test-key", "test-value");
    uint64_t offset = manager.append(msg);
    ASSERT_EQ(offset, 0u);
    
    // Read
    auto read_msg = manager.read(0);
    ASSERT(read_msg != nullptr);
    ASSERT_EQ(read_msg->key, "test-key");
    ASSERT_EQ(read_msg->value, "test-value");
}

TEST(manager_append_many) {
    TestGuard guard;
    
    LogManager::Config config;
    config.base_path = TEST_DIR + "/logs";
    
    LogManager manager(config);
    
    // Append 1000 messages
    for (int i = 0; i < 1000; ++i) {
        Message msg("key" + std::to_string(i), "value" + std::to_string(i));
        uint64_t offset = manager.append(msg);
        ASSERT_EQ(offset, static_cast<uint64_t>(i));
    }
    
    ASSERT_EQ(manager.earliest_offset(), 0u);
    ASSERT_EQ(manager.latest_offset(), 1000u);
    
    // Read random samples
    for (int offset : {0, 100, 500, 999}) {
        auto msg = manager.read(offset);
        ASSERT(msg != nullptr);
        ASSERT_EQ(msg->key, "key" + std::to_string(offset));
    }
}

TEST(manager_read_batch) {
    TestGuard guard;
    
    LogManager::Config config;
    config.base_path = TEST_DIR + "/logs";
    
    LogManager manager(config);
    
    // Append 100 messages
    for (int i = 0; i < 100; ++i) {
        Message msg("k" + std::to_string(i), "v" + std::to_string(i));
        manager.append(msg);
    }
    
    // Read batch
    auto batch = manager.read_batch(10, 20);
    ASSERT_EQ(batch.size(), 20u);
    ASSERT_EQ(batch[0]->key, "k10");
    ASSERT_EQ(batch[19]->key, "k29");
}

TEST(manager_segment_rotation) {
    TestGuard guard;
    
    LogManager::Config config;
    config.base_path = TEST_DIR + "/logs";
    config.max_segment_bytes = 1000; // Small segments for testing
    
    LogManager manager(config);
    
    // Append enough to trigger rotation
    for (int i = 0; i < 100; ++i) {
        Message msg("key" + std::to_string(i), 
                    "value with enough content to fill segments quickly " + std::to_string(i));
        manager.append(msg);
    }
    
    // Check multiple segment files exist
    int log_count = 0;
    for (const auto& entry : fs::directory_iterator(config.base_path)) {
        if (entry.path().extension() == ".log") {
            log_count++;
        }
    }
    
    ASSERT(log_count > 1); // Multiple segments created
    
    // Can still read all messages
    for (int i = 0; i < 100; ++i) {
        auto msg = manager.read(i);
        ASSERT(msg != nullptr);
        ASSERT_EQ(msg->key, "key" + std::to_string(i));
    }
}

TEST(manager_persistence) {
    TestGuard guard;
    
    LogManager::Config config;
    config.base_path = TEST_DIR + "/logs";
    
    // Create and write
    {
        LogManager manager(config);
        
        for (int i = 0; i < 50; ++i) {
            Message msg("key" + std::to_string(i), "value" + std::to_string(i));
            manager.append(msg);
        }
        
        manager.flush();
    }
    
    // Reopen and verify
    {
        LogManager manager(config);
        
        ASSERT_EQ(manager.earliest_offset(), 0u);
        ASSERT_EQ(manager.latest_offset(), 50u);
        
        for (int i = 0; i < 50; ++i) {
            auto msg = manager.read(i);
            ASSERT(msg != nullptr);
            ASSERT_EQ(msg->key, "key" + std::to_string(i));
        }
    }
}

// =============================================================================
// Main
// =============================================================================

int main() {
    std::cout << "\n=== Storage Tests ===\n\n";
    
    std::cout << "--- LogSegment Tests ---\n";
    RUN_TEST(segment_create);
    RUN_TEST(segment_create_with_base_offset);
    RUN_TEST(segment_append_single);
    RUN_TEST(segment_append_multiple);
    RUN_TEST(segment_read_single);
    RUN_TEST(segment_read_multiple);
    RUN_TEST(segment_read_batch);
    RUN_TEST(segment_read_nonexistent);
    RUN_TEST(segment_contains_offset);
    RUN_TEST(segment_reopen);
    RUN_TEST(segment_large_messages);
    RUN_TEST(segment_should_rotate);
    
    std::cout << "\n--- LogManager Tests ---\n";
    RUN_TEST(manager_create_empty);
    RUN_TEST(manager_append_read);
    RUN_TEST(manager_append_many);
    RUN_TEST(manager_read_batch);
    RUN_TEST(manager_segment_rotation);
    RUN_TEST(manager_persistence);
    
    std::cout << "\n=== Results ===\n";
    if (failures == 0) {
        std::cout << "All tests PASSED!\n\n";
        return 0;
    } else {
        std::cout << failures << " test(s) FAILED!\n\n";
        return 1;
    }
}
