/**
 * @file test_serialization.cpp
 * @brief Tests for message serialization and checksum
 */

#include <iostream>
#include <cstring>
#include <cassert>

#include "message.h"

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
        throw std::runtime_error("Assertion failed: " #a " != " #b); \
    } \
} while(0)

int failures = 0;

// =============================================================================
// CRC32 Checksum Tests
// =============================================================================

TEST(checksum_basic) {
    auto msg = make_message("key", "value");
    
    // Initially checksum is 0
    ASSERT_EQ(msg->checksum, 0u);
    
    // Compute checksum
    msg->compute_checksum();
    
    // Checksum should be non-zero for non-empty message
    ASSERT(msg->checksum != 0);
    
    // Verify should pass
    ASSERT(msg->verify_checksum());
}

TEST(checksum_consistency) {
    auto msg1 = make_message("key", "value");
    auto msg2 = make_message("key", "value");
    
    // Same content should produce same checksum
    msg1->compute_checksum();
    msg2->compute_checksum();
    
    ASSERT_EQ(msg1->checksum, msg2->checksum);
}

TEST(checksum_different_content) {
    auto msg1 = make_message("key1", "value");
    auto msg2 = make_message("key2", "value");
    
    msg1->compute_checksum();
    msg2->compute_checksum();
    
    // Different content should produce different checksum
    ASSERT(msg1->checksum != msg2->checksum);
}

TEST(checksum_detects_corruption) {
    auto msg = make_message("key", "value");
    msg->compute_checksum();
    
    // Corrupt the data
    msg->key = "corrupted";
    
    // Verify should fail
    ASSERT(!msg->verify_checksum());
}

TEST(checksum_empty_message) {
    auto msg = make_message("", "");
    msg->compute_checksum();
    
    ASSERT(msg->verify_checksum());
}

// =============================================================================
// Serialization Tests
// =============================================================================

TEST(serialize_basic) {
    auto msg = make_message("test-key", "test-value");
    msg->offset = 12345;
    msg->priority = MessagePriority::HIGH;
    msg->codec = CompressionCodec::LZ4;
    msg->compute_checksum();
    
    auto bytes = msg->serialize();
    
    // Header: 8 + 8 + 4 + 4 + 1 + 1 + 4 + 4 = 34 bytes (added partition)
    // Key: 8 bytes ("test-key")
    // Value: 10 bytes ("test-value")
    // Total: 52 bytes
    ASSERT_EQ(bytes.size(), 52u);
}

TEST(serialize_deserialize_roundtrip) {
    auto original = make_message("my-key", "my-value");
    original->offset = 42;
    original->timestamp_ms = 1234567890;
    original->priority = MessagePriority::CRITICAL;
    original->codec = CompressionCodec::ZSTD;
    original->compute_checksum();
    
    // Serialize
    auto bytes = original->serialize();
    
    // Deserialize
    auto restored = Message::deserialize(bytes.data(), bytes.size());
    
    ASSERT(restored != nullptr);
    ASSERT_EQ(restored->offset, original->offset);
    ASSERT_EQ(restored->timestamp_ms, original->timestamp_ms);
    ASSERT_EQ(restored->checksum, original->checksum);
    ASSERT_EQ(static_cast<int>(restored->priority), static_cast<int>(original->priority));
    ASSERT_EQ(static_cast<int>(restored->codec), static_cast<int>(original->codec));
    ASSERT_EQ(restored->key, original->key);
    ASSERT_EQ(restored->value, original->value);
    
    // Verify checksum still valid
    ASSERT(restored->verify_checksum());
}

TEST(serialize_empty_key) {
    auto msg = make_message("", "value-only");
    msg->compute_checksum();
    
    auto bytes = msg->serialize();
    auto restored = Message::deserialize(bytes.data(), bytes.size());
    
    ASSERT(restored != nullptr);
    ASSERT(restored->key.empty());
    ASSERT_EQ(restored->value, "value-only");
    ASSERT(restored->verify_checksum());
}

TEST(serialize_empty_value) {
    auto msg = make_message("key-only", "");
    msg->compute_checksum();
    
    auto bytes = msg->serialize();
    auto restored = Message::deserialize(bytes.data(), bytes.size());
    
    ASSERT(restored != nullptr);
    ASSERT_EQ(restored->key, "key-only");
    ASSERT(restored->value.empty());
    ASSERT(restored->verify_checksum());
}

TEST(serialize_empty_message) {
    auto msg = make_message("", "");
    msg->compute_checksum();
    
    auto bytes = msg->serialize();
    auto restored = Message::deserialize(bytes.data(), bytes.size());
    
    ASSERT(restored != nullptr);
    ASSERT(restored->key.empty());
    ASSERT(restored->value.empty());
    ASSERT(restored->verify_checksum());
}

TEST(serialize_large_payload) {
    // Create message with 1MB payload
    std::string large_value(1024 * 1024, 'X');
    auto msg = make_message("large", large_value);
    msg->compute_checksum();
    
    auto bytes = msg->serialize();
    auto restored = Message::deserialize(bytes.data(), bytes.size());
    
    ASSERT(restored != nullptr);
    ASSERT_EQ(restored->value.size(), large_value.size());
    ASSERT_EQ(restored->value, large_value);
    ASSERT(restored->verify_checksum());
}

TEST(serialize_binary_data) {
    // Create message with binary data (including null bytes)
    std::string binary_key = "key\x00with\x00nulls";
    binary_key.resize(15); // Ensure size is correct
    std::string binary_value;
    for (int i = 0; i < 256; ++i) {
        binary_value += static_cast<char>(i);
    }
    
    Message msg;
    msg.key = binary_key;
    msg.value = binary_value;
    msg.compute_checksum();
    
    auto bytes = msg.serialize();
    auto restored = Message::deserialize(bytes.data(), bytes.size());
    
    ASSERT(restored != nullptr);
    ASSERT_EQ(restored->key.size(), binary_key.size());
    ASSERT_EQ(restored->value.size(), binary_value.size());
    ASSERT(restored->verify_checksum());
}

// =============================================================================
// Deserialize Error Handling Tests
// =============================================================================

TEST(deserialize_null_data) {
    auto msg = Message::deserialize(nullptr, 100);
    ASSERT(msg == nullptr);
}

TEST(deserialize_zero_size) {
    uint8_t data[1] = {0};
    auto msg = Message::deserialize(data, 0);
    ASSERT(msg == nullptr);
}

TEST(deserialize_truncated_header) {
    // Less than minimum header size (30 bytes)
    uint8_t data[20] = {0};
    auto msg = Message::deserialize(data, 20);
    ASSERT(msg == nullptr);
}

TEST(deserialize_truncated_key) {
    auto original = make_message("key", "value");
    auto bytes = original->serialize();
    
    // Truncate in the middle of key
    auto msg = Message::deserialize(bytes.data(), 32); // Header + partial key
    ASSERT(msg == nullptr);
}

TEST(deserialize_truncated_value) {
    auto original = make_message("key", "value");
    auto bytes = original->serialize();
    
    // Truncate in the middle of value (remove last byte)
    auto msg = Message::deserialize(bytes.data(), bytes.size() - 1);
    ASSERT(msg == nullptr);
}

// =============================================================================
// Wire Format Compatibility Tests
// =============================================================================

TEST(wire_format_little_endian) {
    auto msg = make_message("k", "v");
    msg->offset = 0x0102030405060708ULL;
    msg->timestamp_ms = 0;
    msg->checksum = 0;
    msg->priority = MessagePriority::NORMAL;
    msg->codec = CompressionCodec::NONE;
    
    auto bytes = msg->serialize();
    
    // First 8 bytes should be offset in little-endian
    ASSERT_EQ(bytes[0], 0x08);
    ASSERT_EQ(bytes[1], 0x07);
    ASSERT_EQ(bytes[2], 0x06);
    ASSERT_EQ(bytes[3], 0x05);
    ASSERT_EQ(bytes[4], 0x04);
    ASSERT_EQ(bytes[5], 0x03);
    ASSERT_EQ(bytes[6], 0x02);
    ASSERT_EQ(bytes[7], 0x01);
}

TEST(priority_values_preserved) {
    for (auto prio : {MessagePriority::CRITICAL, MessagePriority::HIGH, 
                       MessagePriority::NORMAL, MessagePriority::LOW}) {
        auto msg = make_message("k", "v");
        msg->priority = prio;
        
        auto bytes = msg->serialize();
        auto restored = Message::deserialize(bytes.data(), bytes.size());
        
        ASSERT(restored != nullptr);
        ASSERT_EQ(static_cast<int>(restored->priority), static_cast<int>(prio));
    }
}

TEST(codec_values_preserved) {
    for (auto codec : {CompressionCodec::NONE, CompressionCodec::SNAPPY, 
                        CompressionCodec::LZ4, CompressionCodec::ZSTD}) {
        auto msg = make_message("k", "v");
        msg->codec = codec;
        
        auto bytes = msg->serialize();
        auto restored = Message::deserialize(bytes.data(), bytes.size());
        
        ASSERT(restored != nullptr);
        ASSERT_EQ(static_cast<int>(restored->codec), static_cast<int>(codec));
    }
}

// =============================================================================
// Main
// =============================================================================

int main() {
    std::cout << "\n=== Message Serialization Tests ===\n\n";
    
    std::cout << "--- CRC32 Checksum Tests ---\n";
    RUN_TEST(checksum_basic);
    RUN_TEST(checksum_consistency);
    RUN_TEST(checksum_different_content);
    RUN_TEST(checksum_detects_corruption);
    RUN_TEST(checksum_empty_message);
    
    std::cout << "\n--- Serialization Roundtrip Tests ---\n";
    RUN_TEST(serialize_basic);
    RUN_TEST(serialize_deserialize_roundtrip);
    RUN_TEST(serialize_empty_key);
    RUN_TEST(serialize_empty_value);
    RUN_TEST(serialize_empty_message);
    RUN_TEST(serialize_large_payload);
    RUN_TEST(serialize_binary_data);
    
    std::cout << "\n--- Deserialize Error Handling Tests ---\n";
    RUN_TEST(deserialize_null_data);
    RUN_TEST(deserialize_zero_size);
    RUN_TEST(deserialize_truncated_header);
    RUN_TEST(deserialize_truncated_key);
    RUN_TEST(deserialize_truncated_value);
    
    std::cout << "\n--- Wire Format Tests ---\n";
    RUN_TEST(wire_format_little_endian);
    RUN_TEST(priority_values_preserved);
    RUN_TEST(codec_values_preserved);
    
    std::cout << "\n=== Results ===\n";
    if (failures == 0) {
        std::cout << "All tests PASSED!\n\n";
        return 0;
    } else {
        std::cout << failures << " test(s) FAILED!\n\n";
        return 1;
    }
}
