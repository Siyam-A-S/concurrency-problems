#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <memory>
#include <chrono>

namespace stream {

/**
 * @brief Message priority for selective dropping under backpressure
 * 
 * Used when message_loss_tolerance > 0.0
 * Lower values = higher priority (less likely to be dropped)
 */
enum class MessagePriority : uint8_t {
    CRITICAL = 0,   // Never drop (financial transactions, critical alerts)
    HIGH = 1,       // Drop only in extreme conditions
    NORMAL = 2,     // Default priority
    LOW = 3         // Drop first when under pressure
};

/**
 * @brief Message compression codec
 */
enum class CompressionCodec : uint8_t {
    NONE = 0,
    SNAPPY = 1,
    LZ4 = 2,
    ZSTD = 3
};

/**
 * @brief Core message structure - MVP version
 * 
 * Design decisions for MVP:
 * - Use std::string for key/value (simple, safe memory management)
 * - Use std::shared_ptr for ownership (thread-safe ref counting built-in)
 * - Keep metadata simple and focus on correctness over micro-optimization
 * 
 * Future optimization opportunities (profile first!):
 * - Cache-line alignment if profiling shows false sharing
 * - Custom allocator if profiling shows allocation bottleneck
 * - Zero-copy with memory pools if profiling shows copy overhead
 */
struct Message {
    // Message metadata
    uint64_t offset{0};              // Unique offset within partition
    uint64_t timestamp_ms{0};        // Unix timestamp in milliseconds
    uint32_t checksum{0};            // CRC32 for integrity check
    MessagePriority priority{MessagePriority::NORMAL};
    CompressionCodec codec{CompressionCodec::NONE};
    
    // Payload - using std::string for safe memory management
    std::string key;                 // Partition key for routing
    std::string value;               // Actual message payload
    
    // Default constructor
    Message() = default;
    
    // Convenient constructor
    Message(std::string k, std::string v, 
            MessagePriority p = MessagePriority::NORMAL)
        : timestamp_ms(current_time_ms())
        , priority(p)
        , key(std::move(k))
        , value(std::move(v)) {}
    
    /**
     * @brief Get total size of message payload
     */
    size_t total_size() const {
        return key.size() + value.size();
    }
    
    /**
     * @brief Get current time in milliseconds
     */
    static uint64_t current_time_ms() {
        auto now = std::chrono::system_clock::now();
        auto duration = now.time_since_epoch();
        return std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
    }
    
    /**
     * @brief Compute CRC32 checksum
     * Uses simple implementation - can be upgraded to hardware CRC32 later
     */
    void compute_checksum();
    
    /**
     * @brief Verify message integrity
     */
    bool verify_checksum() const;
    
    /**
     * @brief Serialize message to wire format
     * 
     * Wire format:
     * [8 bytes: offset][8 bytes: timestamp][4 bytes: checksum]
     * [1 byte: priority][1 byte: codec]
     * [4 bytes: key_size][N bytes: key]
     * [4 bytes: value_size][M bytes: value]
     */
    std::vector<uint8_t> serialize() const;
    
    /**
     * @brief Deserialize from wire format
     * Returns nullptr on failure
     */
    static std::shared_ptr<Message> deserialize(const uint8_t* data, size_t size);
};

// Use shared_ptr for thread-safe reference counting
using MessagePtr = std::shared_ptr<Message>;

/**
 * @brief Create a new message with shared ownership
 */
template<typename... Args>
MessagePtr make_message(Args&&... args) {
    return std::make_shared<Message>(std::forward<Args>(args)...);
}

/**
 * @brief Batch of messages for efficient processing
 * 
 * Batching benefits:
 * - Amortize system call overhead (single write vs many)
 * - Better compression ratios
 * - Reduced network round-trips
 * - Improved disk I/O patterns (sequential writes)
 * 
 * Trade-off: Higher latency for individual messages
 */
class MessageBatch {
public:
    explicit MessageBatch(size_t max_size = 100, 
                          size_t max_bytes = 1024 * 1024)  // 1MB default
        : max_size_(max_size)
        , max_bytes_(max_bytes) {
        messages_.reserve(max_size);
    }
    
    /**
     * @brief Add message to batch
     * Returns false if batch is full
     */
    bool add(MessagePtr msg) {
        if (is_full()) {
            return false;
        }
        
        if (messages_.empty()) {
            first_message_time_ms_ = Message::current_time_ms();
        }
        
        total_bytes_ += msg->total_size();
        messages_.push_back(std::move(msg));
        return true;
    }
    
    /**
     * @brief Check if batch is full (by count or bytes)
     */
    bool is_full() const {
        return messages_.size() >= max_size_ || total_bytes_ >= max_bytes_;
    }
    
    /**
     * @brief Check if batch should be flushed
     * 
     * Flush conditions:
     * 1. Batch is full (count or bytes)
     * 2. Timeout exceeded since first message
     */
    bool should_flush(uint32_t timeout_ms) const {
        if (messages_.empty()) return false;
        if (is_full()) return true;
        
        uint64_t now = Message::current_time_ms();
        return (now - first_message_time_ms_) >= timeout_ms;
    }
    
    /**
     * @brief Get messages and clear batch
     */
    std::vector<MessagePtr> flush() {
        auto result = std::move(messages_);
        messages_.clear();
        messages_.reserve(max_size_);
        total_bytes_ = 0;
        first_message_time_ms_ = 0;
        return result;
    }
    
    size_t size() const { return messages_.size(); }
    size_t total_bytes() const { return total_bytes_; }
    bool empty() const { return messages_.empty(); }
    
private:
    std::vector<MessagePtr> messages_;
    size_t max_size_;
    size_t max_bytes_;
    size_t total_bytes_{0};
    uint64_t first_message_time_ms_{0};
};

} // namespace stream
