#pragma once

#include "../message.h"
#include <cstdint>
#include <string>
#include <vector>
#include <optional>
#include <memory>

namespace stream {
namespace network {

// =============================================================================
// Wire Protocol Constants
// =============================================================================

constexpr uint32_t PROTOCOL_VERSION = 1;
constexpr uint32_t MAGIC_NUMBER = 0x53545250;  // "STRP" in hex
constexpr size_t MAX_REQUEST_SIZE = 16 * 1024 * 1024;  // 16MB max
constexpr size_t HEADER_SIZE = 16;  // magic(4) + version(4) + type(4) + length(4)

// =============================================================================
// Request/Response Types
// =============================================================================

enum class RequestType : uint32_t {
    // Metadata
    API_VERSIONS        = 0,
    METADATA            = 1,
    
    // Topic management
    CREATE_TOPIC        = 10,
    DELETE_TOPIC        = 11,
    LIST_TOPICS         = 12,
    
    // Produce/Consume
    PRODUCE             = 20,
    FETCH               = 21,
    
    // Consumer groups
    JOIN_GROUP          = 30,
    LEAVE_GROUP         = 31,
    COMMIT_OFFSET       = 32,
    FETCH_OFFSET        = 33,
    
    // Connection
    HEARTBEAT           = 40,
    DISCONNECT          = 41
};

enum class ErrorCode : int16_t {
    NONE                    = 0,
    UNKNOWN                 = -1,
    INVALID_REQUEST         = -2,
    TOPIC_NOT_FOUND         = -3,
    PARTITION_NOT_FOUND     = -4,
    INVALID_OFFSET          = -5,
    TOPIC_ALREADY_EXISTS    = -6,
    GROUP_NOT_FOUND         = -7,
    CONSUMER_NOT_FOUND      = -8,
    TIMEOUT                 = -9,
    NETWORK_ERROR           = -10,
    PROTOCOL_ERROR          = -11,
    BROKER_NOT_AVAILABLE    = -12
};

// =============================================================================
// Protocol Header
// =============================================================================

struct ProtocolHeader {
    uint32_t magic{MAGIC_NUMBER};
    uint32_t version{PROTOCOL_VERSION};
    uint32_t type{0};
    uint32_t length{0};  // Length of payload after header
    
    static constexpr size_t size() { return HEADER_SIZE; }
    
    void serialize(uint8_t* buffer) const;
    static ProtocolHeader deserialize(const uint8_t* buffer);
    bool is_valid() const;
};

// =============================================================================
// Request Structures
// =============================================================================

struct CreateTopicRequest {
    std::string topic_name;
    uint32_t num_partitions{1};
    
    std::vector<uint8_t> serialize() const;
    static std::optional<CreateTopicRequest> deserialize(const uint8_t* data, size_t len);
};

struct DeleteTopicRequest {
    std::string topic_name;
    
    std::vector<uint8_t> serialize() const;
    static std::optional<DeleteTopicRequest> deserialize(const uint8_t* data, size_t len);
};

struct ProduceRequest {
    std::string topic_name;
    std::string key;
    std::string value;
    MessagePriority priority{MessagePriority::NORMAL};
    
    std::vector<uint8_t> serialize() const;
    static std::optional<ProduceRequest> deserialize(const uint8_t* data, size_t len);
};

struct FetchRequest {
    std::string topic_name;
    uint32_t partition{0};
    uint64_t offset{0};
    uint32_t max_messages{100};
    
    std::vector<uint8_t> serialize() const;
    static std::optional<FetchRequest> deserialize(const uint8_t* data, size_t len);
};

struct JoinGroupRequest {
    std::string group_id;
    std::string topic_name;
    
    std::vector<uint8_t> serialize() const;
    static std::optional<JoinGroupRequest> deserialize(const uint8_t* data, size_t len);
};

struct LeaveGroupRequest {
    std::string group_id;
    std::string consumer_id;
    
    std::vector<uint8_t> serialize() const;
    static std::optional<LeaveGroupRequest> deserialize(const uint8_t* data, size_t len);
};

struct CommitOffsetRequest {
    std::string group_id;
    std::string topic_name;
    uint32_t partition{0};
    uint64_t offset{0};
    
    std::vector<uint8_t> serialize() const;
    static std::optional<CommitOffsetRequest> deserialize(const uint8_t* data, size_t len);
};

struct FetchOffsetRequest {
    std::string group_id;
    std::string topic_name;
    uint32_t partition{0};
    
    std::vector<uint8_t> serialize() const;
    static std::optional<FetchOffsetRequest> deserialize(const uint8_t* data, size_t len);
};

// =============================================================================
// Response Structures
// =============================================================================

struct GenericResponse {
    ErrorCode error{ErrorCode::NONE};
    std::string error_message;
    
    std::vector<uint8_t> serialize() const;
    static std::optional<GenericResponse> deserialize(const uint8_t* data, size_t len);
};

struct CreateTopicResponse {
    ErrorCode error{ErrorCode::NONE};
    std::string error_message;
    
    std::vector<uint8_t> serialize() const;
    static std::optional<CreateTopicResponse> deserialize(const uint8_t* data, size_t len);
};

struct ListTopicsResponse {
    ErrorCode error{ErrorCode::NONE};
    std::vector<std::string> topics;
    
    std::vector<uint8_t> serialize() const;
    static std::optional<ListTopicsResponse> deserialize(const uint8_t* data, size_t len);
};

struct ProduceResponse {
    ErrorCode error{ErrorCode::NONE};
    std::string error_message;
    uint32_t partition{0};
    uint64_t offset{0};
    
    std::vector<uint8_t> serialize() const;
    static std::optional<ProduceResponse> deserialize(const uint8_t* data, size_t len);
};

struct FetchResponse {
    ErrorCode error{ErrorCode::NONE};
    std::string error_message;
    std::vector<MessagePtr> messages;
    uint64_t high_watermark{0};
    
    std::vector<uint8_t> serialize() const;
    static std::optional<FetchResponse> deserialize(const uint8_t* data, size_t len);
};

struct JoinGroupResponse {
    ErrorCode error{ErrorCode::NONE};
    std::string error_message;
    std::string consumer_id;
    std::vector<uint32_t> assigned_partitions;
    
    std::vector<uint8_t> serialize() const;
    static std::optional<JoinGroupResponse> deserialize(const uint8_t* data, size_t len);
};

struct FetchOffsetResponse {
    ErrorCode error{ErrorCode::NONE};
    uint64_t offset{0};
    
    std::vector<uint8_t> serialize() const;
    static std::optional<FetchOffsetResponse> deserialize(const uint8_t* data, size_t len);
};

struct MetadataResponse {
    ErrorCode error{ErrorCode::NONE};
    struct TopicMetadata {
        std::string name;
        uint32_t partition_count{0};
    };
    std::vector<TopicMetadata> topics;
    
    std::vector<uint8_t> serialize() const;
    static std::optional<MetadataResponse> deserialize(const uint8_t* data, size_t len);
};

// =============================================================================
// Protocol Helpers
// =============================================================================

class ProtocolCodec {
public:
    // Encode full message with header
    static std::vector<uint8_t> encode(RequestType type, const std::vector<uint8_t>& payload);
    
    // Decode header from buffer
    static std::optional<ProtocolHeader> decode_header(const uint8_t* data, size_t len);
    
    // String encoding helpers
    static void write_string(std::vector<uint8_t>& buffer, const std::string& str);
    static std::optional<std::string> read_string(const uint8_t*& data, size_t& remaining);
    
    // Integer encoding helpers (little-endian)
    static void write_u16(std::vector<uint8_t>& buffer, uint16_t value);
    static void write_u32(std::vector<uint8_t>& buffer, uint32_t value);
    static void write_u64(std::vector<uint8_t>& buffer, uint64_t value);
    static void write_i16(std::vector<uint8_t>& buffer, int16_t value);
    
    static std::optional<uint16_t> read_u16(const uint8_t*& data, size_t& remaining);
    static std::optional<uint32_t> read_u32(const uint8_t*& data, size_t& remaining);
    static std::optional<uint64_t> read_u64(const uint8_t*& data, size_t& remaining);
    static std::optional<int16_t> read_i16(const uint8_t*& data, size_t& remaining);
};

// =============================================================================
// Error code to string
// =============================================================================

const char* error_code_to_string(ErrorCode code);

} // namespace network
} // namespace stream
