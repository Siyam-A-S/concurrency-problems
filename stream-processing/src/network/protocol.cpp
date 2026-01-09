#include "network/protocol.h"
#include <cstring>
#include <algorithm>

namespace stream {
namespace network {

// =============================================================================
// Protocol Header
// =============================================================================

void ProtocolHeader::serialize(uint8_t* buffer) const {
    std::memcpy(buffer, &magic, 4);
    std::memcpy(buffer + 4, &version, 4);
    std::memcpy(buffer + 8, &type, 4);
    std::memcpy(buffer + 12, &length, 4);
}

ProtocolHeader ProtocolHeader::deserialize(const uint8_t* buffer) {
    ProtocolHeader header;
    std::memcpy(&header.magic, buffer, 4);
    std::memcpy(&header.version, buffer + 4, 4);
    std::memcpy(&header.type, buffer + 8, 4);
    std::memcpy(&header.length, buffer + 12, 4);
    return header;
}

bool ProtocolHeader::is_valid() const {
    return magic == MAGIC_NUMBER && version == PROTOCOL_VERSION && length <= MAX_REQUEST_SIZE;
}

// =============================================================================
// ProtocolCodec - Integer Helpers
// =============================================================================

void ProtocolCodec::write_u16(std::vector<uint8_t>& buffer, uint16_t value) {
    buffer.push_back(static_cast<uint8_t>(value & 0xFF));
    buffer.push_back(static_cast<uint8_t>((value >> 8) & 0xFF));
}

void ProtocolCodec::write_u32(std::vector<uint8_t>& buffer, uint32_t value) {
    buffer.push_back(static_cast<uint8_t>(value & 0xFF));
    buffer.push_back(static_cast<uint8_t>((value >> 8) & 0xFF));
    buffer.push_back(static_cast<uint8_t>((value >> 16) & 0xFF));
    buffer.push_back(static_cast<uint8_t>((value >> 24) & 0xFF));
}

void ProtocolCodec::write_u64(std::vector<uint8_t>& buffer, uint64_t value) {
    for (int i = 0; i < 8; i++) {
        buffer.push_back(static_cast<uint8_t>((value >> (i * 8)) & 0xFF));
    }
}

void ProtocolCodec::write_i16(std::vector<uint8_t>& buffer, int16_t value) {
    write_u16(buffer, static_cast<uint16_t>(value));
}

std::optional<uint16_t> ProtocolCodec::read_u16(const uint8_t*& data, size_t& remaining) {
    if (remaining < 2) return std::nullopt;
    uint16_t value = static_cast<uint16_t>(data[0]) |
                     (static_cast<uint16_t>(data[1]) << 8);
    data += 2;
    remaining -= 2;
    return value;
}

std::optional<uint32_t> ProtocolCodec::read_u32(const uint8_t*& data, size_t& remaining) {
    if (remaining < 4) return std::nullopt;
    uint32_t value = static_cast<uint32_t>(data[0]) |
                     (static_cast<uint32_t>(data[1]) << 8) |
                     (static_cast<uint32_t>(data[2]) << 16) |
                     (static_cast<uint32_t>(data[3]) << 24);
    data += 4;
    remaining -= 4;
    return value;
}

std::optional<uint64_t> ProtocolCodec::read_u64(const uint8_t*& data, size_t& remaining) {
    if (remaining < 8) return std::nullopt;
    uint64_t value = 0;
    for (int i = 0; i < 8; i++) {
        value |= static_cast<uint64_t>(data[i]) << (i * 8);
    }
    data += 8;
    remaining -= 8;
    return value;
}

std::optional<int16_t> ProtocolCodec::read_i16(const uint8_t*& data, size_t& remaining) {
    auto val = read_u16(data, remaining);
    if (!val) return std::nullopt;
    return static_cast<int16_t>(*val);
}

// =============================================================================
// ProtocolCodec - String Helpers
// =============================================================================

void ProtocolCodec::write_string(std::vector<uint8_t>& buffer, const std::string& str) {
    write_u32(buffer, static_cast<uint32_t>(str.size()));
    buffer.insert(buffer.end(), str.begin(), str.end());
}

std::optional<std::string> ProtocolCodec::read_string(const uint8_t*& data, size_t& remaining) {
    auto len = read_u32(data, remaining);
    if (!len) return std::nullopt;
    if (remaining < *len) return std::nullopt;
    
    std::string str(reinterpret_cast<const char*>(data), *len);
    data += *len;
    remaining -= *len;
    return str;
}

// =============================================================================
// ProtocolCodec - Message Encoding
// =============================================================================

std::vector<uint8_t> ProtocolCodec::encode(RequestType type, const std::vector<uint8_t>& payload) {
    std::vector<uint8_t> result;
    result.reserve(HEADER_SIZE + payload.size());
    
    ProtocolHeader header;
    header.type = static_cast<uint32_t>(type);
    header.length = static_cast<uint32_t>(payload.size());
    
    result.resize(HEADER_SIZE);
    header.serialize(result.data());
    
    result.insert(result.end(), payload.begin(), payload.end());
    return result;
}

std::optional<ProtocolHeader> ProtocolCodec::decode_header(const uint8_t* data, size_t len) {
    if (len < HEADER_SIZE) return std::nullopt;
    
    auto header = ProtocolHeader::deserialize(data);
    if (!header.is_valid()) return std::nullopt;
    
    return header;
}

// =============================================================================
// CreateTopicRequest
// =============================================================================

std::vector<uint8_t> CreateTopicRequest::serialize() const {
    std::vector<uint8_t> buffer;
    ProtocolCodec::write_string(buffer, topic_name);
    ProtocolCodec::write_u32(buffer, num_partitions);
    return buffer;
}

std::optional<CreateTopicRequest> CreateTopicRequest::deserialize(const uint8_t* data, size_t len) {
    const uint8_t* ptr = data;
    size_t remaining = len;
    
    auto name = ProtocolCodec::read_string(ptr, remaining);
    if (!name) return std::nullopt;
    
    auto partitions = ProtocolCodec::read_u32(ptr, remaining);
    if (!partitions) return std::nullopt;
    
    return CreateTopicRequest{*name, *partitions};
}

// =============================================================================
// DeleteTopicRequest
// =============================================================================

std::vector<uint8_t> DeleteTopicRequest::serialize() const {
    std::vector<uint8_t> buffer;
    ProtocolCodec::write_string(buffer, topic_name);
    return buffer;
}

std::optional<DeleteTopicRequest> DeleteTopicRequest::deserialize(const uint8_t* data, size_t len) {
    const uint8_t* ptr = data;
    size_t remaining = len;
    
    auto name = ProtocolCodec::read_string(ptr, remaining);
    if (!name) return std::nullopt;
    
    return DeleteTopicRequest{*name};
}

// =============================================================================
// ProduceRequest
// =============================================================================

std::vector<uint8_t> ProduceRequest::serialize() const {
    std::vector<uint8_t> buffer;
    ProtocolCodec::write_string(buffer, topic_name);
    ProtocolCodec::write_string(buffer, key);
    ProtocolCodec::write_string(buffer, value);
    buffer.push_back(static_cast<uint8_t>(priority));
    return buffer;
}

std::optional<ProduceRequest> ProduceRequest::deserialize(const uint8_t* data, size_t len) {
    const uint8_t* ptr = data;
    size_t remaining = len;
    
    auto topic = ProtocolCodec::read_string(ptr, remaining);
    if (!topic) return std::nullopt;
    
    auto key = ProtocolCodec::read_string(ptr, remaining);
    if (!key) return std::nullopt;
    
    auto value = ProtocolCodec::read_string(ptr, remaining);
    if (!value) return std::nullopt;
    
    if (remaining < 1) return std::nullopt;
    auto priority = static_cast<MessagePriority>(*ptr);
    
    return ProduceRequest{*topic, *key, *value, priority};
}

// =============================================================================
// FetchRequest
// =============================================================================

std::vector<uint8_t> FetchRequest::serialize() const {
    std::vector<uint8_t> buffer;
    ProtocolCodec::write_string(buffer, topic_name);
    ProtocolCodec::write_u32(buffer, partition);
    ProtocolCodec::write_u64(buffer, offset);
    ProtocolCodec::write_u32(buffer, max_messages);
    return buffer;
}

std::optional<FetchRequest> FetchRequest::deserialize(const uint8_t* data, size_t len) {
    const uint8_t* ptr = data;
    size_t remaining = len;
    
    auto topic = ProtocolCodec::read_string(ptr, remaining);
    if (!topic) return std::nullopt;
    
    auto partition = ProtocolCodec::read_u32(ptr, remaining);
    if (!partition) return std::nullopt;
    
    auto offset = ProtocolCodec::read_u64(ptr, remaining);
    if (!offset) return std::nullopt;
    
    auto max_msgs = ProtocolCodec::read_u32(ptr, remaining);
    if (!max_msgs) return std::nullopt;
    
    return FetchRequest{*topic, *partition, *offset, *max_msgs};
}

// =============================================================================
// JoinGroupRequest
// =============================================================================

std::vector<uint8_t> JoinGroupRequest::serialize() const {
    std::vector<uint8_t> buffer;
    ProtocolCodec::write_string(buffer, group_id);
    ProtocolCodec::write_string(buffer, topic_name);
    return buffer;
}

std::optional<JoinGroupRequest> JoinGroupRequest::deserialize(const uint8_t* data, size_t len) {
    const uint8_t* ptr = data;
    size_t remaining = len;
    
    auto group = ProtocolCodec::read_string(ptr, remaining);
    if (!group) return std::nullopt;
    
    auto topic = ProtocolCodec::read_string(ptr, remaining);
    if (!topic) return std::nullopt;
    
    return JoinGroupRequest{*group, *topic};
}

// =============================================================================
// LeaveGroupRequest
// =============================================================================

std::vector<uint8_t> LeaveGroupRequest::serialize() const {
    std::vector<uint8_t> buffer;
    ProtocolCodec::write_string(buffer, group_id);
    ProtocolCodec::write_string(buffer, consumer_id);
    return buffer;
}

std::optional<LeaveGroupRequest> LeaveGroupRequest::deserialize(const uint8_t* data, size_t len) {
    const uint8_t* ptr = data;
    size_t remaining = len;
    
    auto group = ProtocolCodec::read_string(ptr, remaining);
    if (!group) return std::nullopt;
    
    auto consumer = ProtocolCodec::read_string(ptr, remaining);
    if (!consumer) return std::nullopt;
    
    return LeaveGroupRequest{*group, *consumer};
}

// =============================================================================
// CommitOffsetRequest
// =============================================================================

std::vector<uint8_t> CommitOffsetRequest::serialize() const {
    std::vector<uint8_t> buffer;
    ProtocolCodec::write_string(buffer, group_id);
    ProtocolCodec::write_string(buffer, topic_name);
    ProtocolCodec::write_u32(buffer, partition);
    ProtocolCodec::write_u64(buffer, offset);
    return buffer;
}

std::optional<CommitOffsetRequest> CommitOffsetRequest::deserialize(const uint8_t* data, size_t len) {
    const uint8_t* ptr = data;
    size_t remaining = len;
    
    auto group = ProtocolCodec::read_string(ptr, remaining);
    if (!group) return std::nullopt;
    
    auto topic = ProtocolCodec::read_string(ptr, remaining);
    if (!topic) return std::nullopt;
    
    auto partition = ProtocolCodec::read_u32(ptr, remaining);
    if (!partition) return std::nullopt;
    
    auto offset = ProtocolCodec::read_u64(ptr, remaining);
    if (!offset) return std::nullopt;
    
    return CommitOffsetRequest{*group, *topic, *partition, *offset};
}

// =============================================================================
// FetchOffsetRequest
// =============================================================================

std::vector<uint8_t> FetchOffsetRequest::serialize() const {
    std::vector<uint8_t> buffer;
    ProtocolCodec::write_string(buffer, group_id);
    ProtocolCodec::write_string(buffer, topic_name);
    ProtocolCodec::write_u32(buffer, partition);
    return buffer;
}

std::optional<FetchOffsetRequest> FetchOffsetRequest::deserialize(const uint8_t* data, size_t len) {
    const uint8_t* ptr = data;
    size_t remaining = len;
    
    auto group = ProtocolCodec::read_string(ptr, remaining);
    if (!group) return std::nullopt;
    
    auto topic = ProtocolCodec::read_string(ptr, remaining);
    if (!topic) return std::nullopt;
    
    auto partition = ProtocolCodec::read_u32(ptr, remaining);
    if (!partition) return std::nullopt;
    
    return FetchOffsetRequest{*group, *topic, *partition};
}

// =============================================================================
// GenericResponse
// =============================================================================

std::vector<uint8_t> GenericResponse::serialize() const {
    std::vector<uint8_t> buffer;
    ProtocolCodec::write_i16(buffer, static_cast<int16_t>(error));
    ProtocolCodec::write_string(buffer, error_message);
    return buffer;
}

std::optional<GenericResponse> GenericResponse::deserialize(const uint8_t* data, size_t len) {
    const uint8_t* ptr = data;
    size_t remaining = len;
    
    auto err = ProtocolCodec::read_i16(ptr, remaining);
    if (!err) return std::nullopt;
    
    auto msg = ProtocolCodec::read_string(ptr, remaining);
    if (!msg) return std::nullopt;
    
    return GenericResponse{static_cast<ErrorCode>(*err), *msg};
}

// =============================================================================
// CreateTopicResponse
// =============================================================================

std::vector<uint8_t> CreateTopicResponse::serialize() const {
    std::vector<uint8_t> buffer;
    ProtocolCodec::write_i16(buffer, static_cast<int16_t>(error));
    ProtocolCodec::write_string(buffer, error_message);
    return buffer;
}

std::optional<CreateTopicResponse> CreateTopicResponse::deserialize(const uint8_t* data, size_t len) {
    const uint8_t* ptr = data;
    size_t remaining = len;
    
    auto err = ProtocolCodec::read_i16(ptr, remaining);
    if (!err) return std::nullopt;
    
    auto msg = ProtocolCodec::read_string(ptr, remaining);
    if (!msg) return std::nullopt;
    
    return CreateTopicResponse{static_cast<ErrorCode>(*err), *msg};
}

// =============================================================================
// ListTopicsResponse
// =============================================================================

std::vector<uint8_t> ListTopicsResponse::serialize() const {
    std::vector<uint8_t> buffer;
    ProtocolCodec::write_i16(buffer, static_cast<int16_t>(error));
    ProtocolCodec::write_u32(buffer, static_cast<uint32_t>(topics.size()));
    for (const auto& topic : topics) {
        ProtocolCodec::write_string(buffer, topic);
    }
    return buffer;
}

std::optional<ListTopicsResponse> ListTopicsResponse::deserialize(const uint8_t* data, size_t len) {
    const uint8_t* ptr = data;
    size_t remaining = len;
    
    auto err = ProtocolCodec::read_i16(ptr, remaining);
    if (!err) return std::nullopt;
    
    auto count = ProtocolCodec::read_u32(ptr, remaining);
    if (!count) return std::nullopt;
    
    std::vector<std::string> topics;
    topics.reserve(*count);
    
    for (uint32_t i = 0; i < *count; i++) {
        auto topic = ProtocolCodec::read_string(ptr, remaining);
        if (!topic) return std::nullopt;
        topics.push_back(*topic);
    }
    
    return ListTopicsResponse{static_cast<ErrorCode>(*err), std::move(topics)};
}

// =============================================================================
// ProduceResponse
// =============================================================================

std::vector<uint8_t> ProduceResponse::serialize() const {
    std::vector<uint8_t> buffer;
    ProtocolCodec::write_i16(buffer, static_cast<int16_t>(error));
    ProtocolCodec::write_string(buffer, error_message);
    ProtocolCodec::write_u32(buffer, partition);
    ProtocolCodec::write_u64(buffer, offset);
    return buffer;
}

std::optional<ProduceResponse> ProduceResponse::deserialize(const uint8_t* data, size_t len) {
    const uint8_t* ptr = data;
    size_t remaining = len;
    
    auto err = ProtocolCodec::read_i16(ptr, remaining);
    if (!err) return std::nullopt;
    
    auto msg = ProtocolCodec::read_string(ptr, remaining);
    if (!msg) return std::nullopt;
    
    auto partition = ProtocolCodec::read_u32(ptr, remaining);
    if (!partition) return std::nullopt;
    
    auto offset = ProtocolCodec::read_u64(ptr, remaining);
    if (!offset) return std::nullopt;
    
    return ProduceResponse{static_cast<ErrorCode>(*err), *msg, *partition, *offset};
}

// =============================================================================
// FetchResponse
// =============================================================================

std::vector<uint8_t> FetchResponse::serialize() const {
    std::vector<uint8_t> buffer;
    ProtocolCodec::write_i16(buffer, static_cast<int16_t>(error));
    ProtocolCodec::write_string(buffer, error_message);
    ProtocolCodec::write_u64(buffer, high_watermark);
    ProtocolCodec::write_u32(buffer, static_cast<uint32_t>(messages.size()));
    
    for (const auto& msg : messages) {
        // Serialize each message using Message::serialize()
        auto serialized = msg->serialize();
        ProtocolCodec::write_u32(buffer, static_cast<uint32_t>(serialized.size()));
        buffer.insert(buffer.end(), serialized.begin(), serialized.end());
    }
    
    return buffer;
}

std::optional<FetchResponse> FetchResponse::deserialize(const uint8_t* data, size_t len) {
    const uint8_t* ptr = data;
    size_t remaining = len;
    
    auto err = ProtocolCodec::read_i16(ptr, remaining);
    if (!err) return std::nullopt;
    
    auto error_msg = ProtocolCodec::read_string(ptr, remaining);
    if (!error_msg) return std::nullopt;
    
    auto hwm = ProtocolCodec::read_u64(ptr, remaining);
    if (!hwm) return std::nullopt;
    
    auto count = ProtocolCodec::read_u32(ptr, remaining);
    if (!count) return std::nullopt;
    
    std::vector<MessagePtr> messages;
    messages.reserve(*count);
    
    for (uint32_t i = 0; i < *count; i++) {
        auto msg_len = ProtocolCodec::read_u32(ptr, remaining);
        if (!msg_len) return std::nullopt;
        if (remaining < *msg_len) return std::nullopt;
        
        auto msg = Message::deserialize(ptr, *msg_len);
        if (!msg) return std::nullopt;
        
        messages.push_back(msg);
        ptr += *msg_len;
        remaining -= *msg_len;
    }
    
    FetchResponse resp;
    resp.error = static_cast<ErrorCode>(*err);
    resp.error_message = *error_msg;
    resp.high_watermark = *hwm;
    resp.messages = std::move(messages);
    return resp;
}

// =============================================================================
// JoinGroupResponse
// =============================================================================

std::vector<uint8_t> JoinGroupResponse::serialize() const {
    std::vector<uint8_t> buffer;
    ProtocolCodec::write_i16(buffer, static_cast<int16_t>(error));
    ProtocolCodec::write_string(buffer, error_message);
    ProtocolCodec::write_string(buffer, consumer_id);
    ProtocolCodec::write_u32(buffer, static_cast<uint32_t>(assigned_partitions.size()));
    for (auto p : assigned_partitions) {
        ProtocolCodec::write_u32(buffer, p);
    }
    return buffer;
}

std::optional<JoinGroupResponse> JoinGroupResponse::deserialize(const uint8_t* data, size_t len) {
    const uint8_t* ptr = data;
    size_t remaining = len;
    
    auto err = ProtocolCodec::read_i16(ptr, remaining);
    if (!err) return std::nullopt;
    
    auto msg = ProtocolCodec::read_string(ptr, remaining);
    if (!msg) return std::nullopt;
    
    auto consumer_id = ProtocolCodec::read_string(ptr, remaining);
    if (!consumer_id) return std::nullopt;
    
    auto count = ProtocolCodec::read_u32(ptr, remaining);
    if (!count) return std::nullopt;
    
    std::vector<uint32_t> partitions;
    partitions.reserve(*count);
    
    for (uint32_t i = 0; i < *count; i++) {
        auto p = ProtocolCodec::read_u32(ptr, remaining);
        if (!p) return std::nullopt;
        partitions.push_back(*p);
    }
    
    return JoinGroupResponse{static_cast<ErrorCode>(*err), *msg, *consumer_id, std::move(partitions)};
}

// =============================================================================
// FetchOffsetResponse
// =============================================================================

std::vector<uint8_t> FetchOffsetResponse::serialize() const {
    std::vector<uint8_t> buffer;
    ProtocolCodec::write_i16(buffer, static_cast<int16_t>(error));
    ProtocolCodec::write_u64(buffer, offset);
    return buffer;
}

std::optional<FetchOffsetResponse> FetchOffsetResponse::deserialize(const uint8_t* data, size_t len) {
    const uint8_t* ptr = data;
    size_t remaining = len;
    
    auto err = ProtocolCodec::read_i16(ptr, remaining);
    if (!err) return std::nullopt;
    
    auto offset = ProtocolCodec::read_u64(ptr, remaining);
    if (!offset) return std::nullopt;
    
    return FetchOffsetResponse{static_cast<ErrorCode>(*err), *offset};
}

// =============================================================================
// MetadataResponse
// =============================================================================

std::vector<uint8_t> MetadataResponse::serialize() const {
    std::vector<uint8_t> buffer;
    ProtocolCodec::write_i16(buffer, static_cast<int16_t>(error));
    ProtocolCodec::write_u32(buffer, static_cast<uint32_t>(topics.size()));
    for (const auto& topic : topics) {
        ProtocolCodec::write_string(buffer, topic.name);
        ProtocolCodec::write_u32(buffer, topic.partition_count);
    }
    return buffer;
}

std::optional<MetadataResponse> MetadataResponse::deserialize(const uint8_t* data, size_t len) {
    const uint8_t* ptr = data;
    size_t remaining = len;
    
    auto err = ProtocolCodec::read_i16(ptr, remaining);
    if (!err) return std::nullopt;
    
    auto count = ProtocolCodec::read_u32(ptr, remaining);
    if (!count) return std::nullopt;
    
    std::vector<MetadataResponse::TopicMetadata> topics;
    topics.reserve(*count);
    
    for (uint32_t i = 0; i < *count; i++) {
        auto name = ProtocolCodec::read_string(ptr, remaining);
        if (!name) return std::nullopt;
        
        auto partitions = ProtocolCodec::read_u32(ptr, remaining);
        if (!partitions) return std::nullopt;
        
        topics.push_back({*name, *partitions});
    }
    
    return MetadataResponse{static_cast<ErrorCode>(*err), std::move(topics)};
}

// =============================================================================
// Error Code to String
// =============================================================================

const char* error_code_to_string(ErrorCode code) {
    switch (code) {
        case ErrorCode::NONE: return "Success";
        case ErrorCode::UNKNOWN: return "Unknown error";
        case ErrorCode::INVALID_REQUEST: return "Invalid request";
        case ErrorCode::TOPIC_NOT_FOUND: return "Topic not found";
        case ErrorCode::PARTITION_NOT_FOUND: return "Partition not found";
        case ErrorCode::INVALID_OFFSET: return "Invalid offset";
        case ErrorCode::TOPIC_ALREADY_EXISTS: return "Topic already exists";
        case ErrorCode::GROUP_NOT_FOUND: return "Consumer group not found";
        case ErrorCode::CONSUMER_NOT_FOUND: return "Consumer not found";
        case ErrorCode::TIMEOUT: return "Request timeout";
        case ErrorCode::NETWORK_ERROR: return "Network error";
        case ErrorCode::PROTOCOL_ERROR: return "Protocol error";
        case ErrorCode::BROKER_NOT_AVAILABLE: return "Broker not available";
        default: return "Unknown error code";
    }
}

} // namespace network
} // namespace stream
