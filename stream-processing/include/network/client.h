#pragma once

#include "protocol.h"
#include <string>
#include <memory>
#include <vector>
#include <mutex>
#include <atomic>
#include <chrono>
#include <optional>
#include <functional>

namespace stream {
namespace network {

// =============================================================================
// Client Configuration
// =============================================================================

struct ClientConfig {
    std::string host{"localhost"};
    uint16_t port{9092};
    std::chrono::milliseconds connect_timeout{5000};
    std::chrono::milliseconds request_timeout{30000};
    bool auto_reconnect{true};
    size_t max_retries{3};
};

// =============================================================================
// Client Statistics
// =============================================================================

struct ClientStats {
    uint64_t requests_sent{0};
    uint64_t responses_received{0};
    uint64_t errors{0};
    uint64_t reconnects{0};
    uint64_t bytes_sent{0};
    uint64_t bytes_received{0};
};

// =============================================================================
// TCP Client Class
// =============================================================================

class TcpClient {
public:
    explicit TcpClient(const ClientConfig& config = ClientConfig{});
    ~TcpClient();
    
    // Disable copy
    TcpClient(const TcpClient&) = delete;
    TcpClient& operator=(const TcpClient&) = delete;
    
    // Connection
    bool connect();
    void disconnect();
    bool is_connected() const { return fd_ >= 0; }
    bool reconnect();
    
    // Send request and wait for response
    template<typename ResponseT>
    std::optional<ResponseT> send_request(RequestType type, const std::vector<uint8_t>& payload);
    
    // Low-level send/receive
    bool send_message(RequestType type, const std::vector<uint8_t>& payload);
    std::optional<std::pair<ProtocolHeader, std::vector<uint8_t>>> receive_response();
    
    // Stats
    const ClientStats& stats() const { return stats_; }
    
    // Config access
    const ClientConfig& config() const { return config_; }

private:
    ClientConfig config_;
    int fd_{-1};
    ClientStats stats_;
    mutable std::mutex io_mutex_;
    
    bool send_all(const uint8_t* data, size_t len);
    bool recv_all(uint8_t* data, size_t len);
};

// Template implementation
template<typename ResponseT>
std::optional<ResponseT> TcpClient::send_request(RequestType type, const std::vector<uint8_t>& payload) {
    if (!send_message(type, payload)) {
        return std::nullopt;
    }
    
    auto response = receive_response();
    if (!response) {
        return std::nullopt;
    }
    
    return ResponseT::deserialize(response->second.data(), response->second.size());
}

// =============================================================================
// High-Level Client API
// =============================================================================

/**
 * High-level client for stream processing operations.
 * Wraps TcpClient with convenient methods.
 */
class StreamClient {
public:
    explicit StreamClient(const ClientConfig& config = ClientConfig{});
    ~StreamClient();
    
    // Connection
    bool connect();
    void disconnect();
    bool is_connected() const;
    
    // Topic operations
    bool create_topic(const std::string& name, uint32_t partitions = 1);
    bool delete_topic(const std::string& name);
    std::vector<std::string> list_topics();
    std::optional<MetadataResponse> get_metadata();
    
    // Produce
    std::optional<ProduceResponse> produce(
        const std::string& topic,
        const std::string& key,
        const std::string& value,
        MessagePriority priority = MessagePriority::NORMAL
    );
    
    // Consume
    std::optional<FetchResponse> fetch(
        const std::string& topic,
        uint32_t partition,
        uint64_t offset,
        uint32_t max_messages = 100
    );
    
    // Consumer groups
    std::optional<JoinGroupResponse> join_group(
        const std::string& group_id,
        const std::string& topic
    );
    
    bool leave_group(const std::string& group_id, const std::string& consumer_id);
    
    bool commit_offset(
        const std::string& group_id,
        const std::string& topic,
        uint32_t partition,
        uint64_t offset
    );
    
    std::optional<uint64_t> fetch_offset(
        const std::string& group_id,
        const std::string& topic,
        uint32_t partition
    );

private:
    std::unique_ptr<TcpClient> client_;
};

} // namespace network
} // namespace stream
