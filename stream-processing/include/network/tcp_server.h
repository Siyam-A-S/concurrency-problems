#pragma once

#include "protocol.h"
#include "../broker.h"
#include <cstdint>
#include <string>
#include <memory>
#include <functional>
#include <atomic>
#include <thread>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace stream {
namespace network {

// Forward declarations
class Connection;
class TcpServer;

// =============================================================================
// Connection State
// =============================================================================

enum class ConnectionState {
    CONNECTED,
    AUTHENTICATED,
    CLOSING,
    CLOSED
};

// =============================================================================
// Connection Statistics
// =============================================================================

struct ConnectionStats {
    uint64_t bytes_received{0};
    uint64_t bytes_sent{0};
    uint64_t requests_processed{0};
    uint64_t errors{0};
    std::chrono::steady_clock::time_point connected_at;
    std::chrono::steady_clock::time_point last_active;
};

// =============================================================================
// Connection Class
// =============================================================================

class Connection : public std::enable_shared_from_this<Connection> {
public:
    using Ptr = std::shared_ptr<Connection>;
    
    Connection(int fd, const std::string& remote_addr, uint16_t remote_port);
    ~Connection();
    
    // Disable copy
    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;
    
    // Accessors
    int fd() const { return fd_; }
    const std::string& remote_address() const { return remote_addr_; }
    uint16_t remote_port() const { return remote_port_; }
    ConnectionState state() const { return state_; }
    const ConnectionStats& stats() const { return stats_; }
    
    // I/O operations
    bool read_available();  // Returns false if connection should be closed
    bool write_pending();   // Returns false if connection should be closed
    bool has_pending_writes() const;
    
    // Message handling
    bool has_complete_message() const;
    std::pair<ProtocolHeader, std::vector<uint8_t>> extract_message();
    void queue_response(RequestType type, const std::vector<uint8_t>& payload);
    
    // State management
    void set_state(ConnectionState state) { state_ = state; }
    void update_activity();
    bool is_idle(std::chrono::seconds timeout) const;
    
    // Close
    void close();

private:
    int fd_;
    std::string remote_addr_;
    uint16_t remote_port_;
    ConnectionState state_;
    ConnectionStats stats_;
    
    // Read buffer
    std::vector<uint8_t> read_buffer_;
    size_t read_pos_{0};
    
    // Write buffer
    std::vector<uint8_t> write_buffer_;
    size_t write_pos_{0};
    
    mutable std::mutex write_mutex_;
};

// =============================================================================
// Server Configuration
// =============================================================================

struct ServerConfig {
    std::string bind_address{"0.0.0.0"};
    uint16_t port{9092};
    int backlog{128};
    size_t max_connections{10000};
    std::chrono::seconds idle_timeout{300};  // 5 minutes
    size_t num_io_threads{4};
};

// =============================================================================
// Server Statistics
// =============================================================================

struct ServerStats {
    std::atomic<uint64_t> total_connections{0};
    std::atomic<uint64_t> active_connections{0};
    std::atomic<uint64_t> total_requests{0};
    std::atomic<uint64_t> total_bytes_received{0};
    std::atomic<uint64_t> total_bytes_sent{0};
    std::chrono::steady_clock::time_point started_at;
};

// =============================================================================
// Request Handler Callback
// =============================================================================

using RequestHandler = std::function<std::vector<uint8_t>(
    Connection::Ptr connection,
    RequestType type,
    const std::vector<uint8_t>& payload
)>;

// =============================================================================
// TCP Server Class
// =============================================================================

class TcpServer {
public:
    explicit TcpServer(const ServerConfig& config = ServerConfig{});
    ~TcpServer();
    
    // Disable copy
    TcpServer(const TcpServer&) = delete;
    TcpServer& operator=(const TcpServer&) = delete;
    
    // Set request handler (must be called before start)
    void set_request_handler(RequestHandler handler);
    
    // Lifecycle
    bool start();
    void stop();
    bool is_running() const { return running_; }
    
    // Stats
    const ServerStats& stats() const { return stats_; }
    size_t connection_count() const;
    
    // For testing - get bound port (useful when using port 0)
    uint16_t bound_port() const { return bound_port_; }

private:
    ServerConfig config_;
    int listen_fd_{-1};
    int epoll_fd_{-1};
    uint16_t bound_port_{0};
    
    std::atomic<bool> running_{false};
    
    // Connections
    mutable std::mutex connections_mutex_;
    std::unordered_map<int, Connection::Ptr> connections_;
    
    // Worker threads
    std::vector<std::thread> io_threads_;
    
    // Request handler
    RequestHandler request_handler_;
    
    // Stats
    ServerStats stats_;
    
    // Internal methods
    bool setup_listener();
    void accept_loop();
    void io_loop();
    void handle_accept();
    void handle_connection_event(int fd, uint32_t events);
    void process_connection(Connection::Ptr conn);
    void remove_connection(int fd);
    void cleanup_idle_connections();
    
    // epoll helpers
    bool add_to_epoll(int fd, uint32_t events);
    bool modify_epoll(int fd, uint32_t events);
    void remove_from_epoll(int fd);
};

// =============================================================================
// Broker Request Handler
// =============================================================================

/**
 * Creates a request handler that processes requests using the given broker.
 * This connects the network layer to the broker layer.
 */
class BrokerRequestHandler {
public:
    explicit BrokerRequestHandler(std::shared_ptr<Broker> broker);
    
    std::vector<uint8_t> handle(
        Connection::Ptr connection,
        RequestType type,
        const std::vector<uint8_t>& payload
    );

private:
    std::shared_ptr<Broker> broker_;
    
    // Consumer groups: group_id -> ConsumerGroup
    mutable std::mutex groups_mutex_;
    std::unordered_map<std::string, std::unique_ptr<ConsumerGroup>> consumer_groups_;
    
    // Handler methods
    std::vector<uint8_t> handle_metadata();
    std::vector<uint8_t> handle_create_topic(const std::vector<uint8_t>& payload);
    std::vector<uint8_t> handle_delete_topic(const std::vector<uint8_t>& payload);
    std::vector<uint8_t> handle_list_topics();
    std::vector<uint8_t> handle_produce(const std::vector<uint8_t>& payload);
    std::vector<uint8_t> handle_fetch(const std::vector<uint8_t>& payload);
    std::vector<uint8_t> handle_join_group(const std::vector<uint8_t>& payload);
    std::vector<uint8_t> handle_leave_group(const std::vector<uint8_t>& payload);
    std::vector<uint8_t> handle_commit_offset(const std::vector<uint8_t>& payload);
    std::vector<uint8_t> handle_fetch_offset(const std::vector<uint8_t>& payload);
    
    ConsumerGroup* get_or_create_group(const std::string& group_id, const std::string& topic);
};

} // namespace network
} // namespace stream
