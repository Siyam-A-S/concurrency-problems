#include "network/tcp_server.h"

#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <cerrno>
#include <cstring>
#include <algorithm>

namespace stream {
namespace network {

// =============================================================================
// Helper Functions
// =============================================================================

namespace {

bool set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) return false;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK) != -1;
}

bool set_tcp_nodelay(int fd) {
    int flag = 1;
    return setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag)) == 0;
}

bool set_reuseaddr(int fd) {
    int flag = 1;
    return setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &flag, sizeof(flag)) == 0;
}

} // anonymous namespace

// =============================================================================
// Connection Implementation
// =============================================================================

Connection::Connection(int fd, const std::string& remote_addr, uint16_t remote_port)
    : fd_(fd)
    , remote_addr_(remote_addr)
    , remote_port_(remote_port)
    , state_(ConnectionState::CONNECTED) {
    
    stats_.connected_at = std::chrono::steady_clock::now();
    stats_.last_active = stats_.connected_at;
    
    // Reserve buffer space
    read_buffer_.reserve(64 * 1024);  // 64KB initial
}

Connection::~Connection() {
    close();
}

void Connection::close() {
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
    state_ = ConnectionState::CLOSED;
}

bool Connection::read_available() {
    if (state_ == ConnectionState::CLOSED) return false;
    
    uint8_t temp[8192];
    
    while (true) {
        ssize_t n = ::recv(fd_, temp, sizeof(temp), 0);
        
        if (n > 0) {
            read_buffer_.insert(read_buffer_.end(), temp, temp + n);
            stats_.bytes_received += static_cast<uint64_t>(n);
            update_activity();
        } else if (n == 0) {
            // Connection closed by peer
            return false;
        } else {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // No more data available
                return true;
            }
            if (errno == EINTR) {
                continue;  // Retry
            }
            // Error
            return false;
        }
    }
}

bool Connection::write_pending() {
    std::lock_guard<std::mutex> lock(write_mutex_);
    
    if (write_buffer_.empty()) return true;
    
    while (write_pos_ < write_buffer_.size()) {
        ssize_t n = ::send(fd_, write_buffer_.data() + write_pos_,
                           write_buffer_.size() - write_pos_, MSG_NOSIGNAL);
        
        if (n > 0) {
            write_pos_ += static_cast<size_t>(n);
            stats_.bytes_sent += static_cast<uint64_t>(n);
        } else if (n == 0) {
            return false;
        } else {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // Would block, try again later
                return true;
            }
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
    }
    
    // All data sent, clear buffer
    write_buffer_.clear();
    write_pos_ = 0;
    return true;
}

bool Connection::has_pending_writes() const {
    std::lock_guard<std::mutex> lock(write_mutex_);
    return write_pos_ < write_buffer_.size();
}

bool Connection::has_complete_message() const {
    if (read_buffer_.size() < HEADER_SIZE) return false;
    
    auto header = ProtocolHeader::deserialize(read_buffer_.data());
    if (!header.is_valid()) return false;
    
    return read_buffer_.size() >= HEADER_SIZE + header.length;
}

std::pair<ProtocolHeader, std::vector<uint8_t>> Connection::extract_message() {
    auto header = ProtocolHeader::deserialize(read_buffer_.data());
    
    size_t total_len = HEADER_SIZE + header.length;
    std::vector<uint8_t> payload(
        read_buffer_.begin() + HEADER_SIZE,
        read_buffer_.begin() + static_cast<ptrdiff_t>(total_len)
    );
    
    // Remove consumed data from buffer
    read_buffer_.erase(read_buffer_.begin(), read_buffer_.begin() + static_cast<ptrdiff_t>(total_len));
    
    stats_.requests_processed++;
    
    return {header, std::move(payload)};
}

void Connection::queue_response(RequestType type, const std::vector<uint8_t>& payload) {
    auto encoded = ProtocolCodec::encode(type, payload);
    
    std::lock_guard<std::mutex> lock(write_mutex_);
    write_buffer_.insert(write_buffer_.end(), encoded.begin(), encoded.end());
}

void Connection::update_activity() {
    stats_.last_active = std::chrono::steady_clock::now();
}

bool Connection::is_idle(std::chrono::seconds timeout) const {
    auto now = std::chrono::steady_clock::now();
    auto idle_time = std::chrono::duration_cast<std::chrono::seconds>(now - stats_.last_active);
    return idle_time > timeout;
}

// =============================================================================
// TcpServer Implementation
// =============================================================================

TcpServer::TcpServer(const ServerConfig& config)
    : config_(config) {
    
    stats_.started_at = std::chrono::steady_clock::now();
}

TcpServer::~TcpServer() {
    stop();
}

void TcpServer::set_request_handler(RequestHandler handler) {
    request_handler_ = std::move(handler);
}

bool TcpServer::setup_listener() {
    // Create socket
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
        return false;
    }
    
    // Set socket options
    if (!set_reuseaddr(listen_fd_)) {
        ::close(listen_fd_);
        listen_fd_ = -1;
        return false;
    }
    
    if (!set_nonblocking(listen_fd_)) {
        ::close(listen_fd_);
        listen_fd_ = -1;
        return false;
    }
    
    // Bind
    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(config_.port);
    
    if (inet_pton(AF_INET, config_.bind_address.c_str(), &addr.sin_addr) <= 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
        return false;
    }
    
    if (bind(listen_fd_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
        return false;
    }
    
    // Get actual bound port (for port 0 case)
    socklen_t addr_len = sizeof(addr);
    if (getsockname(listen_fd_, reinterpret_cast<struct sockaddr*>(&addr), &addr_len) == 0) {
        bound_port_ = ntohs(addr.sin_port);
    }
    
    // Listen
    if (listen(listen_fd_, config_.backlog) < 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
        return false;
    }
    
    return true;
}

bool TcpServer::start() {
    if (running_) return false;
    
    if (!setup_listener()) {
        return false;
    }
    
    // Create epoll instance
    epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ < 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
        return false;
    }
    
    // Add listener to epoll
    if (!add_to_epoll(listen_fd_, EPOLLIN)) {
        ::close(epoll_fd_);
        ::close(listen_fd_);
        epoll_fd_ = -1;
        listen_fd_ = -1;
        return false;
    }
    
    running_ = true;
    
    // Start I/O threads
    for (size_t i = 0; i < config_.num_io_threads; i++) {
        io_threads_.emplace_back(&TcpServer::io_loop, this);
    }
    
    return true;
}

void TcpServer::stop() {
    if (!running_.exchange(false)) return;
    
    // Close epoll to wake up threads
    if (epoll_fd_ >= 0) {
        ::close(epoll_fd_);
        epoll_fd_ = -1;
    }
    
    // Wait for threads
    for (auto& t : io_threads_) {
        if (t.joinable()) {
            t.join();
        }
    }
    io_threads_.clear();
    
    // Close listener
    if (listen_fd_ >= 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
    }
    
    // Close all connections
    std::lock_guard<std::mutex> lock(connections_mutex_);
    connections_.clear();
}

size_t TcpServer::connection_count() const {
    std::lock_guard<std::mutex> lock(connections_mutex_);
    return connections_.size();
}

void TcpServer::io_loop() {
    constexpr int MAX_EVENTS = 64;
    struct epoll_event events[MAX_EVENTS];
    
    while (running_) {
        int n = epoll_wait(epoll_fd_, events, MAX_EVENTS, 100);  // 100ms timeout
        
        if (n < 0) {
            if (errno == EINTR) continue;
            break;  // Fatal error
        }
        
        for (int i = 0; i < n; i++) {
            int fd = events[i].data.fd;
            uint32_t ev = events[i].events;
            
            if (fd == listen_fd_) {
                handle_accept();
            } else {
                handle_connection_event(fd, ev);
            }
        }
        
        // Periodically cleanup idle connections
        cleanup_idle_connections();
    }
}

void TcpServer::handle_accept() {
    while (true) {
        struct sockaddr_in client_addr{};
        socklen_t addr_len = sizeof(client_addr);
        
        int client_fd = accept4(listen_fd_, 
                                reinterpret_cast<struct sockaddr*>(&client_addr),
                                &addr_len, SOCK_NONBLOCK | SOCK_CLOEXEC);
        
        if (client_fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;  // No more pending connections
            }
            if (errno == EINTR) {
                continue;
            }
            break;  // Error
        }
        
        // Check connection limit
        if (stats_.active_connections >= config_.max_connections) {
            ::close(client_fd);
            continue;
        }
        
        // Set TCP_NODELAY
        set_tcp_nodelay(client_fd);
        
        // Get client info
        char addr_str[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &client_addr.sin_addr, addr_str, sizeof(addr_str));
        uint16_t port = ntohs(client_addr.sin_port);
        
        // Create connection
        auto conn = std::make_shared<Connection>(client_fd, addr_str, port);
        
        // Add to epoll
        if (!add_to_epoll(client_fd, EPOLLIN | EPOLLOUT | EPOLLET)) {
            continue;
        }
        
        // Store connection
        {
            std::lock_guard<std::mutex> lock(connections_mutex_);
            connections_[client_fd] = conn;
        }
        
        stats_.total_connections++;
        stats_.active_connections++;
    }
}

void TcpServer::handle_connection_event(int fd, uint32_t events) {
    Connection::Ptr conn;
    {
        std::lock_guard<std::mutex> lock(connections_mutex_);
        auto it = connections_.find(fd);
        if (it == connections_.end()) return;
        conn = it->second;
    }
    
    // Handle errors
    if (events & (EPOLLERR | EPOLLHUP)) {
        remove_connection(fd);
        return;
    }
    
    // Handle readable
    if (events & EPOLLIN) {
        if (!conn->read_available()) {
            remove_connection(fd);
            return;
        }
        
        // Process complete messages
        process_connection(conn);
    }
    
    // Handle writable
    if (events & EPOLLOUT) {
        if (!conn->write_pending()) {
            remove_connection(fd);
            return;
        }
    }
}

void TcpServer::process_connection(Connection::Ptr conn) {
    while (conn->has_complete_message()) {
        auto [header, payload] = conn->extract_message();
        
        stats_.total_requests++;
        
        if (request_handler_) {
            auto response = request_handler_(
                conn, 
                static_cast<RequestType>(header.type),
                payload
            );
            
            conn->queue_response(static_cast<RequestType>(header.type), response);
            
            // Try to send immediately
            if (!conn->write_pending()) {
                remove_connection(conn->fd());
                return;
            }
        }
    }
}

void TcpServer::remove_connection(int fd) {
    remove_from_epoll(fd);
    
    std::lock_guard<std::mutex> lock(connections_mutex_);
    auto it = connections_.find(fd);
    if (it != connections_.end()) {
        it->second->close();
        connections_.erase(it);
        stats_.active_connections--;
    }
}

void TcpServer::cleanup_idle_connections() {
    // Only run periodically
    static thread_local std::chrono::steady_clock::time_point last_cleanup;
    auto now = std::chrono::steady_clock::now();
    if (now - last_cleanup < std::chrono::seconds(30)) {
        return;
    }
    last_cleanup = now;
    
    std::vector<int> to_remove;
    
    {
        std::lock_guard<std::mutex> lock(connections_mutex_);
        for (const auto& [fd, conn] : connections_) {
            if (conn->is_idle(config_.idle_timeout)) {
                to_remove.push_back(fd);
            }
        }
    }
    
    for (int fd : to_remove) {
        remove_connection(fd);
    }
}

bool TcpServer::add_to_epoll(int fd, uint32_t events) {
    struct epoll_event ev{};
    ev.events = events;
    ev.data.fd = fd;
    return epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev) == 0;
}

bool TcpServer::modify_epoll(int fd, uint32_t events) {
    struct epoll_event ev{};
    ev.events = events;
    ev.data.fd = fd;
    return epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev) == 0;
}

void TcpServer::remove_from_epoll(int fd) {
    epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
}

// =============================================================================
// BrokerRequestHandler Implementation
// =============================================================================

BrokerRequestHandler::BrokerRequestHandler(std::shared_ptr<Broker> broker)
    : broker_(std::move(broker)) {
}

std::vector<uint8_t> BrokerRequestHandler::handle(
    Connection::Ptr /*connection*/,
    RequestType type,
    const std::vector<uint8_t>& payload) {
    
    switch (type) {
        case RequestType::METADATA:
            return handle_metadata();
        case RequestType::CREATE_TOPIC:
            return handle_create_topic(payload);
        case RequestType::DELETE_TOPIC:
            return handle_delete_topic(payload);
        case RequestType::LIST_TOPICS:
            return handle_list_topics();
        case RequestType::PRODUCE:
            return handle_produce(payload);
        case RequestType::FETCH:
            return handle_fetch(payload);
        case RequestType::JOIN_GROUP:
            return handle_join_group(payload);
        case RequestType::LEAVE_GROUP:
            return handle_leave_group(payload);
        case RequestType::COMMIT_OFFSET:
            return handle_commit_offset(payload);
        case RequestType::FETCH_OFFSET:
            return handle_fetch_offset(payload);
        case RequestType::HEARTBEAT:
            return GenericResponse{ErrorCode::NONE, ""}.serialize();
        default:
            return GenericResponse{ErrorCode::INVALID_REQUEST, "Unknown request type"}.serialize();
    }
}

std::vector<uint8_t> BrokerRequestHandler::handle_metadata() {
    MetadataResponse response;
    response.error = ErrorCode::NONE;
    
    auto topics = broker_->list_topics();
    for (const auto& topic_name : topics) {
        uint32_t partitions = broker_->get_partition_count(topic_name);
        response.topics.push_back({topic_name, partitions});
    }
    
    return response.serialize();
}

std::vector<uint8_t> BrokerRequestHandler::handle_create_topic(const std::vector<uint8_t>& payload) {
    auto req = CreateTopicRequest::deserialize(payload.data(), payload.size());
    if (!req) {
        return CreateTopicResponse{ErrorCode::INVALID_REQUEST, "Failed to parse request"}.serialize();
    }
    
    if (broker_->topic_exists(req->topic_name)) {
        return CreateTopicResponse{ErrorCode::TOPIC_ALREADY_EXISTS, "Topic already exists"}.serialize();
    }
    
    if (broker_->create_topic(req->topic_name, req->num_partitions)) {
        return CreateTopicResponse{ErrorCode::NONE, ""}.serialize();
    }
    
    return CreateTopicResponse{ErrorCode::UNKNOWN, "Failed to create topic"}.serialize();
}

std::vector<uint8_t> BrokerRequestHandler::handle_delete_topic(const std::vector<uint8_t>& payload) {
    auto req = DeleteTopicRequest::deserialize(payload.data(), payload.size());
    if (!req) {
        return GenericResponse{ErrorCode::INVALID_REQUEST, "Failed to parse request"}.serialize();
    }
    
    if (!broker_->topic_exists(req->topic_name)) {
        return GenericResponse{ErrorCode::TOPIC_NOT_FOUND, "Topic not found"}.serialize();
    }
    
    if (broker_->delete_topic(req->topic_name)) {
        return GenericResponse{ErrorCode::NONE, ""}.serialize();
    }
    
    return GenericResponse{ErrorCode::UNKNOWN, "Failed to delete topic"}.serialize();
}

std::vector<uint8_t> BrokerRequestHandler::handle_list_topics() {
    ListTopicsResponse response;
    response.error = ErrorCode::NONE;
    response.topics = broker_->list_topics();
    return response.serialize();
}

std::vector<uint8_t> BrokerRequestHandler::handle_produce(const std::vector<uint8_t>& payload) {
    auto req = ProduceRequest::deserialize(payload.data(), payload.size());
    if (!req) {
        return ProduceResponse{ErrorCode::INVALID_REQUEST, "Failed to parse request", 0, 0}.serialize();
    }
    
    auto result = broker_->produce(req->topic_name, req->key, req->value, req->priority);
    
    if (result.success) {
        return ProduceResponse{ErrorCode::NONE, "", result.partition, result.offset}.serialize();
    }
    
    return ProduceResponse{ErrorCode::TOPIC_NOT_FOUND, result.error, 0, 0}.serialize();
}

std::vector<uint8_t> BrokerRequestHandler::handle_fetch(const std::vector<uint8_t>& payload) {
    auto req = FetchRequest::deserialize(payload.data(), payload.size());
    if (!req) {
        FetchResponse resp;
        resp.error = ErrorCode::INVALID_REQUEST;
        resp.error_message = "Failed to parse request";
        return resp.serialize();
    }
    
    auto messages = broker_->consume_batch(req->topic_name, req->partition, req->offset, req->max_messages);
    uint64_t hwm = broker_->get_latest_offset(req->topic_name, req->partition);
    
    FetchResponse response;
    response.error = ErrorCode::NONE;
    response.messages = std::move(messages);
    response.high_watermark = hwm;
    return response.serialize();
}

std::vector<uint8_t> BrokerRequestHandler::handle_join_group(const std::vector<uint8_t>& payload) {
    auto req = JoinGroupRequest::deserialize(payload.data(), payload.size());
    if (!req) {
        return JoinGroupResponse{ErrorCode::INVALID_REQUEST, "Failed to parse request", "", {}}.serialize();
    }
    
    if (!broker_->topic_exists(req->topic_name)) {
        return JoinGroupResponse{ErrorCode::TOPIC_NOT_FOUND, "Topic not found", "", {}}.serialize();
    }
    
    auto* group = get_or_create_group(req->group_id, req->topic_name);
    std::string consumer_id = group->join();
    auto partitions = group->get_assigned_partitions(consumer_id);
    
    return JoinGroupResponse{ErrorCode::NONE, "", consumer_id, partitions}.serialize();
}

std::vector<uint8_t> BrokerRequestHandler::handle_leave_group(const std::vector<uint8_t>& payload) {
    auto req = LeaveGroupRequest::deserialize(payload.data(), payload.size());
    if (!req) {
        return GenericResponse{ErrorCode::INVALID_REQUEST, "Failed to parse request"}.serialize();
    }
    
    std::lock_guard<std::mutex> lock(groups_mutex_);
    auto it = consumer_groups_.find(req->group_id);
    if (it == consumer_groups_.end()) {
        return GenericResponse{ErrorCode::GROUP_NOT_FOUND, "Group not found"}.serialize();
    }
    
    it->second->leave(req->consumer_id);
    return GenericResponse{ErrorCode::NONE, ""}.serialize();
}

std::vector<uint8_t> BrokerRequestHandler::handle_commit_offset(const std::vector<uint8_t>& payload) {
    auto req = CommitOffsetRequest::deserialize(payload.data(), payload.size());
    if (!req) {
        return GenericResponse{ErrorCode::INVALID_REQUEST, "Failed to parse request"}.serialize();
    }
    
    broker_->commit_offset(req->group_id, req->topic_name, req->partition, req->offset);
    return GenericResponse{ErrorCode::NONE, ""}.serialize();
}

std::vector<uint8_t> BrokerRequestHandler::handle_fetch_offset(const std::vector<uint8_t>& payload) {
    auto req = FetchOffsetRequest::deserialize(payload.data(), payload.size());
    if (!req) {
        return FetchOffsetResponse{ErrorCode::INVALID_REQUEST, 0}.serialize();
    }
    
    uint64_t offset = broker_->get_committed_offset(req->group_id, req->topic_name, req->partition);
    return FetchOffsetResponse{ErrorCode::NONE, offset}.serialize();
}

ConsumerGroup* BrokerRequestHandler::get_or_create_group(const std::string& group_id, const std::string& topic) {
    std::lock_guard<std::mutex> lock(groups_mutex_);
    
    auto it = consumer_groups_.find(group_id);
    if (it != consumer_groups_.end()) {
        return it->second.get();
    }
    
    auto group = std::make_unique<ConsumerGroup>(broker_, group_id, topic);
    auto* ptr = group.get();
    consumer_groups_[group_id] = std::move(group);
    return ptr;
}

} // namespace network
} // namespace stream
