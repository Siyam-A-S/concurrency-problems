#include "network/client.h"

#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <fcntl.h>
#include <poll.h>
#include <cerrno>
#include <cstring>
#include <thread>

namespace stream {
namespace network {

// =============================================================================
// TcpClient Implementation
// =============================================================================

TcpClient::TcpClient(const ClientConfig& config)
    : config_(config) {
}

TcpClient::~TcpClient() {
    disconnect();
}

bool TcpClient::connect() {
    if (fd_ >= 0) return true;  // Already connected
    
    // Resolve hostname
    struct addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    
    struct addrinfo* result = nullptr;
    std::string port_str = std::to_string(config_.port);
    
    int err = getaddrinfo(config_.host.c_str(), port_str.c_str(), &hints, &result);
    if (err != 0 || !result) {
        return false;
    }
    
    // Create socket
    fd_ = socket(result->ai_family, SOCK_STREAM, result->ai_protocol);
    if (fd_ < 0) {
        freeaddrinfo(result);
        return false;
    }
    
    // Set non-blocking for connection timeout
    int flags = fcntl(fd_, F_GETFL, 0);
    fcntl(fd_, F_SETFL, flags | O_NONBLOCK);
    
    // Start connection
    err = ::connect(fd_, result->ai_addr, result->ai_addrlen);
    freeaddrinfo(result);
    
    if (err < 0 && errno != EINPROGRESS) {
        ::close(fd_);
        fd_ = -1;
        return false;
    }
    
    // Wait for connection with timeout
    struct pollfd pfd{};
    pfd.fd = fd_;
    pfd.events = POLLOUT;
    
    int timeout_ms = static_cast<int>(config_.connect_timeout.count());
    int poll_result = poll(&pfd, 1, timeout_ms);
    
    if (poll_result <= 0) {
        ::close(fd_);
        fd_ = -1;
        return false;
    }
    
    // Check for connection error
    int sock_err = 0;
    socklen_t len = sizeof(sock_err);
    getsockopt(fd_, SOL_SOCKET, SO_ERROR, &sock_err, &len);
    
    if (sock_err != 0) {
        ::close(fd_);
        fd_ = -1;
        return false;
    }
    
    // Set back to blocking
    fcntl(fd_, F_SETFL, flags);
    
    // Set TCP_NODELAY
    int nodelay = 1;
    setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));
    
    return true;
}

void TcpClient::disconnect() {
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

bool TcpClient::reconnect() {
    disconnect();
    
    for (size_t attempt = 0; attempt < config_.max_retries; attempt++) {
        if (connect()) {
            stats_.reconnects++;
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100 * (attempt + 1)));
    }
    
    return false;
}

bool TcpClient::send_message(RequestType type, const std::vector<uint8_t>& payload) {
    std::lock_guard<std::mutex> lock(io_mutex_);
    
    if (fd_ < 0) {
        if (!config_.auto_reconnect || !reconnect()) {
            stats_.errors++;
            return false;
        }
    }
    
    auto encoded = ProtocolCodec::encode(type, payload);
    
    if (!send_all(encoded.data(), encoded.size())) {
        stats_.errors++;
        disconnect();
        return false;
    }
    
    stats_.requests_sent++;
    stats_.bytes_sent += encoded.size();
    return true;
}

std::optional<std::pair<ProtocolHeader, std::vector<uint8_t>>> TcpClient::receive_response() {
    std::lock_guard<std::mutex> lock(io_mutex_);
    
    if (fd_ < 0) {
        return std::nullopt;
    }
    
    // Set receive timeout
    struct timeval tv;
    tv.tv_sec = static_cast<long>(config_.request_timeout.count() / 1000);
    tv.tv_usec = static_cast<long>((config_.request_timeout.count() % 1000) * 1000);
    setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    
    // Read header
    uint8_t header_buf[HEADER_SIZE];
    if (!recv_all(header_buf, HEADER_SIZE)) {
        stats_.errors++;
        disconnect();
        return std::nullopt;
    }
    
    auto header = ProtocolHeader::deserialize(header_buf);
    if (!header.is_valid()) {
        stats_.errors++;
        disconnect();
        return std::nullopt;
    }
    
    // Read payload
    std::vector<uint8_t> payload(header.length);
    if (header.length > 0 && !recv_all(payload.data(), header.length)) {
        stats_.errors++;
        disconnect();
        return std::nullopt;
    }
    
    stats_.responses_received++;
    stats_.bytes_received += HEADER_SIZE + header.length;
    
    return std::make_pair(header, std::move(payload));
}

bool TcpClient::send_all(const uint8_t* data, size_t len) {
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = ::send(fd_, data + sent, len - sent, MSG_NOSIGNAL);
        if (n <= 0) {
            if (n < 0 && errno == EINTR) continue;
            return false;
        }
        sent += static_cast<size_t>(n);
    }
    return true;
}

bool TcpClient::recv_all(uint8_t* data, size_t len) {
    size_t received = 0;
    while (received < len) {
        ssize_t n = ::recv(fd_, data + received, len - received, 0);
        if (n <= 0) {
            if (n < 0 && errno == EINTR) continue;
            return false;
        }
        received += static_cast<size_t>(n);
    }
    return true;
}

// =============================================================================
// StreamClient Implementation
// =============================================================================

StreamClient::StreamClient(const ClientConfig& config)
    : client_(std::make_unique<TcpClient>(config)) {
}

StreamClient::~StreamClient() = default;

bool StreamClient::connect() {
    return client_->connect();
}

void StreamClient::disconnect() {
    client_->disconnect();
}

bool StreamClient::is_connected() const {
    return client_->is_connected();
}

bool StreamClient::create_topic(const std::string& name, uint32_t partitions) {
    CreateTopicRequest req{name, partitions};
    auto response = client_->send_request<CreateTopicResponse>(RequestType::CREATE_TOPIC, req.serialize());
    return response && response->error == ErrorCode::NONE;
}

bool StreamClient::delete_topic(const std::string& name) {
    DeleteTopicRequest req{name};
    auto response = client_->send_request<GenericResponse>(RequestType::DELETE_TOPIC, req.serialize());
    return response && response->error == ErrorCode::NONE;
}

std::vector<std::string> StreamClient::list_topics() {
    auto response = client_->send_request<ListTopicsResponse>(RequestType::LIST_TOPICS, {});
    if (response && response->error == ErrorCode::NONE) {
        return response->topics;
    }
    return {};
}

std::optional<MetadataResponse> StreamClient::get_metadata() {
    return client_->send_request<MetadataResponse>(RequestType::METADATA, {});
}

std::optional<ProduceResponse> StreamClient::produce(
    const std::string& topic,
    const std::string& key,
    const std::string& value,
    MessagePriority priority) {
    
    ProduceRequest req{topic, key, value, priority};
    return client_->send_request<ProduceResponse>(RequestType::PRODUCE, req.serialize());
}

std::optional<FetchResponse> StreamClient::fetch(
    const std::string& topic,
    uint32_t partition,
    uint64_t offset,
    uint32_t max_messages) {
    
    FetchRequest req{topic, partition, offset, max_messages};
    return client_->send_request<FetchResponse>(RequestType::FETCH, req.serialize());
}

std::optional<JoinGroupResponse> StreamClient::join_group(
    const std::string& group_id,
    const std::string& topic) {
    
    JoinGroupRequest req{group_id, topic};
    return client_->send_request<JoinGroupResponse>(RequestType::JOIN_GROUP, req.serialize());
}

bool StreamClient::leave_group(const std::string& group_id, const std::string& consumer_id) {
    LeaveGroupRequest req{group_id, consumer_id};
    auto response = client_->send_request<GenericResponse>(RequestType::LEAVE_GROUP, req.serialize());
    return response && response->error == ErrorCode::NONE;
}

bool StreamClient::commit_offset(
    const std::string& group_id,
    const std::string& topic,
    uint32_t partition,
    uint64_t offset) {
    
    CommitOffsetRequest req{group_id, topic, partition, offset};
    auto response = client_->send_request<GenericResponse>(RequestType::COMMIT_OFFSET, req.serialize());
    return response && response->error == ErrorCode::NONE;
}

std::optional<uint64_t> StreamClient::fetch_offset(
    const std::string& group_id,
    const std::string& topic,
    uint32_t partition) {
    
    FetchOffsetRequest req{group_id, topic, partition};
    auto response = client_->send_request<FetchOffsetResponse>(RequestType::FETCH_OFFSET, req.serialize());
    if (response && response->error == ErrorCode::NONE) {
        return response->offset;
    }
    return std::nullopt;
}

} // namespace network
} // namespace stream
