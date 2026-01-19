/**
 * Coinbase WebSocket Connector
 * 
 * Connects to Coinbase's public WebSocket feed for market data.
 * This uses the public market data feed which doesn't require authentication
 * for level 2 order book and trade data.
 */

#pragma once

#include <string>
#include <vector>
#include <functional>
#include <thread>
#include <atomic>
#include <mutex>
#include <netdb.h>
#include <unistd.h>
#include <cstring>
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <openssl/sha.h>
#include <sstream>
#include <iomanip>
#include <random>

namespace coinbase {
namespace ws {

// Market data structures
struct Trade {
    std::string product_id;
    std::string trade_id;
    std::string side;  // "buy" or "sell"
    double price;
    double size;
    std::string time;
};

struct Ticker {
    std::string product_id;
    double price;
    double open_24h;
    double volume_24h;
    double low_24h;
    double high_24h;
    double volume_30d;
    double best_bid;
    double best_ask;
    std::string side;
    std::string time;
    std::string trade_id;
    double last_size;
};

struct L2Update {
    std::string product_id;
    std::string time;
    std::vector<std::tuple<std::string, std::string, std::string>> changes; // side, price, size
};

// Callbacks
using TradeCallback = std::function<void(const Trade&)>;
using TickerCallback = std::function<void(const Ticker&)>;
using L2UpdateCallback = std::function<void(const L2Update&)>;
using ConnectionCallback = std::function<void(bool connected, const std::string& reason)>;

class WebSocketConnector {
public:
    WebSocketConnector(bool use_sandbox = true)
        : use_sandbox_(use_sandbox), connected_(false), running_(false),
          ssl_ctx_(nullptr), ssl_(nullptr), sock_fd_(-1) {
        // Initialize OpenSSL
        SSL_library_init();
        SSL_load_error_strings();
        OpenSSL_add_all_algorithms();
    }
    
    ~WebSocketConnector() {
        disconnect();
    }
    
    void set_trade_callback(TradeCallback cb) { trade_callback_ = std::move(cb); }
    void set_ticker_callback(TickerCallback cb) { ticker_callback_ = std::move(cb); }
    void set_l2_callback(L2UpdateCallback cb) { l2_callback_ = std::move(cb); }
    void set_connection_callback(ConnectionCallback cb) { connection_callback_ = std::move(cb); }
    
    bool connect() {
        // Clean up any existing connection first
        if (connected_ || running_ || receive_thread_.joinable()) {
            disconnect();
        }
        
        // Determine host
        const char* host = use_sandbox_ 
            ? "ws-feed-public.sandbox.exchange.coinbase.com"
            : "ws-feed.exchange.coinbase.com";
        const int port = 443;
        
        // Create socket
        struct hostent* server = gethostbyname(host);
        if (!server) {
            notify_connection(false, "DNS resolution failed for " + std::string(host));
            return false;
        }
        
        sock_fd_ = socket(AF_INET, SOCK_STREAM, 0);
        if (sock_fd_ < 0) {
            notify_connection(false, "Socket creation failed");
            return false;
        }
        
        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        memcpy(&addr.sin_addr.s_addr, server->h_addr, server->h_length);
        
        if (::connect(sock_fd_, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
            close(sock_fd_);
            sock_fd_ = -1;
            notify_connection(false, "TCP connection failed");
            return false;
        }
        
        // Setup SSL
        ssl_ctx_ = SSL_CTX_new(TLS_client_method());
        if (!ssl_ctx_) {
            close(sock_fd_);
            sock_fd_ = -1;
            notify_connection(false, "SSL context creation failed");
            return false;
        }
        
        ssl_ = SSL_new(ssl_ctx_);
        SSL_set_fd(ssl_, sock_fd_);
        SSL_set_tlsext_host_name(ssl_, host);
        
        if (SSL_connect(ssl_) <= 0) {
            SSL_free(ssl_);
            SSL_CTX_free(ssl_ctx_);
            close(sock_fd_);
            ssl_ = nullptr;
            ssl_ctx_ = nullptr;
            sock_fd_ = -1;
            notify_connection(false, "SSL handshake failed");
            return false;
        }
        
        // WebSocket handshake
        std::string ws_key = generate_ws_key();
        std::ostringstream request;
        request << "GET / HTTP/1.1\r\n"
                << "Host: " << host << "\r\n"
                << "Upgrade: websocket\r\n"
                << "Connection: Upgrade\r\n"
                << "Sec-WebSocket-Key: " << ws_key << "\r\n"
                << "Sec-WebSocket-Version: 13\r\n"
                << "\r\n";
        
        std::string req_str = request.str();
        if (SSL_write(ssl_, req_str.c_str(), req_str.length()) <= 0) {
            cleanup_connection();
            notify_connection(false, "WebSocket handshake send failed");
            return false;
        }
        
        // Read response
        char buffer[4096];
        int bytes = SSL_read(ssl_, buffer, sizeof(buffer) - 1);
        if (bytes <= 0) {
            cleanup_connection();
            notify_connection(false, "WebSocket handshake response failed");
            return false;
        }
        buffer[bytes] = '\0';
        
        if (strstr(buffer, "101") == nullptr) {
            cleanup_connection();
            notify_connection(false, "WebSocket upgrade rejected");
            return false;
        }
        
        connected_ = true;
        running_ = true;
        
        // Start receive thread
        receive_thread_ = std::thread(&WebSocketConnector::receive_loop, this);
        
        notify_connection(true, "Connected to " + std::string(host));
        return true;
    }
    
    void disconnect() {
        running_ = false;
        connected_ = false;
        
        // Shutdown SSL to unblock any pending SSL_read in the receive thread
        {
            std::lock_guard<std::mutex> lock(send_mutex_);
            if (ssl_) {
                SSL_shutdown(ssl_);
            }
            if (sock_fd_ >= 0) {
                shutdown(sock_fd_, SHUT_RDWR);
            }
        }
        
        // Now safe to join the thread
        if (receive_thread_.joinable()) {
            receive_thread_.join();
        }
        
        cleanup_connection();
    }
    
    bool subscribe(const std::vector<std::string>& products, 
                   const std::vector<std::string>& channels = {"ticker", "matches"}) {
        if (!connected_) return false;
        
        // Build subscribe message
        std::ostringstream msg;
        msg << "{\"type\":\"subscribe\",\"product_ids\":[";
        for (size_t i = 0; i < products.size(); i++) {
            if (i > 0) msg << ",";
            msg << "\"" << products[i] << "\"";
        }
        msg << "],\"channels\":[";
        for (size_t i = 0; i < channels.size(); i++) {
            if (i > 0) msg << ",";
            msg << "\"" << channels[i] << "\"";
        }
        msg << "]}";
        
        return send_ws_message(msg.str());
    }
    
    bool unsubscribe(const std::vector<std::string>& products,
                     const std::vector<std::string>& channels = {"ticker", "matches"}) {
        if (!connected_) return false;
        
        std::ostringstream msg;
        msg << "{\"type\":\"unsubscribe\",\"product_ids\":[";
        for (size_t i = 0; i < products.size(); i++) {
            if (i > 0) msg << ",";
            msg << "\"" << products[i] << "\"";
        }
        msg << "],\"channels\":[";
        for (size_t i = 0; i < channels.size(); i++) {
            if (i > 0) msg << ",";
            msg << "\"" << channels[i] << "\"";
        }
        msg << "]}";
        
        return send_ws_message(msg.str());
    }
    
    bool is_connected() const { return connected_; }
    
    uint64_t messages_received() const { return messages_received_; }
    
private:
    void cleanup_connection() {
        if (ssl_) {
            SSL_shutdown(ssl_);
            SSL_free(ssl_);
            ssl_ = nullptr;
        }
        if (ssl_ctx_) {
            SSL_CTX_free(ssl_ctx_);
            ssl_ctx_ = nullptr;
        }
        if (sock_fd_ >= 0) {
            close(sock_fd_);
            sock_fd_ = -1;
        }
    }
    
    void notify_connection(bool connected, const std::string& reason) {
        if (connection_callback_) {
            connection_callback_(connected, reason);
        }
    }
    
    std::string generate_ws_key() {
        static const char charset[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, sizeof(charset) - 2);
        
        std::string key;
        for (int i = 0; i < 22; i++) {
            key += charset[dis(gen)];
        }
        return key + "==";
    }
    
    bool send_ws_message(const std::string& message) {
        std::lock_guard<std::mutex> lock(send_mutex_);
        
        std::vector<uint8_t> frame;
        frame.push_back(0x81);  // Text frame, FIN bit set
        
        size_t len = message.length();
        if (len <= 125) {
            frame.push_back(0x80 | len);  // Masked
        } else if (len <= 65535) {
            frame.push_back(0x80 | 126);
            frame.push_back((len >> 8) & 0xFF);
            frame.push_back(len & 0xFF);
        } else {
            frame.push_back(0x80 | 127);
            for (int i = 7; i >= 0; i--) {
                frame.push_back((len >> (i * 8)) & 0xFF);
            }
        }
        
        // Masking key
        uint8_t mask[4];
        std::random_device rd;
        for (int i = 0; i < 4; i++) {
            mask[i] = rd() & 0xFF;
            frame.push_back(mask[i]);
        }
        
        // Masked payload
        for (size_t i = 0; i < len; i++) {
            frame.push_back(message[i] ^ mask[i % 4]);
        }
        
        int sent = SSL_write(ssl_, frame.data(), frame.size());
        return sent > 0;
    }
    
    void receive_loop() {
        std::vector<uint8_t> buffer(65536);
        std::string message_buffer;
        
        while (running_ && connected_) {
            int bytes = SSL_read(ssl_, buffer.data(), buffer.size());
            if (bytes <= 0) {
                if (running_) {
                    connected_ = false;
                    notify_connection(false, "Connection lost");
                }
                break;
            }
            
            // Parse WebSocket frames
            size_t pos = 0;
            while (pos < (size_t)bytes) {
                if (pos + 2 > (size_t)bytes) break;
                
                uint8_t opcode = buffer[pos] & 0x0F;
                bool fin = (buffer[pos] & 0x80) != 0;
                uint8_t len_byte = buffer[pos + 1] & 0x7F;
                pos += 2;
                
                size_t payload_len = len_byte;
                if (len_byte == 126) {
                    if (pos + 2 > (size_t)bytes) break;
                    payload_len = (buffer[pos] << 8) | buffer[pos + 1];
                    pos += 2;
                } else if (len_byte == 127) {
                    if (pos + 8 > (size_t)bytes) break;
                    payload_len = 0;
                    for (int i = 0; i < 8; i++) {
                        payload_len = (payload_len << 8) | buffer[pos + i];
                    }
                    pos += 8;
                }
                
                if (pos + payload_len > (size_t)bytes) break;
                
                if (opcode == 0x01 || opcode == 0x00) {  // Text or continuation
                    message_buffer.append(reinterpret_cast<char*>(&buffer[pos]), payload_len);
                    
                    if (fin) {
                        process_message(message_buffer);
                        message_buffer.clear();
                    }
                } else if (opcode == 0x08) {  // Close
                    connected_ = false;
                    notify_connection(false, "Server closed connection");
                    return;
                } else if (opcode == 0x09) {  // Ping
                    // Send pong
                    std::vector<uint8_t> pong = {0x8A, 0x00};
                    SSL_write(ssl_, pong.data(), pong.size());
                }
                
                pos += payload_len;
            }
        }
    }
    
    void process_message(const std::string& msg) {
        messages_received_++;
        
        // Simple JSON parsing for the messages we care about
        std::string type = extract_json_string(msg, "type");
        
        if (type == "ticker" && ticker_callback_) {
            Ticker ticker;
            ticker.product_id = extract_json_string(msg, "product_id");
            ticker.price = extract_json_double(msg, "price");
            ticker.open_24h = extract_json_double(msg, "open_24h");
            ticker.volume_24h = extract_json_double(msg, "volume_24h");
            ticker.low_24h = extract_json_double(msg, "low_24h");
            ticker.high_24h = extract_json_double(msg, "high_24h");
            ticker.best_bid = extract_json_double(msg, "best_bid");
            ticker.best_ask = extract_json_double(msg, "best_ask");
            ticker.side = extract_json_string(msg, "side");
            ticker.time = extract_json_string(msg, "time");
            ticker.trade_id = extract_json_string(msg, "trade_id");
            ticker.last_size = extract_json_double(msg, "last_size");
            ticker_callback_(ticker);
        }
        else if ((type == "match" || type == "last_match") && trade_callback_) {
            Trade trade;
            trade.product_id = extract_json_string(msg, "product_id");
            trade.trade_id = extract_json_string(msg, "trade_id");
            trade.side = extract_json_string(msg, "side");
            trade.price = extract_json_double(msg, "price");
            trade.size = extract_json_double(msg, "size");
            trade.time = extract_json_string(msg, "time");
            trade_callback_(trade);
        }
        else if (type == "l2update" && l2_callback_) {
            L2Update update;
            update.product_id = extract_json_string(msg, "product_id");
            update.time = extract_json_string(msg, "time");
            // TODO: Parse changes array
            l2_callback_(update);
        }
        else if (type == "subscriptions") {
            // Subscription confirmation
        }
        else if (type == "error") {
            std::string error_msg = extract_json_string(msg, "message");
            notify_connection(false, "Error: " + error_msg);
        }
    }
    
    std::string extract_json_string(const std::string& json, const std::string& key) {
        std::string search = "\"" + key + "\":\"";
        size_t pos = json.find(search);
        if (pos == std::string::npos) return "";
        pos += search.length();
        size_t end = json.find("\"", pos);
        if (end == std::string::npos) return "";
        return json.substr(pos, end - pos);
    }
    
    double extract_json_double(const std::string& json, const std::string& key) {
        // Try quoted number first
        std::string search = "\"" + key + "\":\"";
        size_t pos = json.find(search);
        if (pos != std::string::npos) {
            pos += search.length();
            size_t end = json.find("\"", pos);
            if (end != std::string::npos) {
                try {
                    return std::stod(json.substr(pos, end - pos));
                } catch (...) {
                    return 0.0;
                }
            }
        }
        
        // Try unquoted number
        search = "\"" + key + "\":";
        pos = json.find(search);
        if (pos != std::string::npos) {
            pos += search.length();
            size_t end = pos;
            while (end < json.length() && (isdigit(json[end]) || json[end] == '.' || json[end] == '-')) {
                end++;
            }
            if (end > pos) {
                try {
                    return std::stod(json.substr(pos, end - pos));
                } catch (...) {
                    return 0.0;
                }
            }
        }
        
        return 0.0;
    }
    
    bool use_sandbox_;
    std::atomic<bool> connected_;
    std::atomic<bool> running_;
    std::atomic<uint64_t> messages_received_{0};
    
    SSL_CTX* ssl_ctx_;
    SSL* ssl_;
    int sock_fd_;
    
    std::thread receive_thread_;
    std::mutex send_mutex_;
    
    TradeCallback trade_callback_;
    TickerCallback ticker_callback_;
    L2UpdateCallback l2_callback_;
    ConnectionCallback connection_callback_;
};

} // namespace ws
} // namespace coinbase
