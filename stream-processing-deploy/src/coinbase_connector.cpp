/**
 * Coinbase FIX API Connector Implementation
 */

#include "coinbase_connector.h"

#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstring>
#include <sstream>
#include <iomanip>
#include <algorithm>
#include <openssl/hmac.h>
#include <openssl/bio.h>
#include <openssl/evp.h>
#include <openssl/buffer.h>

namespace coinbase {

// =============================================================================
// Helper Functions
// =============================================================================

namespace {

std::string get_utc_timestamp() {
    auto now = std::chrono::system_clock::now();
    auto time_t_now = std::chrono::system_clock::to_time_t(now);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()) % 1000;
    
    std::tm tm_utc;
    gmtime_r(&time_t_now, &tm_utc);
    
    std::ostringstream oss;
    oss << std::put_time(&tm_utc, "%Y%m%d-%H:%M:%S")
        << '.' << std::setfill('0') << std::setw(3) << ms.count();
    return oss.str();
}

uint64_t get_timestamp_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

std::string base64_encode(const unsigned char* data, size_t len) {
    BIO* bio = BIO_new(BIO_s_mem());
    BIO* b64 = BIO_new(BIO_f_base64());
    BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);
    bio = BIO_push(b64, bio);
    BIO_write(bio, data, static_cast<int>(len));
    BIO_flush(bio);
    
    BUF_MEM* buffer;
    BIO_get_mem_ptr(bio, &buffer);
    std::string result(buffer->data, buffer->length);
    BIO_free_all(bio);
    return result;
}

std::vector<unsigned char> base64_decode(const std::string& encoded) {
    BIO* bio = BIO_new_mem_buf(encoded.data(), static_cast<int>(encoded.size()));
    BIO* b64 = BIO_new(BIO_f_base64());
    BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);
    bio = BIO_push(b64, bio);
    
    std::vector<unsigned char> result(encoded.size());
    int len = BIO_read(bio, result.data(), static_cast<int>(result.size()));
    result.resize(len > 0 ? len : 0);
    BIO_free_all(bio);
    return result;
}

} // anonymous namespace

// =============================================================================
// FixMessage Implementation
// =============================================================================

FixMessage::FixMessage(char msg_type) : msg_type_(msg_type) {}

void FixMessage::set_field(int tag, const std::string& value) {
    fields_[tag] = value;
}

void FixMessage::set_field(int tag, int value) {
    fields_[tag] = std::to_string(value);
}

void FixMessage::set_field(int tag, double value) {
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(8) << value;
    std::string s = oss.str();
    // Remove trailing zeros
    s.erase(s.find_last_not_of('0') + 1, std::string::npos);
    if (s.back() == '.') s.pop_back();
    fields_[tag] = s;
}

void FixMessage::set_field(int tag, char value) {
    fields_[tag] = std::string(1, value);
}

std::string FixMessage::get_field(int tag) const {
    auto it = fields_.find(tag);
    return it != fields_.end() ? it->second : "";
}

bool FixMessage::has_field(int tag) const {
    return fields_.find(tag) != fields_.end();
}

char FixMessage::msg_type() const {
    return msg_type_;
}

std::string FixMessage::serialize(const std::string& sender, const std::string& target, int seq_num) const {
    std::ostringstream body;
    
    // Message type
    body << "35=" << msg_type_ << '\x01';
    
    // Standard header fields
    body << "49=" << sender << '\x01';
    body << "56=" << target << '\x01';
    body << "34=" << seq_num << '\x01';
    body << "52=" << get_utc_timestamp() << '\x01';
    
    // Body fields (sorted by tag for consistency)
    for (const auto& [tag, value] : fields_) {
        body << tag << '=' << value << '\x01';
    }
    
    std::string body_str = body.str();
    
    // Build complete message
    std::ostringstream msg;
    msg << "8=FIX.4.2" << '\x01';
    msg << "9=" << body_str.length() << '\x01';
    msg << body_str;
    
    std::string msg_str = msg.str();
    
    // Add checksum
    msg_str += "10=" + compute_checksum(msg_str) + '\x01';
    
    return msg_str;
}

FixMessage FixMessage::parse(const std::string& raw) {
    FixMessage msg;
    
    size_t pos = 0;
    while (pos < raw.length()) {
        size_t eq_pos = raw.find('=', pos);
        if (eq_pos == std::string::npos) break;
        
        size_t soh_pos = raw.find('\x01', eq_pos);
        if (soh_pos == std::string::npos) soh_pos = raw.length();
        
        int tag = std::stoi(raw.substr(pos, eq_pos - pos));
        std::string value = raw.substr(eq_pos + 1, soh_pos - eq_pos - 1);
        
        if (tag == FixTag::MsgType && !value.empty()) {
            msg.msg_type_ = value[0];
        } else {
            msg.fields_[tag] = value;
        }
        
        pos = soh_pos + 1;
    }
    
    return msg;
}

std::string FixMessage::to_string() const {
    std::ostringstream oss;
    oss << "MsgType=" << msg_type_ << " ";
    for (const auto& [tag, value] : fields_) {
        oss << tag << "=" << value << " ";
    }
    return oss.str();
}

std::string FixMessage::compute_checksum(const std::string& msg) {
    unsigned int sum = 0;
    for (char c : msg) {
        sum += static_cast<unsigned char>(c);
    }
    sum %= 256;
    
    std::ostringstream oss;
    oss << std::setfill('0') << std::setw(3) << sum;
    return oss.str();
}

// =============================================================================
// CoinbaseConnector Implementation
// =============================================================================

CoinbaseConnector::CoinbaseConnector(const CoinbaseConfig& config)
    : config_(config) {
    
    // Initialize OpenSSL
    SSL_library_init();
    SSL_load_error_strings();
    OpenSSL_add_all_algorithms();
}

CoinbaseConnector::~CoinbaseConnector() {
    disconnect();
    cleanup_ssl();
}

bool CoinbaseConnector::init_ssl() {
    ssl_ctx_ = SSL_CTX_new(TLS_client_method());
    if (!ssl_ctx_) {
        return false;
    }
    
    // Set minimum TLS version
    SSL_CTX_set_min_proto_version(ssl_ctx_, TLS1_2_VERSION);
    
    // Load system CA certificates
    SSL_CTX_set_default_verify_paths(ssl_ctx_);
    SSL_CTX_set_verify(ssl_ctx_, SSL_VERIFY_PEER, nullptr);
    
    return true;
}

void CoinbaseConnector::cleanup_ssl() {
    if (ssl_) {
        SSL_shutdown(ssl_);
        SSL_free(ssl_);
        ssl_ = nullptr;
    }
    if (ssl_ctx_) {
        SSL_CTX_free(ssl_ctx_);
        ssl_ctx_ = nullptr;
    }
}

bool CoinbaseConnector::tcp_connect() {
    struct addrinfo hints{}, *result;
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    
    std::string host = config_.get_host();
    std::string port = std::to_string(config_.get_port());
    
    int ret = getaddrinfo(host.c_str(), port.c_str(), &hints, &result);
    if (ret != 0) {
        return false;
    }
    
    socket_fd_ = socket(result->ai_family, result->ai_socktype, result->ai_protocol);
    if (socket_fd_ < 0) {
        freeaddrinfo(result);
        return false;
    }
    
    // Set TCP_NODELAY
    int flag = 1;
    setsockopt(socket_fd_, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
    
    // Connect
    ret = ::connect(socket_fd_, result->ai_addr, result->ai_addrlen);
    freeaddrinfo(result);
    
    if (ret < 0) {
        close(socket_fd_);
        socket_fd_ = -1;
        return false;
    }
    
    return true;
}

bool CoinbaseConnector::ssl_handshake() {
    ssl_ = SSL_new(ssl_ctx_);
    if (!ssl_) {
        return false;
    }
    
    SSL_set_fd(ssl_, socket_fd_);
    SSL_set_tlsext_host_name(ssl_, config_.get_host().c_str());
    
    int ret = SSL_connect(ssl_);
    if (ret != 1) {
        return false;
    }
    
    // Verify certificate
    if (SSL_get_verify_result(ssl_) != X509_V_OK) {
        return false;
    }
    
    return true;
}

std::string CoinbaseConnector::generate_signature(const std::string& timestamp,
                                                   const std::string& msg_type,
                                                   int seq_num) {
    // Coinbase FIX uses: HMAC-SHA256(secret, timestamp + msg_type + seq_num + sender + target + passphrase)
    std::string prehash = timestamp + msg_type + std::to_string(seq_num) +
                          config_.sender_comp_id + config_.target_comp_id + config_.passphrase;
    
    // Decode the base64 secret
    auto secret = base64_decode(config_.api_secret);
    
    // Compute HMAC-SHA256
    unsigned char hmac[EVP_MAX_MD_SIZE];
    unsigned int hmac_len;
    
    HMAC(EVP_sha256(), secret.data(), static_cast<int>(secret.size()),
         reinterpret_cast<const unsigned char*>(prehash.data()), prehash.size(),
         hmac, &hmac_len);
    
    return base64_encode(hmac, hmac_len);
}

bool CoinbaseConnector::send_logon() {
    FixMessage msg(static_cast<char>(FixMsgType::LOGON));
    
    std::string timestamp = get_utc_timestamp();
    int seq = seq_num_out_++;
    
    // Generate signature
    std::string signature = generate_signature(timestamp, "A", seq);
    
    msg.set_field(FixTag::EncryptMethod, 0);
    msg.set_field(FixTag::HeartBtInt, config_.heartbeat_interval_sec);
    msg.set_field(FixTag::Password, config_.passphrase);
    msg.set_field(FixTag::RawData, signature);
    
    return send_message(msg);
}

bool CoinbaseConnector::send_logout() {
    FixMessage msg(static_cast<char>(FixMsgType::LOGOUT));
    return send_message(msg);
}

bool CoinbaseConnector::send_heartbeat(const std::string& test_req_id) {
    FixMessage msg(static_cast<char>(FixMsgType::HEARTBEAT));
    if (!test_req_id.empty()) {
        msg.set_field(FixTag::TestReqID, test_req_id);
    }
    return send_message(msg);
}

bool CoinbaseConnector::send_message(const FixMessage& msg) {
    std::lock_guard<std::mutex> lock(send_mutex_);
    
    if (!ssl_ || !connected_) {
        return false;
    }
    
    int seq = seq_num_out_++;
    std::string data = msg.serialize(config_.sender_comp_id, config_.target_comp_id, seq);
    
    int sent = SSL_write(ssl_, data.data(), static_cast<int>(data.size()));
    if (sent != static_cast<int>(data.size())) {
        return false;
    }
    
    messages_sent_++;
    return true;
}

bool CoinbaseConnector::connect() {
    if (connected_) {
        return true;
    }
    
    // Initialize SSL
    if (!init_ssl()) {
        if (connection_cb_) {
            connection_cb_(false, "Failed to initialize SSL");
        }
        return false;
    }
    
    // TCP connect
    if (!tcp_connect()) {
        if (connection_cb_) {
            connection_cb_(false, "Failed to connect to " + config_.get_host());
        }
        cleanup_ssl();
        return false;
    }
    
    // SSL handshake
    if (!ssl_handshake()) {
        if (connection_cb_) {
            connection_cb_(false, "SSL handshake failed");
        }
        close(socket_fd_);
        socket_fd_ = -1;
        cleanup_ssl();
        return false;
    }
    
    running_ = true;
    connected_ = true;
    
    // Start receive thread
    recv_thread_ = std::thread(&CoinbaseConnector::recv_loop, this);
    
    // Send logon
    if (!send_logon()) {
        disconnect();
        if (connection_cb_) {
            connection_cb_(false, "Failed to send logon");
        }
        return false;
    }
    
    // Start heartbeat thread
    heartbeat_thread_ = std::thread(&CoinbaseConnector::heartbeat_loop, this);
    
    return true;
}

void CoinbaseConnector::disconnect() {
    if (!running_) {
        return;
    }
    
    running_ = false;
    
    // Send logout if connected
    if (connected_) {
        send_logout();
    }
    connected_ = false;
    
    // Close SSL
    if (ssl_) {
        SSL_shutdown(ssl_);
    }
    
    // Close socket
    if (socket_fd_ >= 0) {
        close(socket_fd_);
        socket_fd_ = -1;
    }
    
    // Wait for threads
    if (recv_thread_.joinable()) {
        recv_thread_.join();
    }
    if (heartbeat_thread_.joinable()) {
        heartbeat_thread_.join();
    }
    
    cleanup_ssl();
    
    if (connection_cb_) {
        connection_cb_(false, "Disconnected");
    }
}

void CoinbaseConnector::recv_loop() {
    std::string buffer;
    char chunk[4096];
    
    while (running_) {
        int n = SSL_read(ssl_, chunk, sizeof(chunk));
        
        if (n > 0) {
            buffer.append(chunk, n);
            
            // Process complete FIX messages (delimited by checksum field ending with SOH)
            size_t pos;
            while ((pos = buffer.find("10=")) != std::string::npos) {
                // Find the SOH after checksum (checksum is always 3 digits)
                size_t end_pos = buffer.find('\x01', pos + 6);
                if (end_pos == std::string::npos) {
                    break;  // Incomplete message
                }
                
                std::string msg_str = buffer.substr(0, end_pos + 1);
                buffer.erase(0, end_pos + 1);
                
                FixMessage msg = FixMessage::parse(msg_str);
                messages_received_++;
                handle_message(msg);
            }
        } else if (n == 0) {
            // Connection closed
            connected_ = false;
            if (connection_cb_) {
                connection_cb_(false, "Connection closed by server");
            }
            break;
        } else {
            int err = SSL_get_error(ssl_, n);
            if (err != SSL_ERROR_WANT_READ && err != SSL_ERROR_WANT_WRITE) {
                connected_ = false;
                if (connection_cb_) {
                    connection_cb_(false, "SSL read error");
                }
                break;
            }
        }
    }
}

void CoinbaseConnector::heartbeat_loop() {
    while (running_) {
        std::this_thread::sleep_for(std::chrono::seconds(config_.heartbeat_interval_sec));
        
        if (connected_) {
            send_heartbeat();
        }
    }
}

void CoinbaseConnector::handle_message(const FixMessage& msg) {
    switch (static_cast<FixMsgType>(msg.msg_type())) {
        case FixMsgType::LOGON:
            handle_logon(msg);
            break;
        case FixMsgType::LOGOUT:
            handle_logout(msg);
            break;
        case FixMsgType::HEARTBEAT:
            handle_heartbeat(msg);
            break;
        case FixMsgType::TEST_REQUEST:
            handle_test_request(msg);
            break;
        case FixMsgType::REJECT:
        case FixMsgType::BUSINESS_REJECT:
            handle_reject(msg);
            break;
        case FixMsgType::EXECUTION_REPORT:
            handle_execution_report(msg);
            break;
        case FixMsgType::MARKET_DATA_SNAPSHOT:
            handle_market_data_snapshot(msg);
            break;
        case FixMsgType::MARKET_DATA_INCREMENTAL:
            handle_market_data_incremental(msg);
            break;
        default:
            break;
    }
}

void CoinbaseConnector::handle_logon(const FixMessage& /*msg*/) {
    connected_ = true;
    if (connection_cb_) {
        connection_cb_(true, "Logon successful");
    }
}

void CoinbaseConnector::handle_logout(const FixMessage& msg) {
    std::string reason = msg.get_field(FixTag::Text);
    connected_ = false;
    if (connection_cb_) {
        connection_cb_(false, "Logout: " + reason);
    }
}

void CoinbaseConnector::handle_heartbeat(const FixMessage& /*msg*/) {
    // Nothing to do, just confirms connection is alive
}

void CoinbaseConnector::handle_test_request(const FixMessage& msg) {
    std::string test_req_id = msg.get_field(FixTag::TestReqID);
    send_heartbeat(test_req_id);
}

void CoinbaseConnector::handle_reject(const FixMessage& msg) {
    std::string text = msg.get_field(FixTag::Text);
    // Log or handle rejection
    (void)text;
}

void CoinbaseConnector::handle_execution_report(const FixMessage& msg) {
    if (!execution_cb_) return;
    
    ExecutionReport report;
    report.order_id = msg.get_field(FixTag::OrderID);
    report.cl_ord_id = msg.get_field(FixTag::ClOrdID);
    report.symbol = msg.get_field(FixTag::Symbol);
    
    std::string side = msg.get_field(FixTag::Side);
    report.side = side.empty() ? '0' : side[0];
    
    std::string status = msg.get_field(FixTag::OrdStatus);
    report.ord_status = status.empty() ? '0' : status[0];
    
    std::string price = msg.get_field(FixTag::Price);
    if (!price.empty()) report.price = std::stod(price);
    
    std::string qty = msg.get_field(FixTag::OrderQty);
    if (!qty.empty()) report.qty = std::stod(qty);
    
    std::string cum_qty = msg.get_field(FixTag::CumQty);
    if (!cum_qty.empty()) report.filled_qty = std::stod(cum_qty);
    
    std::string avg_px = msg.get_field(FixTag::AvgPx);
    if (!avg_px.empty()) report.avg_price = std::stod(avg_px);
    
    report.text = msg.get_field(FixTag::Text);
    report.timestamp_ms = get_timestamp_ms();
    
    execution_cb_(report);
}

void CoinbaseConnector::handle_market_data_snapshot(const FixMessage& msg) {
    if (!market_data_cb_) return;
    
    std::vector<MarketDataEntry> entries;
    std::string symbol = msg.get_field(FixTag::Symbol);
    
    // Parse NoMDEntries group
    std::string num_entries = msg.get_field(FixTag::NoMDEntries);
    if (num_entries.empty()) return;
    
    // Note: Full implementation would parse repeating groups
    // This is simplified for demonstration
    
    MarketDataEntry entry;
    entry.symbol = symbol;
    entry.timestamp_ms = get_timestamp_ms();
    
    std::string entry_type = msg.get_field(FixTag::MDEntryType);
    if (!entry_type.empty()) entry.entry_type = entry_type[0];
    
    std::string px = msg.get_field(FixTag::MDEntryPx);
    if (!px.empty()) entry.price = std::stod(px);
    
    std::string size = msg.get_field(FixTag::MDEntrySize);
    if (!size.empty()) entry.size = std::stod(size);
    
    entry.order_id = msg.get_field(FixTag::MDEntryID);
    
    entries.push_back(entry);
    market_data_cb_(entries);
}

void CoinbaseConnector::handle_market_data_incremental(const FixMessage& msg) {
    // Similar to snapshot handling
    handle_market_data_snapshot(msg);
}

bool CoinbaseConnector::subscribe_market_data(const std::vector<std::string>& symbols) {
    FixMessage msg(static_cast<char>(FixMsgType::MARKET_DATA_REQUEST));
    
    static int req_id = 1;
    msg.set_field(FixTag::MDReqID, std::to_string(req_id++));
    msg.set_field(FixTag::SubscriptionRequestType, '1');  // Subscribe
    msg.set_field(FixTag::MarketDepth, 0);  // Full book
    
    // Entry types: Bid, Offer, Trade
    msg.set_field(FixTag::NoMDEntryTypes, 3);
    // Note: Repeating groups need special handling
    
    msg.set_field(FixTag::NoRelatedSym, static_cast<int>(symbols.size()));
    // First symbol for simplicity
    if (!symbols.empty()) {
        msg.set_field(FixTag::Symbol, symbols[0]);
    }
    
    return send_message(msg);
}

bool CoinbaseConnector::unsubscribe_market_data(const std::vector<std::string>& symbols) {
    FixMessage msg(static_cast<char>(FixMsgType::MARKET_DATA_REQUEST));
    
    static int req_id = 1000;
    msg.set_field(FixTag::MDReqID, std::to_string(req_id++));
    msg.set_field(FixTag::SubscriptionRequestType, '2');  // Unsubscribe
    
    msg.set_field(FixTag::NoRelatedSym, static_cast<int>(symbols.size()));
    if (!symbols.empty()) {
        msg.set_field(FixTag::Symbol, symbols[0]);
    }
    
    return send_message(msg);
}

bool CoinbaseConnector::send_new_order(const std::string& cl_ord_id,
                                        const std::string& symbol,
                                        char side,
                                        double qty,
                                        double price,
                                        char ord_type) {
    FixMessage msg(static_cast<char>(FixMsgType::NEW_ORDER_SINGLE));
    
    msg.set_field(FixTag::ClOrdID, cl_ord_id);
    msg.set_field(FixTag::Symbol, symbol);
    msg.set_field(FixTag::Side, side);
    msg.set_field(FixTag::OrderQty, qty);
    msg.set_field(FixTag::OrdType, ord_type);
    
    if (ord_type == '2') {  // Limit order
        msg.set_field(FixTag::Price, price);
    }
    
    msg.set_field(FixTag::TimeInForce, '1');  // GTC
    msg.set_field(FixTag::TransactTime, get_utc_timestamp());
    
    return send_message(msg);
}

bool CoinbaseConnector::cancel_order(const std::string& orig_cl_ord_id,
                                      const std::string& cl_ord_id,
                                      const std::string& symbol,
                                      char side) {
    FixMessage msg(static_cast<char>(FixMsgType::ORDER_CANCEL_REQUEST));
    
    msg.set_field(FixTag::OrigClOrdID, orig_cl_ord_id);
    msg.set_field(FixTag::ClOrdID, cl_ord_id);
    msg.set_field(FixTag::Symbol, symbol);
    msg.set_field(FixTag::Side, side);
    msg.set_field(FixTag::TransactTime, get_utc_timestamp());
    
    return send_message(msg);
}

} // namespace coinbase
