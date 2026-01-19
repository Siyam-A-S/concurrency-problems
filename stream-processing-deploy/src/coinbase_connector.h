/**
 * Coinbase FIX API Connector
 * 
 * Connects to Coinbase Exchange FIX API and streams market data
 * into the stream processor.
 * 
 * FIX Protocol: Financial Information eXchange
 * Endpoints:
 *   - Production: tcp+ssl://fix-ord.exchange.coinbase.com:6121
 *   - Sandbox: tcp+ssl://fix-ord.sandbox.exchange.coinbase.com:6121
 */

#pragma once

#include <string>
#include <memory>
#include <thread>
#include <atomic>
#include <functional>
#include <vector>
#include <map>
#include <chrono>
#include <mutex>
#include <openssl/ssl.h>
#include <openssl/err.h>

namespace coinbase {

// =============================================================================
// Configuration
// =============================================================================

struct CoinbaseConfig {
    // API Credentials (from Coinbase Pro/Exchange)
    std::string api_key;
    std::string api_secret;
    std::string passphrase;
    
    // Connection settings
    bool use_sandbox{true};  // Use sandbox for testing
    std::string sender_comp_id;  // Your FIX SenderCompID
    std::string target_comp_id{"Coinbase"};
    
    // Reconnection settings
    bool auto_reconnect{true};
    int reconnect_delay_ms{5000};
    int max_reconnect_attempts{10};
    
    // Heartbeat interval (FIX standard)
    int heartbeat_interval_sec{30};
    
    // Get endpoint based on environment
    std::string get_host() const {
        return use_sandbox 
            ? "fix-ord.sandbox.exchange.coinbase.com"
            : "fix-ord.exchange.coinbase.com";
    }
    
    int get_port() const { return 6121; }
};

// =============================================================================
// FIX Message Types
// =============================================================================

enum class FixMsgType {
    HEARTBEAT = '0',
    TEST_REQUEST = '1',
    LOGON = 'A',
    LOGOUT = '5',
    EXECUTION_REPORT = '8',
    NEW_ORDER_SINGLE = 'D',
    ORDER_CANCEL_REQUEST = 'F',
    ORDER_STATUS_REQUEST = 'H',
    MARKET_DATA_REQUEST = 'V',
    MARKET_DATA_SNAPSHOT = 'W',
    MARKET_DATA_INCREMENTAL = 'X',
    REJECT = '3',
    BUSINESS_REJECT = 'j'
};

// =============================================================================
// FIX Message Builder/Parser
// =============================================================================

class FixMessage {
public:
    FixMessage() = default;
    explicit FixMessage(char msg_type);
    
    // Set fields
    void set_field(int tag, const std::string& value);
    void set_field(int tag, int value);
    void set_field(int tag, double value);
    void set_field(int tag, char value);
    
    // Get fields
    std::string get_field(int tag) const;
    bool has_field(int tag) const;
    
    // Message type
    char msg_type() const;
    
    // Serialize to FIX wire format
    std::string serialize(const std::string& sender, const std::string& target, int seq_num) const;
    
    // Parse from wire format
    static FixMessage parse(const std::string& raw);
    
    // Debug string
    std::string to_string() const;

private:
    char msg_type_{0};
    std::map<int, std::string> fields_;
    
    static std::string compute_checksum(const std::string& msg);
};

// =============================================================================
// Market Data Callback Types
// =============================================================================

struct MarketDataEntry {
    std::string symbol;
    char entry_type;  // '0'=Bid, '1'=Offer, '2'=Trade
    double price{0.0};
    double size{0.0};
    std::string order_id;
    uint64_t timestamp_ms{0};
};

struct ExecutionReport {
    std::string order_id;
    std::string cl_ord_id;
    std::string symbol;
    char side;  // '1'=Buy, '2'=Sell
    char ord_status;  // '0'=New, '1'=PartialFill, '2'=Filled, etc.
    double price{0.0};
    double qty{0.0};
    double filled_qty{0.0};
    double avg_price{0.0};
    std::string text;
    uint64_t timestamp_ms{0};
};

using MarketDataCallback = std::function<void(const std::vector<MarketDataEntry>&)>;
using ExecutionCallback = std::function<void(const ExecutionReport&)>;
using ConnectionCallback = std::function<void(bool connected, const std::string& reason)>;

// =============================================================================
// Coinbase FIX Connector
// =============================================================================

class CoinbaseConnector {
public:
    explicit CoinbaseConnector(const CoinbaseConfig& config);
    ~CoinbaseConnector();
    
    // Disable copy
    CoinbaseConnector(const CoinbaseConnector&) = delete;
    CoinbaseConnector& operator=(const CoinbaseConnector&) = delete;
    
    // Connection
    bool connect();
    void disconnect();
    bool is_connected() const { return connected_; }
    
    // Callbacks
    void set_market_data_callback(MarketDataCallback cb) { market_data_cb_ = std::move(cb); }
    void set_execution_callback(ExecutionCallback cb) { execution_cb_ = std::move(cb); }
    void set_connection_callback(ConnectionCallback cb) { connection_cb_ = std::move(cb); }
    
    // Market Data Subscription
    bool subscribe_market_data(const std::vector<std::string>& symbols);
    bool unsubscribe_market_data(const std::vector<std::string>& symbols);
    
    // Order Operations
    bool send_new_order(const std::string& cl_ord_id,
                        const std::string& symbol,
                        char side,  // '1'=Buy, '2'=Sell
                        double qty,
                        double price,
                        char ord_type = '2');  // '1'=Market, '2'=Limit
    
    bool cancel_order(const std::string& orig_cl_ord_id,
                      const std::string& cl_ord_id,
                      const std::string& symbol,
                      char side);
    
    // Statistics
    uint64_t messages_received() const { return messages_received_; }
    uint64_t messages_sent() const { return messages_sent_; }

private:
    CoinbaseConfig config_;
    
    // SSL/TLS
    SSL_CTX* ssl_ctx_{nullptr};
    SSL* ssl_{nullptr};
    int socket_fd_{-1};
    
    // State
    std::atomic<bool> connected_{false};
    std::atomic<bool> running_{false};
    std::atomic<int> seq_num_out_{1};
    std::atomic<int> seq_num_in_{1};
    
    // Threads
    std::thread recv_thread_;
    std::thread heartbeat_thread_;
    
    // Callbacks
    MarketDataCallback market_data_cb_;
    ExecutionCallback execution_cb_;
    ConnectionCallback connection_cb_;
    
    // Stats
    std::atomic<uint64_t> messages_received_{0};
    std::atomic<uint64_t> messages_sent_{0};
    
    // Mutex for send operations
    std::mutex send_mutex_;
    
    // Internal methods
    bool init_ssl();
    void cleanup_ssl();
    bool tcp_connect();
    bool ssl_handshake();
    bool send_logon();
    bool send_logout();
    bool send_heartbeat(const std::string& test_req_id = "");
    bool send_message(const FixMessage& msg);
    
    void recv_loop();
    void heartbeat_loop();
    
    void handle_message(const FixMessage& msg);
    void handle_logon(const FixMessage& msg);
    void handle_logout(const FixMessage& msg);
    void handle_heartbeat(const FixMessage& msg);
    void handle_test_request(const FixMessage& msg);
    void handle_reject(const FixMessage& msg);
    void handle_execution_report(const FixMessage& msg);
    void handle_market_data_snapshot(const FixMessage& msg);
    void handle_market_data_incremental(const FixMessage& msg);
    
    std::string generate_signature(const std::string& timestamp, 
                                   const std::string& msg_type,
                                   int seq_num);
};

// =============================================================================
// FIX Tag Constants (subset used by Coinbase)
// =============================================================================

namespace FixTag {
    constexpr int BeginString = 8;
    constexpr int BodyLength = 9;
    constexpr int MsgType = 35;
    constexpr int SenderCompID = 49;
    constexpr int TargetCompID = 56;
    constexpr int MsgSeqNum = 34;
    constexpr int SendingTime = 52;
    constexpr int CheckSum = 10;
    
    // Logon
    constexpr int EncryptMethod = 98;
    constexpr int HeartBtInt = 108;
    constexpr int RawData = 96;
    constexpr int Password = 554;
    
    // Order
    constexpr int ClOrdID = 11;
    constexpr int OrderID = 37;
    constexpr int OrigClOrdID = 41;
    constexpr int Symbol = 55;
    constexpr int Side = 54;
    constexpr int OrderQty = 38;
    constexpr int OrdType = 40;
    constexpr int Price = 44;
    constexpr int TimeInForce = 59;
    constexpr int OrdStatus = 39;
    constexpr int ExecType = 150;
    constexpr int LeavesQty = 151;
    constexpr int CumQty = 14;
    constexpr int AvgPx = 6;
    constexpr int Text = 58;
    constexpr int TransactTime = 60;
    
    // Market Data
    constexpr int MDReqID = 262;
    constexpr int SubscriptionRequestType = 263;
    constexpr int MarketDepth = 264;
    constexpr int NoMDEntryTypes = 267;
    constexpr int MDEntryType = 269;
    constexpr int NoRelatedSym = 146;
    constexpr int NoMDEntries = 268;
    constexpr int MDEntryPx = 270;
    constexpr int MDEntrySize = 271;
    constexpr int MDEntryID = 278;
    
    // Session
    constexpr int TestReqID = 112;
    constexpr int RefSeqNum = 45;
    constexpr int RefMsgType = 372;
    constexpr int SessionRejectReason = 373;
}

} // namespace coinbase
