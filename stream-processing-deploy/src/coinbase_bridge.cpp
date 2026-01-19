/**
 * Coinbase Bridge Server
 * 
 * Connects to Coinbase FIX API and streams market data
 * into the local stream processor.
 * 
 * Usage:
 *   ./coinbase-bridge --api-key=XXX --api-secret=XXX --passphrase=XXX
 */

#include "coinbase_connector.h"
#include "network/client.h"

#include <iostream>
#include <csignal>
#include <atomic>
#include <thread>
#include <cstdlib>

using namespace coinbase;
using namespace stream::network;

// Global shutdown flag
std::atomic<bool> g_shutdown{false};

void signal_handler(int signum) {
    std::cout << "\n[BRIDGE] Received signal " << signum << ", shutting down...\n";
    g_shutdown = true;
}

struct BridgeConfig {
    // Coinbase credentials
    std::string api_key;
    std::string api_secret;
    std::string passphrase;
    std::string sender_comp_id;
    bool use_sandbox{true};
    
    // Stream processor connection
    std::string stream_host{"localhost"};
    uint16_t stream_port{9092};
    
    // Subscriptions
    std::vector<std::string> symbols{"BTC-USD", "ETH-USD"};
};

BridgeConfig parse_args(int argc, char* argv[]) {
    BridgeConfig config;
    
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        
        if (arg.find("--api-key=") == 0) {
            config.api_key = arg.substr(10);
        } else if (arg.find("--api-secret=") == 0) {
            config.api_secret = arg.substr(13);
        } else if (arg.find("--passphrase=") == 0) {
            config.passphrase = arg.substr(13);
        } else if (arg.find("--sender-comp-id=") == 0) {
            config.sender_comp_id = arg.substr(17);
        } else if (arg.find("--stream-host=") == 0) {
            config.stream_host = arg.substr(14);
        } else if (arg.find("--stream-port=") == 0) {
            config.stream_port = static_cast<uint16_t>(std::stoi(arg.substr(14)));
        } else if (arg.find("--symbols=") == 0) {
            config.symbols.clear();
            std::string symbols = arg.substr(10);
            size_t pos = 0;
            while ((pos = symbols.find(',')) != std::string::npos) {
                config.symbols.push_back(symbols.substr(0, pos));
                symbols.erase(0, pos + 1);
            }
            if (!symbols.empty()) {
                config.symbols.push_back(symbols);
            }
        } else if (arg == "--production") {
            config.use_sandbox = false;
        } else if (arg == "--sandbox") {
            config.use_sandbox = true;
        } else if (arg == "--help" || arg == "-h") {
            std::cout << "Coinbase FIX Bridge\n"
                      << "Usage: " << argv[0] << " [options]\n\n"
                      << "Required:\n"
                      << "  --api-key=KEY           Coinbase API key\n"
                      << "  --api-secret=SECRET     Coinbase API secret (base64)\n"
                      << "  --passphrase=PASS       Coinbase API passphrase\n"
                      << "  --sender-comp-id=ID     Your FIX SenderCompID\n"
                      << "\nOptional:\n"
                      << "  --sandbox               Use sandbox environment (default)\n"
                      << "  --production            Use production environment\n"
                      << "  --stream-host=HOST      Stream processor host (default: localhost)\n"
                      << "  --stream-port=PORT      Stream processor port (default: 9092)\n"
                      << "  --symbols=SYM1,SYM2     Symbols to subscribe (default: BTC-USD,ETH-USD)\n"
                      << "  --help, -h              Show this help\n";
            std::exit(0);
        }
    }
    
    // Override from environment
    if (const char* env = std::getenv("COINBASE_API_KEY")) config.api_key = env;
    if (const char* env = std::getenv("COINBASE_API_SECRET")) config.api_secret = env;
    if (const char* env = std::getenv("COINBASE_PASSPHRASE")) config.passphrase = env;
    if (const char* env = std::getenv("COINBASE_SENDER_COMP_ID")) config.sender_comp_id = env;
    if (const char* env = std::getenv("STREAM_HOST")) config.stream_host = env;
    if (const char* env = std::getenv("STREAM_PORT")) config.stream_port = static_cast<uint16_t>(std::stoi(env));
    if (const char* env = std::getenv("COINBASE_USE_SANDBOX")) {
        std::string val = env;
        config.use_sandbox = (val == "true" || val == "1" || val == "yes");
    }
    if (const char* env = std::getenv("COINBASE_SYMBOLS")) {
        config.symbols.clear();
        std::string symbols = env;
        size_t pos = 0;
        while ((pos = symbols.find(',')) != std::string::npos) {
            config.symbols.push_back(symbols.substr(0, pos));
            symbols.erase(0, pos + 1);
        }
        if (!symbols.empty()) {
            config.symbols.push_back(symbols);
        }
    }
    
    return config;
}

void print_banner() {
    std::cout << R"(
╔═══════════════════════════════════════════════════════════╗
║          Coinbase FIX API Bridge v1.0                     ║
║      Streams Market Data to Stream Processor              ║
╚═══════════════════════════════════════════════════════════╝
)" << std::endl;
}

int main(int argc, char* argv[]) {
    try {
        print_banner();
        std::cout << "[BRIDGE] Starting initialization...\n" << std::flush;
        
        auto config = parse_args(argc, argv);
        std::cout << "[BRIDGE] Parsed arguments\n" << std::flush;
        
        // Validate required fields
        if (config.api_key.empty() || config.api_secret.empty() || 
            config.passphrase.empty() || config.sender_comp_id.empty()) {
            std::cerr << "[BRIDGE] ERROR: Missing required credentials\n"
                      << "  api_key: " << (config.api_key.empty() ? "MISSING" : "OK") << "\n"
                      << "  api_secret: " << (config.api_secret.empty() ? "MISSING" : "OK") << "\n"  
                      << "  passphrase: " << (config.passphrase.empty() ? "MISSING" : "OK") << "\n"
                      << "  sender_comp_id: " << (config.sender_comp_id.empty() ? "MISSING" : "OK") << "\n"
                      << "         Use --help for usage information\n";
            return 1;
        }
        
        std::cout << "[BRIDGE] Credentials validated\n" << std::flush;
        
        // Setup signal handlers
        std::signal(SIGINT, signal_handler);
        std::signal(SIGTERM, signal_handler);
        
        std::cout << "[BRIDGE] Configuration:\n"
                  << "  Environment:    " << (config.use_sandbox ? "SANDBOX" : "PRODUCTION") << "\n"
                  << "  FIX Endpoint:   " << (config.use_sandbox 
                        ? "fix-ord.sandbox.exchange.coinbase.com:6121"
                        : "fix-ord.exchange.coinbase.com:6121") << "\n"
                  << "  Stream Target:  " << config.stream_host << ":" << config.stream_port << "\n"
                  << "  Symbols:        ";
        for (const auto& s : config.symbols) std::cout << s << " ";
        std::cout << "\n" << std::flush;
        
        // Connect to stream processor
        std::cout << "[BRIDGE] Creating stream client...\n" << std::flush;
        ClientConfig stream_config;
        stream_config.host = config.stream_host;
        stream_config.port = config.stream_port;
        
        StreamClient stream_client(stream_config);
        std::cout << "[BRIDGE] Stream client created\n" << std::flush;
        
        std::cout << "[BRIDGE] Connecting to stream processor...\n" << std::flush;
        if (!stream_client.connect()) {
            std::cerr << "[BRIDGE] ERROR: Failed to connect to stream processor at "
                      << config.stream_host << ":" << config.stream_port << "\n";
            return 1;
        }
        std::cout << "[BRIDGE] ✓ Connected to stream processor\n" << std::flush;
        
        // Create topics for market data
        for (const auto& symbol : config.symbols) {
            std::string topic_name = "coinbase." + symbol;
            // Replace - with _ for topic name
            std::replace(topic_name.begin(), topic_name.end(), '-', '_');
            
            stream_client.create_topic(topic_name, 4);
            std::cout << "[BRIDGE] Created topic: " << topic_name << "\n";
        }
        
        // Also create execution topic
        stream_client.create_topic("coinbase.executions", 4);
        std::cout << "[BRIDGE] Created topic: coinbase.executions\n" << std::flush;
        
        // Setup Coinbase connector
        std::cout << "[BRIDGE] Setting up Coinbase connector...\n" << std::flush;
        CoinbaseConfig cb_config;
        cb_config.api_key = config.api_key;
        cb_config.api_secret = config.api_secret;
        cb_config.passphrase = config.passphrase;
        cb_config.sender_comp_id = config.sender_comp_id;
        cb_config.use_sandbox = config.use_sandbox;
        
        CoinbaseConnector connector(cb_config);
        std::cout << "[BRIDGE] Coinbase connector created\n" << std::flush;
        
        // Set callbacks
        connector.set_connection_callback([](bool connected, const std::string& reason) {
            if (connected) {
                std::cout << "[COINBASE] ✓ Connected: " << reason << "\n";
            } else {
                std::cout << "[COINBASE] ✗ Disconnected: " << reason << "\n";
            }
        });
        
        connector.set_market_data_callback([&stream_client](const std::vector<MarketDataEntry>& entries) {
            for (const auto& entry : entries) {
                std::string topic = "coinbase." + entry.symbol;
                std::replace(topic.begin(), topic.end(), '-', '_');
                
                // Format: type|price|size|timestamp
                char type_char = entry.entry_type;
                std::string type_str = (type_char == '0') ? "BID" : 
                                       (type_char == '1') ? "ASK" : "TRADE";
                
                std::string value = type_str + "|" + 
                                   std::to_string(entry.price) + "|" +
                                   std::to_string(entry.size) + "|" +
                                   std::to_string(entry.timestamp_ms);
                
                stream_client.produce(topic, entry.order_id, value);
            }
        });
        
        connector.set_execution_callback([&stream_client](const ExecutionReport& report) {
            std::string topic = "coinbase.executions";
            
            // Format: order_id|symbol|side|status|price|qty|filled|avg_price
            char side_char = report.side;
            std::string side_str = (side_char == '1') ? "BUY" : "SELL";
            
            std::string value = report.order_id + "|" +
                               report.symbol + "|" +
                               side_str + "|" +
                               std::string(1, report.ord_status) + "|" +
                               std::to_string(report.price) + "|" +
                               std::to_string(report.qty) + "|" +
                               std::to_string(report.filled_qty) + "|" +
                               std::to_string(report.avg_price);
            
            stream_client.produce(topic, report.cl_ord_id, value);
        });
        
        // Connect to Coinbase
        std::cout << "[BRIDGE] Connecting to Coinbase FIX API...\n" << std::flush;
        if (!connector.connect()) {
            std::cerr << "[BRIDGE] WARNING: Initial connection to Coinbase failed, will retry...\n";
        }
        
        // Wait for connection to establish
        std::this_thread::sleep_for(std::chrono::seconds(2));
        
        if (connector.is_connected()) {
            std::cout << "[BRIDGE] ✓ Connected to Coinbase FIX API\n" << std::flush;
            
            // Subscribe to market data
            std::cout << "[BRIDGE] Subscribing to market data...\n" << std::flush;
            connector.subscribe_market_data(config.symbols);
        } else {
            std::cout << "[BRIDGE] WARNING: Not yet connected to Coinbase, will retry...\n" << std::flush;
        }
        
        std::cout << "[BRIDGE] ✓ Bridge started!\n"
                  << "[BRIDGE] Streaming data from Coinbase to stream processor...\n\n" << std::flush;
        
        // Main loop
        uint64_t last_stats = 0;
        while (!g_shutdown) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            
            // Print stats every 30 seconds
            auto now = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::steady_clock::now().time_since_epoch()).count();
            
            if (now - last_stats >= 30) {
                last_stats = now;
                std::cout << "[STATS] Messages received: " << connector.messages_received()
                          << " | Messages sent: " << connector.messages_sent()
                          << " | Connected: " << (connector.is_connected() ? "yes" : "no")
                          << "\n" << std::flush;
            }
            
            // Check if still connected
            if (!connector.is_connected()) {
                std::cout << "[BRIDGE] Connection lost, attempting reconnect...\n" << std::flush;
                connector.connect();
                std::this_thread::sleep_for(std::chrono::seconds(5));
            }
        }
        
        // Shutdown
        std::cout << "[BRIDGE] Shutting down...\n" << std::flush;
        connector.unsubscribe_market_data(config.symbols);
        connector.disconnect();
        stream_client.disconnect();
        
        std::cout << "[BRIDGE] Goodbye!\n" << std::flush;
        return 0;
        
    } catch (const std::exception& e) {
        std::cerr << "[BRIDGE] FATAL ERROR: " << e.what() << "\n";
        return 1;
    } catch (...) {
        std::cerr << "[BRIDGE] FATAL ERROR: Unknown exception\n";
        return 1;
    }
}
