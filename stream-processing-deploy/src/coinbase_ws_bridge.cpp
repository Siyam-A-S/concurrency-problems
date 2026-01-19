/**
 * Coinbase WebSocket Bridge Server
 * 
 * Connects to Coinbase's public WebSocket feed and streams market data
 * into the local stream processor.
 * 
 * This uses the PUBLIC WebSocket API which doesn't require authentication
 * for market data (tickers, trades, order book updates).
 */

#include "coinbase_websocket.h"
#include "network/client.h"

#include <iostream>
#include <csignal>
#include <atomic>
#include <thread>
#include <cstdlib>
#include <algorithm>

using namespace coinbase::ws;
using namespace stream::network;

// Global shutdown flag
std::atomic<bool> g_shutdown{false};

void signal_handler(int signum) {
    std::cout << "\n[BRIDGE] Received signal " << signum << ", shutting down...\n";
    g_shutdown = true;
}

struct BridgeConfig {
    bool use_sandbox{true};
    
    // Stream processor connection
    std::string stream_host{"localhost"};
    uint16_t stream_port{9092};
    
    // Subscriptions
    std::vector<std::string> symbols{"BTC-USD", "ETH-USD"};
    std::vector<std::string> channels{"ticker", "matches"};
};

BridgeConfig parse_args(int argc, char* argv[]) {
    BridgeConfig config;
    
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        
        if (arg.find("--stream-host=") == 0) {
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
        } else if (arg.find("--channels=") == 0) {
            config.channels.clear();
            std::string channels = arg.substr(11);
            size_t pos = 0;
            while ((pos = channels.find(',')) != std::string::npos) {
                config.channels.push_back(channels.substr(0, pos));
                channels.erase(0, pos + 1);
            }
            if (!channels.empty()) {
                config.channels.push_back(channels);
            }
        } else if (arg == "--production") {
            config.use_sandbox = false;
        } else if (arg == "--sandbox") {
            config.use_sandbox = true;
        } else if (arg == "--help" || arg == "-h") {
            std::cout << "Coinbase WebSocket Bridge\n"
                      << "Usage: " << argv[0] << " [options]\n\n"
                      << "Options:\n"
                      << "  --sandbox               Use sandbox environment (default)\n"
                      << "  --production            Use production environment (real data!)\n"
                      << "  --stream-host=HOST      Stream processor host (default: localhost)\n"
                      << "  --stream-port=PORT      Stream processor port (default: 9092)\n"
                      << "  --symbols=SYM1,SYM2     Symbols to subscribe (default: BTC-USD,ETH-USD)\n"
                      << "  --channels=CH1,CH2      Channels to subscribe (default: ticker,matches)\n"
                      << "  --help, -h              Show this help\n"
                      << "\nAvailable channels:\n"
                      << "  ticker    - Real-time price updates\n"
                      << "  matches   - Trade executions\n"
                      << "  level2    - Order book updates\n"
                      << "  heartbeat - Connection keepalive\n";
            std::exit(0);
        }
    }
    
    // Override from environment
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
    if (const char* env = std::getenv("COINBASE_CHANNELS")) {
        config.channels.clear();
        std::string channels = env;
        size_t pos = 0;
        while ((pos = channels.find(',')) != std::string::npos) {
            config.channels.push_back(channels.substr(0, pos));
            channels.erase(0, pos + 1);
        }
        if (!channels.empty()) {
            config.channels.push_back(channels);
        }
    }
    
    return config;
}

void print_banner() {
    std::cout << R"(
╔═══════════════════════════════════════════════════════════╗
║       Coinbase WebSocket Bridge v1.0                      ║
║    Streams Real-Time Market Data to Stream Processor      ║
║                                                           ║
║    No authentication required for public market data!     ║
╚═══════════════════════════════════════════════════════════╝
)" << std::endl;
}

int main(int argc, char* argv[]) {
    try {
        print_banner();
        std::cout << "[BRIDGE] Starting initialization...\n" << std::flush;
        
        auto config = parse_args(argc, argv);
        std::cout << "[BRIDGE] Parsed arguments\n" << std::flush;
        
        // Setup signal handlers
        std::signal(SIGINT, signal_handler);
        std::signal(SIGTERM, signal_handler);
        
        std::cout << "[BRIDGE] Configuration:\n"
                  << "  Environment:    " << (config.use_sandbox ? "SANDBOX" : "PRODUCTION") << "\n"
                  << "  WebSocket Feed: " << (config.use_sandbox 
                        ? "ws-feed-public.sandbox.exchange.coinbase.com"
                        : "ws-feed.exchange.coinbase.com") << "\n"
                  << "  Stream Target:  " << config.stream_host << ":" << config.stream_port << "\n"
                  << "  Symbols:        ";
        for (const auto& s : config.symbols) std::cout << s << " ";
        std::cout << "\n  Channels:       ";
        for (const auto& c : config.channels) std::cout << c << " ";
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
        
        // Also create trades and tickers topics
        stream_client.create_topic("coinbase.trades", 8);
        stream_client.create_topic("coinbase.tickers", 4);
        std::cout << "[BRIDGE] Created topic: coinbase.trades\n";
        std::cout << "[BRIDGE] Created topic: coinbase.tickers\n" << std::flush;
        
        // Setup WebSocket connector
        std::cout << "[BRIDGE] Setting up WebSocket connector...\n" << std::flush;
        WebSocketConnector connector(config.use_sandbox);
        
        // Set callbacks
        connector.set_connection_callback([](bool connected, const std::string& reason) {
            if (connected) {
                std::cout << "[WEBSOCKET] ✓ " << reason << "\n" << std::flush;
            } else {
                std::cout << "[WEBSOCKET] ✗ " << reason << "\n" << std::flush;
            }
        });
        
        std::atomic<uint64_t> trades_count{0};
        std::atomic<uint64_t> tickers_count{0};
        
        connector.set_trade_callback([&stream_client, &trades_count](const Trade& trade) {
            trades_count++;
            
            // Send to symbol-specific topic
            std::string topic = "coinbase." + trade.product_id;
            std::replace(topic.begin(), topic.end(), '-', '_');
            
            // Format: TRADE|side|price|size|time|trade_id
            std::string value = "TRADE|" + trade.side + "|" +
                               std::to_string(trade.price) + "|" +
                               std::to_string(trade.size) + "|" +
                               trade.time + "|" + trade.trade_id;
            
            stream_client.produce(topic, trade.product_id + ":" + trade.trade_id, value);
            
            // Also send to consolidated trades topic
            stream_client.produce("coinbase.trades", trade.product_id + ":" + trade.trade_id, 
                                 trade.product_id + "|" + value);
        });
        
        connector.set_ticker_callback([&stream_client, &tickers_count](const Ticker& ticker) {
            tickers_count++;
            
            // Send to symbol-specific topic
            std::string topic = "coinbase." + ticker.product_id;
            std::replace(topic.begin(), topic.end(), '-', '_');
            
            // Format: TICKER|price|bid|ask|24h_vol|time
            std::string value = "TICKER|" +
                               std::to_string(ticker.price) + "|" +
                               std::to_string(ticker.best_bid) + "|" +
                               std::to_string(ticker.best_ask) + "|" +
                               std::to_string(ticker.volume_24h) + "|" +
                               ticker.time;
            
            stream_client.produce(topic, ticker.product_id + ":ticker", value);
            
            // Also send to consolidated tickers topic
            stream_client.produce("coinbase.tickers", ticker.product_id + ":ticker",
                                 ticker.product_id + "|" + value);
        });
        
        // Connect to Coinbase WebSocket
        std::cout << "[BRIDGE] Connecting to Coinbase WebSocket...\n" << std::flush;
        if (!connector.connect()) {
            std::cerr << "[BRIDGE] ERROR: Failed to connect to Coinbase WebSocket\n";
            return 1;
        }
        
        // Wait a moment for connection to stabilize
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        
        // Subscribe to market data
        std::cout << "[BRIDGE] Subscribing to market data...\n" << std::flush;
        if (!connector.subscribe(config.symbols, config.channels)) {
            std::cerr << "[BRIDGE] WARNING: Subscription may have failed\n";
        }
        
        std::cout << "[BRIDGE] ✓ Bridge started!\n"
                  << "[BRIDGE] Streaming real-time data from Coinbase...\n\n" << std::flush;
        
        // Main loop
        uint64_t last_stats = 0;
        uint64_t last_trades = 0;
        uint64_t last_tickers = 0;
        
        while (!g_shutdown) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            
            // Print stats every 10 seconds
            auto now = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::steady_clock::now().time_since_epoch()).count();
            
            if (now - last_stats >= 10) {
                uint64_t current_trades = trades_count.load();
                uint64_t current_tickers = tickers_count.load();
                
                double trades_per_sec = (current_trades - last_trades) / 10.0;
                double tickers_per_sec = (current_tickers - last_tickers) / 10.0;
                
                std::cout << "[STATS] Messages: " << connector.messages_received()
                          << " | Trades: " << current_trades << " (" << trades_per_sec << "/s)"
                          << " | Tickers: " << current_tickers << " (" << tickers_per_sec << "/s)"
                          << " | Connected: " << (connector.is_connected() ? "yes" : "no")
                          << "\n" << std::flush;
                
                last_stats = now;
                last_trades = current_trades;
                last_tickers = current_tickers;
            }
            
            // Check if still connected
            if (!connector.is_connected()) {
                std::cout << "[BRIDGE] Connection lost, attempting reconnect...\n" << std::flush;
                if (connector.connect()) {
                    connector.subscribe(config.symbols, config.channels);
                }
                std::this_thread::sleep_for(std::chrono::seconds(5));
            }
        }
        
        // Shutdown
        std::cout << "[BRIDGE] Shutting down...\n" << std::flush;
        connector.unsubscribe(config.symbols, config.channels);
        connector.disconnect();
        stream_client.disconnect();
        
        std::cout << "[BRIDGE] Final stats - Trades: " << trades_count 
                  << " | Tickers: " << tickers_count << "\n";
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
