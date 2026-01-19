/**
 * Stream Processor Server - Main Entry Point
 * 
 * Starts the TCP server and exposes the stream processing API
 * for external clients to connect and process streams.
 */

#include "network/tcp_server.h"
#include "broker.h"
#include "config.h"

#include <iostream>
#include <csignal>
#include <atomic>
#include <memory>
#include <string>
#include <cstdlib>
#include <sys/stat.h>

using namespace stream;
using namespace stream::network;

// Global shutdown flag
std::atomic<bool> g_shutdown{false};

// Signal handler
void signal_handler(int signum) {
    std::cout << "\n[SERVER] Received signal " << signum << ", shutting down...\n";
    g_shutdown = true;
}

// Parse command line arguments
struct ServerOptions {
    std::string data_dir = "/data";
    std::string bind_address = "0.0.0.0";
    uint16_t port = 9092;
    size_t io_threads = 4;
    size_t max_connections = 10000;
};

ServerOptions parse_args(int argc, char* argv[]) {
    ServerOptions opts;
    
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        
        if (arg.find("--data-dir=") == 0) {
            opts.data_dir = arg.substr(11);
        } else if (arg.find("--port=") == 0) {
            opts.port = static_cast<uint16_t>(std::stoi(arg.substr(7)));
        } else if (arg.find("--bind=") == 0) {
            opts.bind_address = arg.substr(7);
        } else if (arg.find("--io-threads=") == 0) {
            opts.io_threads = std::stoul(arg.substr(13));
        } else if (arg.find("--max-connections=") == 0) {
            opts.max_connections = std::stoul(arg.substr(18));
        } else if (arg == "--help" || arg == "-h") {
            std::cout << "Stream Processor Server\n"
                      << "Usage: " << argv[0] << " [options]\n\n"
                      << "Options:\n"
                      << "  --data-dir=PATH       Data directory (default: /data)\n"
                      << "  --port=PORT           Listen port (default: 9092)\n"
                      << "  --bind=ADDRESS        Bind address (default: 0.0.0.0)\n"
                      << "  --io-threads=N        I/O threads (default: 4)\n"
                      << "  --max-connections=N   Max connections (default: 10000)\n"
                      << "  --help, -h            Show this help\n";
            std::exit(0);
        }
    }
    
    // Override from environment variables
    if (const char* env = std::getenv("STREAM_PORT")) {
        opts.port = static_cast<uint16_t>(std::stoi(env));
    }
    if (const char* env = std::getenv("STREAM_BIND")) {
        opts.bind_address = env;
    }
    if (const char* env = std::getenv("STREAM_IO_THREADS")) {
        opts.io_threads = std::stoul(env);
    }
    if (const char* env = std::getenv("STREAM_MAX_CONNECTIONS")) {
        opts.max_connections = std::stoul(env);
    }
    if (const char* env = std::getenv("STREAM_DATA_DIR")) {
        opts.data_dir = env;
    }
    
    return opts;
}

void print_banner() {
    std::cout << R"(
╔═══════════════════════════════════════════════════════════╗
║           Stream Processor Server v1.0                    ║
║       High-Performance Message Streaming System           ║
╚═══════════════════════════════════════════════════════════╝
)" << std::endl;
}

int main(int argc, char* argv[]) {
    print_banner();
    
    // Parse arguments
    auto opts = parse_args(argc, argv);
    
    // Setup signal handlers
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);
    
    // Create data directory if it doesn't exist (C++17 compatible)
    mkdir(opts.data_dir.c_str(), 0755);
    
    std::cout << "[SERVER] Configuration:\n"
              << "  Data directory: " << opts.data_dir << "\n"
              << "  Bind address:   " << opts.bind_address << "\n"
              << "  Port:           " << opts.port << "\n"
              << "  I/O threads:    " << opts.io_threads << "\n"
              << "  Max connections:" << opts.max_connections << "\n"
              << std::endl;
    
    // Create broker
    std::cout << "[SERVER] Initializing broker...\n";
    auto broker = std::make_shared<Broker>(opts.data_dir);
    
    // Create request handler
    BrokerRequestHandler handler(broker);
    
    // Configure server
    ServerConfig server_config;
    server_config.bind_address = opts.bind_address;
    server_config.port = opts.port;
    server_config.num_io_threads = opts.io_threads;
    server_config.max_connections = opts.max_connections;
    server_config.idle_timeout = std::chrono::seconds(300);  // 5 min idle timeout
    
    // Create and start server
    TcpServer server(server_config);
    server.set_request_handler([&handler](Connection::Ptr conn, RequestType type, const std::vector<uint8_t>& payload) {
        return handler.handle(conn, type, payload);
    });
    
    std::cout << "[SERVER] Starting TCP server...\n";
    if (!server.start()) {
        std::cerr << "[SERVER] ERROR: Failed to start server on " 
                  << opts.bind_address << ":" << opts.port << "\n";
        return 1;
    }
    
    std::cout << "[SERVER] ✓ Server started successfully!\n"
              << "[SERVER] Listening on " << opts.bind_address << ":" << server.bound_port() << "\n"
              << "[SERVER] Ready to accept connections...\n"
              << std::endl;
    
    // Print available APIs
    std::cout << "[SERVER] Available APIs:\n"
              << "  - METADATA        : Get broker metadata\n"
              << "  - CREATE_TOPIC    : Create a new topic\n"
              << "  - DELETE_TOPIC    : Delete a topic\n"
              << "  - LIST_TOPICS     : List all topics\n"
              << "  - PRODUCE         : Send messages to a topic\n"
              << "  - FETCH           : Consume messages from a topic\n"
              << "  - JOIN_GROUP      : Join a consumer group\n"
              << "  - LEAVE_GROUP     : Leave a consumer group\n"
              << "  - COMMIT_OFFSET   : Commit consumer offset\n"
              << "  - FETCH_OFFSET    : Get committed offset\n"
              << std::endl;
    
    // Main loop - wait for shutdown signal
    while (!g_shutdown) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        
        // Periodically print stats
        static auto last_stats = std::chrono::steady_clock::now();
        auto now = std::chrono::steady_clock::now();
        if (now - last_stats > std::chrono::seconds(60)) {
            last_stats = now;
            const auto& stats = server.stats();
            auto broker_stats = broker->get_stats();
            
            std::cout << "[STATS] Connections: " << stats.active_connections 
                      << " | Requests: " << stats.total_requests
                      << " | Topics: " << broker_stats.topic_count
                      << " | Messages: " << broker_stats.total_messages
                      << "\n";
        }
    }
    
    // Shutdown
    std::cout << "[SERVER] Shutting down...\n";
    server.stop();
    broker->shutdown();
    
    std::cout << "[SERVER] Goodbye!\n";
    return 0;
}
