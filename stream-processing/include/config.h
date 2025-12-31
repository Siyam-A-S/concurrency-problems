#pragma once

#include <cstdint>
#include <string>
#include <memory>
#include <mutex>
#include <shared_mutex>

namespace stream {

/**
 * @brief Runtime configuration - MVP version
 * 
 * Design rationale for MVP:
 * - Uses std::shared_mutex for thread-safe read/write access
 * - Readers don't block each other (shared lock)
 * - Writers get exclusive access
 * - Simple, correct, easy to understand
 * 
 * Performance characteristics:
 * - Reads: ~50-100ns (shared lock acquisition)
 * - Writes: ~100-200ns (exclusive lock acquisition)
 * - Good enough for configuration (rarely written, occasionally read)
 * 
 * Future optimization path (profile first!):
 * - Use atomics for frequently read, rarely written values
 * - Copy-on-write for entire config struct
 * - RCU (Read-Copy-Update) for extreme read performance
 * 
 * When to upgrade:
 * - If profiling shows config reads are a bottleneck (unlikely)
 * - If you need sub-microsecond config access
 */
struct BrokerConfig {
    // Network configuration
    uint16_t port{9092};                              // Broker listen port
    uint32_t max_connections{10000};                  // Max concurrent clients
    uint32_t socket_rcvbuf_bytes{128 * 1024};         // 128KB receive buffer
    uint32_t socket_sndbuf_bytes{128 * 1024};         // 128KB send buffer
    
    // Bandwidth management
    uint64_t bandwidth_limit_bytes_per_sec{1ULL * 1024 * 1024 * 1024}; // 1 GB/s
    bool bandwidth_throttling_enabled{false};
    
    // Message loss tolerance (0.0 = no loss allowed, 1.0 = can drop all)
    double message_loss_tolerance{0.0};               // Default: At-least-once
    
    // Topic management
    bool auto_create_topics{true};                    // Dynamic topic creation
    uint32_t default_partition_count{8};              // Partitions per topic
    uint32_t default_replication_factor{3};           // Replicas per partition
    
    // Reliability modes
    bool failsafe_mode{false};                        // true = sync, false = async
    uint32_t min_insync_replicas{2};                  // For failsafe mode
    
    // Batching configuration (trade-off: latency vs throughput)
    uint32_t max_batch_size{100};                     // Messages per batch
    uint32_t batch_timeout_ms{10};                    // Max wait time for batch
    
    // Memory management
    uint64_t message_pool_size{100000};               // Pre-allocated messages
    uint64_t buffer_pool_size{10000};                 // Pre-allocated buffers
    
    // Storage configuration
    uint64_t log_segment_bytes{1ULL * 1024 * 1024 * 1024}; // 1GB per segment
    uint64_t log_retention_ms{7 * 24 * 3600 * 1000ULL};    // 7 days
    bool log_compression_enabled{true};
    uint32_t log_flush_interval_ms{1000};             // fsync interval
    
    // Performance tuning
    bool zero_copy_enabled{true};                     // Use sendfile() when possible
    bool numa_aware{false};                           // Disabled for MVP simplicity
    uint32_t io_thread_count{4};                      // Network I/O threads
    uint32_t worker_thread_count{8};                  // Message processing threads
    
    /**
     * @brief Load configuration from file
     * Supports JSON format for MVP
     * TODO: Implement actual file loading
     */
    bool load_from_file(const std::string& /* path */) {
        // MVP: Not implemented yet
        return false;
    }
    
    /**
     * @brief Save current configuration to file
     * TODO: Implement actual file saving
     */
    bool save_to_file(const std::string& /* path */) const {
        // MVP: Not implemented yet
        return false;
    }
    
    /**
     * @brief Dump current configuration as string
     * Useful for debugging and monitoring
     */
    std::string to_string() const {
        // MVP: Simple implementation
        return "BrokerConfig{port=" + std::to_string(port) + 
               ", max_connections=" + std::to_string(max_connections) + 
               ", ...}";
    }
    
    /**
     * @brief Validate configuration values
     * Returns error message if invalid, empty string if valid
     */
    std::string validate() const {
        if (port == 0) return "port cannot be 0";
        if (max_connections == 0) return "max_connections cannot be 0";
        if (worker_thread_count == 0) return "worker_thread_count cannot be 0";
        if (message_loss_tolerance < 0.0 || message_loss_tolerance > 1.0) {
            return "message_loss_tolerance must be between 0.0 and 1.0";
        }
        return ""; // Valid
    }
};

/**
 * @brief Thread-safe configuration manager - MVP version
 * 
 * Uses reader-writer lock for efficient concurrent access:
 * - Multiple readers can access config simultaneously
 * - Writers get exclusive access
 * 
 * Usage:
 *   // Reading config (shared lock)
 *   auto config = ConfigManager::instance().get_config();
 *   uint16_t port = config.port;
 *   
 *   // Updating config (exclusive lock)
 *   ConfigManager::instance().update([](BrokerConfig& cfg) {
 *       cfg.port = 9093;
 *   });
 */
class ConfigManager {
public:
    /**
     * @brief Get singleton instance (thread-safe)
     */
    static ConfigManager& instance() {
        static ConfigManager instance;
        return instance;
    }
    
    /**
     * @brief Get a copy of current configuration (thread-safe read)
     * Returns a copy to avoid holding lock while using config
     */
    BrokerConfig get_config() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return config_;
    }
    
    /**
     * @brief Update configuration with a modifier function (thread-safe write)
     * 
     * @param modifier Function that modifies the config
     * @return true if update was valid, false if validation failed
     * 
     * Example:
     *   manager.update([](BrokerConfig& cfg) {
     *       cfg.port = 9093;
     *       cfg.max_connections = 5000;
     *   });
     */
    template<typename Func>
    bool update(Func modifier) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        
        // Apply modifications
        modifier(config_);
        
        // Validate new configuration
        std::string error = config_.validate();
        if (!error.empty()) {
            // TODO: Log validation error
            return false;
        }
        
        return true;
    }
    
    /**
     * @brief Replace entire configuration
     */
    bool set_config(const BrokerConfig& new_config) {
        std::string error = new_config.validate();
        if (!error.empty()) {
            return false;
        }
        
        std::unique_lock<std::shared_mutex> lock(mutex_);
        config_ = new_config;
        return true;
    }
    
    /**
     * @brief Load configuration from file
     */
    bool load(const std::string& path) {
        BrokerConfig new_config;
        if (!new_config.load_from_file(path)) {
            return false;
        }
        return set_config(new_config);
    }
    
    /**
     * @brief Save configuration to file
     */
    bool save(const std::string& path) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return config_.save_to_file(path);
    }
    
    // Delete copy/move (singleton pattern)
    ConfigManager(const ConfigManager&) = delete;
    ConfigManager& operator=(const ConfigManager&) = delete;
    
private:
    ConfigManager() = default;
    
    BrokerConfig config_;
    mutable std::shared_mutex mutex_;
};

/**
 * @brief RAII helper for temporary config changes (useful for testing)
 * 
 * Usage:
 *   {
 *       ConfigScope scope([](BrokerConfig& cfg) { cfg.port = 9999; });
 *       // ... test with modified config ...
 *   } // Original config restored here
 */
class ConfigScope {
public:
    template<typename Func>
    explicit ConfigScope(Func modifier) 
        : original_(ConfigManager::instance().get_config()) {
        ConfigManager::instance().update(modifier);
    }
    
    ~ConfigScope() {
        ConfigManager::instance().set_config(original_);
    }
    
    // Non-copyable
    ConfigScope(const ConfigScope&) = delete;
    ConfigScope& operator=(const ConfigScope&) = delete;
    
private:
    BrokerConfig original_;
};

} // namespace stream
