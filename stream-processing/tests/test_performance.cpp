#include "broker.h"
#include "network/tcp_server.h"
#include "network/client.h"
#include <iostream>
#include <iomanip>
#include <thread>
#include <atomic>
#include <vector>
#include <chrono>
#include <numeric>
#include <algorithm>
#include <cmath>
#include <filesystem>

using namespace stream;
using namespace stream::network;
namespace fs = std::filesystem;

// =============================================================================
// Utilities
// =============================================================================

class TempDir {
public:
    TempDir() {
        path_ = fs::temp_directory_path() / ("perf_test_" + std::to_string(counter_++));
        fs::create_directories(path_);
    }
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path_, ec);
    }
    std::string path() const { return path_.string(); }
private:
    fs::path path_;
    static std::atomic<int> counter_;
};
std::atomic<int> TempDir::counter_{0};

struct LatencyStats {
    double min_us;
    double max_us;
    double avg_us;
    double p50_us;
    double p95_us;
    double p99_us;
    double stddev_us;
};

LatencyStats calculate_latency_stats(std::vector<double>& latencies) {
    if (latencies.empty()) {
        return {0, 0, 0, 0, 0, 0, 0};
    }
    
    std::sort(latencies.begin(), latencies.end());
    
    double sum = std::accumulate(latencies.begin(), latencies.end(), 0.0);
    double avg = sum / latencies.size();
    
    double sq_sum = 0;
    for (auto l : latencies) {
        sq_sum += (l - avg) * (l - avg);
    }
    double stddev = std::sqrt(sq_sum / latencies.size());
    
    size_t n = latencies.size();
    return {
        latencies.front(),
        latencies.back(),
        avg,
        latencies[n / 2],
        latencies[static_cast<size_t>(n * 0.95)],
        latencies[static_cast<size_t>(n * 0.99)],
        stddev
    };
}

void print_separator(const std::string& title) {
    std::cout << "\n" << std::string(70, '=') << "\n";
    std::cout << " " << title << "\n";
    std::cout << std::string(70, '=') << "\n";
}

void print_metric(const std::string& name, double value, const std::string& unit) {
    std::cout << "  " << std::left << std::setw(35) << name 
              << std::right << std::setw(15) << std::fixed << std::setprecision(2) 
              << value << " " << unit << "\n";
}

void print_latency_stats(const std::string& prefix, const LatencyStats& stats) {
    std::cout << "  " << prefix << " Latency:\n";
    std::cout << "    Min:    " << std::setw(10) << std::fixed << std::setprecision(2) << stats.min_us << " µs\n";
    std::cout << "    Avg:    " << std::setw(10) << stats.avg_us << " µs\n";
    std::cout << "    P50:    " << std::setw(10) << stats.p50_us << " µs\n";
    std::cout << "    P95:    " << std::setw(10) << stats.p95_us << " µs\n";
    std::cout << "    P99:    " << std::setw(10) << stats.p99_us << " µs\n";
    std::cout << "    Max:    " << std::setw(10) << stats.max_us << " µs\n";
    std::cout << "    StdDev: " << std::setw(10) << stats.stddev_us << " µs\n";
}

// =============================================================================
// Benchmark 1: Direct Broker Throughput (No Network)
// =============================================================================

void benchmark_broker_throughput() {
    print_separator("Benchmark 1: Direct Broker Throughput");
    
    TempDir temp;
    auto broker = std::make_shared<Broker>(temp.path());
    broker->create_topic("perf-topic", 4);
    
    const int num_messages = 50000;
    const std::string key = "benchmark-key";
    const std::string value(100, 'x');  // 100-byte payload
    
    // Warmup
    for (int i = 0; i < 1000; i++) {
        broker->produce("perf-topic", key, value);
    }
    
    // Produce benchmark
    std::vector<double> produce_latencies;
    produce_latencies.reserve(num_messages);
    
    auto start = std::chrono::high_resolution_clock::now();
    
    for (int i = 0; i < num_messages; i++) {
        auto t1 = std::chrono::high_resolution_clock::now();
        broker->produce("perf-topic", key, value);
        auto t2 = std::chrono::high_resolution_clock::now();
        produce_latencies.push_back(
            std::chrono::duration<double, std::micro>(t2 - t1).count()
        );
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    double produce_duration_s = std::chrono::duration<double>(end - start).count();
    double produce_throughput = num_messages / produce_duration_s;
    double produce_mb_s = (num_messages * (key.size() + value.size())) / produce_duration_s / 1024 / 1024;
    
    // Consume benchmark
    std::vector<double> consume_latencies;
    consume_latencies.reserve(num_messages);
    
    start = std::chrono::high_resolution_clock::now();
    
    int total_consumed = 0;
    for (uint32_t p = 0; p < 4; p++) {
        uint64_t offset = 0;
        while (true) {
            auto t1 = std::chrono::high_resolution_clock::now();
            auto msgs = broker->consume_batch("perf-topic", p, offset, 100);
            auto t2 = std::chrono::high_resolution_clock::now();
            
            if (msgs.empty()) break;
            
            consume_latencies.push_back(
                std::chrono::duration<double, std::micro>(t2 - t1).count()
            );
            
            total_consumed += msgs.size();
            offset = msgs.back()->offset + 1;
        }
    }
    
    end = std::chrono::high_resolution_clock::now();
    double consume_duration_s = std::chrono::duration<double>(end - start).count();
    double consume_throughput = total_consumed / consume_duration_s;
    double consume_mb_s = (total_consumed * (key.size() + value.size())) / consume_duration_s / 1024 / 1024;
    
    // Print results
    std::cout << "\n  Configuration:\n";
    std::cout << "    Messages:     " << num_messages << "\n";
    std::cout << "    Payload size: " << value.size() << " bytes\n";
    std::cout << "    Partitions:   4\n\n";
    
    std::cout << "  Produce Results:\n";
    print_metric("Throughput", produce_throughput, "msg/s");
    print_metric("Data rate", produce_mb_s, "MB/s");
    print_latency_stats("Produce", calculate_latency_stats(produce_latencies));
    
    std::cout << "\n  Consume Results:\n";
    print_metric("Throughput", consume_throughput, "msg/s");
    print_metric("Data rate", consume_mb_s, "MB/s");
    print_latency_stats("Consume batch", calculate_latency_stats(consume_latencies));
}

// =============================================================================
// Benchmark 2: Concurrent Producer Performance
// =============================================================================

void benchmark_concurrent_producers() {
    print_separator("Benchmark 2: Concurrent Producer Performance");
    
    TempDir temp;
    auto broker = std::make_shared<Broker>(temp.path());
    broker->create_topic("concurrent-topic", 8);
    
    const std::vector<int> thread_counts = {1, 2, 4, 8};
    const int messages_per_thread = 10000;
    const std::string value(100, 'x');
    
    std::cout << "\n  Messages per thread: " << messages_per_thread << "\n";
    std::cout << "  Payload size: " << value.size() << " bytes\n\n";
    
    std::cout << "  " << std::left << std::setw(10) << "Threads" 
              << std::setw(15) << "Throughput" 
              << std::setw(15) << "Speedup"
              << std::setw(15) << "Per-thread" << "\n";
    std::cout << "  " << std::string(55, '-') << "\n";
    
    double baseline_throughput = 0;
    
    for (int num_threads : thread_counts) {
        std::atomic<int> success_count{0};
        
        auto start = std::chrono::high_resolution_clock::now();
        
        std::vector<std::thread> threads;
        for (int t = 0; t < num_threads; t++) {
            threads.emplace_back([&, t]() {
                for (int i = 0; i < messages_per_thread; i++) {
                    auto result = broker->produce("concurrent-topic", 
                        "key-" + std::to_string(t), value);
                    if (result.success) success_count++;
                }
            });
        }
        
        for (auto& th : threads) th.join();
        
        auto end = std::chrono::high_resolution_clock::now();
        double duration_s = std::chrono::duration<double>(end - start).count();
        double throughput = success_count / duration_s;
        
        if (num_threads == 1) baseline_throughput = throughput;
        double speedup = throughput / baseline_throughput;
        double per_thread = throughput / num_threads;
        
        std::cout << "  " << std::left << std::setw(10) << num_threads
                  << std::setw(15) << std::fixed << std::setprecision(0) << throughput
                  << std::setw(15) << std::setprecision(2) << speedup << "x"
                  << std::setw(15) << std::setprecision(0) << per_thread << " msg/s\n";
    }
}

// =============================================================================
// Benchmark 3: Network Round-Trip Performance
// =============================================================================

void benchmark_network_roundtrip() {
    print_separator("Benchmark 3: Network Round-Trip Performance");
    
    TempDir temp;
    auto broker = std::make_shared<Broker>(temp.path());
    BrokerRequestHandler handler(broker);
    
    ServerConfig server_config;
    server_config.port = 0;
    server_config.num_io_threads = 2;
    
    TcpServer server(server_config);
    server.set_request_handler([&](auto conn, auto type, auto payload) {
        return handler.handle(conn, type, payload);
    });
    server.start();
    
    ClientConfig client_config;
    client_config.host = "127.0.0.1";
    client_config.port = server.bound_port();
    
    StreamClient client(client_config);
    client.connect();
    
    // Create topic
    client.create_topic("network-test", 2);
    
    const int num_requests = 5000;
    const std::string value(100, 'x');
    
    // Warmup
    for (int i = 0; i < 50; i++) {
        client.produce("network-test", "key", value);
    }
    
    // Produce latency test
    std::vector<double> produce_latencies;
    produce_latencies.reserve(num_requests);
    
    auto start = std::chrono::high_resolution_clock::now();
    
    for (int i = 0; i < num_requests; i++) {
        auto t1 = std::chrono::high_resolution_clock::now();
        client.produce("network-test", "key", value);
        auto t2 = std::chrono::high_resolution_clock::now();
        produce_latencies.push_back(
            std::chrono::duration<double, std::micro>(t2 - t1).count()
        );
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    double duration_s = std::chrono::duration<double>(end - start).count();
    double throughput = num_requests / duration_s;
    
    // Fetch latency test
    std::vector<double> fetch_latencies;
    fetch_latencies.reserve(100);
    
    for (int i = 0; i < 100; i++) {
        auto t1 = std::chrono::high_resolution_clock::now();
        client.fetch("network-test", 0, 0, 100);
        auto t2 = std::chrono::high_resolution_clock::now();
        fetch_latencies.push_back(
            std::chrono::duration<double, std::micro>(t2 - t1).count()
        );
    }
    
    std::cout << "\n  Configuration:\n";
    std::cout << "    Requests:     " << num_requests << "\n";
    std::cout << "    Payload size: " << value.size() << " bytes\n";
    std::cout << "    IO threads:   " << server_config.num_io_threads << "\n\n";
    
    print_metric("Produce throughput", throughput, "req/s");
    print_latency_stats("Produce RTT", calculate_latency_stats(produce_latencies));
    std::cout << "\n";
    print_latency_stats("Fetch RTT", calculate_latency_stats(fetch_latencies));
    
    server.stop();
}

// =============================================================================
// Benchmark 4: Multi-Client Concurrent Network
// =============================================================================

void benchmark_multi_client_network() {
    print_separator("Benchmark 4: Multi-Client Concurrent Network");
    
    const std::vector<int> client_counts = {1, 2, 4, 8};
    const int requests_per_client = 500;
    const std::string value(100, 'x');
    
    std::cout << "\n  Requests per client: " << requests_per_client << "\n";
    std::cout << "  Payload size: " << value.size() << " bytes\n\n";
    
    std::cout << "  " << std::left << std::setw(10) << "Clients" 
              << std::setw(15) << "Throughput" 
              << std::setw(15) << "Avg Latency"
              << std::setw(15) << "P99 Latency" << "\n";
    std::cout << "  " << std::string(55, '-') << "\n";
    
    for (int num_clients : client_counts) {
        // Create fresh server for each test iteration
        TempDir temp;
        auto broker = std::make_shared<Broker>(temp.path());
        BrokerRequestHandler handler(broker);
        
        ServerConfig server_config;
        server_config.port = 0;
        server_config.num_io_threads = 2;
        
        TcpServer server(server_config);
        server.set_request_handler([&handler](auto conn, auto type, auto payload) {
            return handler.handle(conn, type, payload);
        });
        server.start();
        
        ClientConfig client_config;
        client_config.host = "127.0.0.1";
        client_config.port = server.bound_port();
        
        // Setup topic
        {
            StreamClient setup_client(client_config);
            setup_client.connect();
            setup_client.create_topic("multi-client", 4);
            setup_client.disconnect();
        }
        
        std::atomic<int> success_count{0};
        std::mutex latencies_mutex;
        std::vector<double> combined_latencies;
        combined_latencies.reserve(num_clients * requests_per_client);
        
        auto start = std::chrono::high_resolution_clock::now();
        
        std::vector<std::thread> threads;
        for (int c = 0; c < num_clients; c++) {
            threads.emplace_back([&, c]() {
                std::vector<double> local_latencies;
                local_latencies.reserve(requests_per_client);
                
                StreamClient cli(client_config);
                if (!cli.connect()) return;
                
                for (int i = 0; i < requests_per_client; i++) {
                    auto t1 = std::chrono::high_resolution_clock::now();
                    auto result = cli.produce("multi-client", "key-" + std::to_string(c), value);
                    auto t2 = std::chrono::high_resolution_clock::now();
                    
                    if (result && result->error == ErrorCode::NONE) {
                        success_count++;
                        local_latencies.push_back(
                            std::chrono::duration<double, std::micro>(t2 - t1).count()
                        );
                    }
                }
                
                cli.disconnect();
                
                // Safely merge latencies
                std::lock_guard<std::mutex> lock(latencies_mutex);
                combined_latencies.insert(combined_latencies.end(), 
                                          local_latencies.begin(), local_latencies.end());
            });
        }
        
        for (auto& th : threads) th.join();
        
        auto end = std::chrono::high_resolution_clock::now();
        double duration_s = std::chrono::duration<double>(end - start).count();
        double throughput = success_count / duration_s;
        
        auto stats = calculate_latency_stats(combined_latencies);
        
        std::cout << "  " << std::left << std::setw(10) << num_clients
                  << std::setw(15) << std::fixed << std::setprecision(0) << throughput
                  << std::setw(15) << std::setprecision(1) << stats.avg_us << " µs"
                  << std::setw(15) << stats.p99_us << " µs\n";
        
        server.stop();
        
        // Small delay between tests
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
}

// =============================================================================
// Benchmark 5: Message Size Impact
// =============================================================================

void benchmark_message_sizes() {
    print_separator("Benchmark 5: Message Size Impact on Throughput");
    
    TempDir temp;
    auto broker = std::make_shared<Broker>(temp.path());
    broker->create_topic("size-test", 2);
    
    const std::vector<size_t> sizes = {64, 256, 1024, 4096, 16384};
    const int duration_ms = 1000;  // Run each test for 1 second
    
    std::cout << "\n  " << std::left << std::setw(12) << "Size" 
              << std::setw(15) << "Throughput" 
              << std::setw(15) << "Data Rate"
              << std::setw(15) << "Avg Latency" << "\n";
    std::cout << "  " << std::string(57, '-') << "\n";
    
    for (size_t msg_size : sizes) {
        std::string value(msg_size, 'x');
        std::vector<double> latencies;
        latencies.reserve(100000);
        
        auto start = std::chrono::high_resolution_clock::now();
        auto deadline = start + std::chrono::milliseconds(duration_ms);
        int count = 0;
        
        while (std::chrono::high_resolution_clock::now() < deadline) {
            auto t1 = std::chrono::high_resolution_clock::now();
            broker->produce("size-test", "k", value);
            auto t2 = std::chrono::high_resolution_clock::now();
            latencies.push_back(
                std::chrono::duration<double, std::micro>(t2 - t1).count()
            );
            count++;
        }
        
        auto end = std::chrono::high_resolution_clock::now();
        double duration_s = std::chrono::duration<double>(end - start).count();
        double throughput = count / duration_s;
        double data_rate_mb = (count * msg_size) / duration_s / 1024 / 1024;
        auto stats = calculate_latency_stats(latencies);
        
        std::string size_str;
        if (msg_size >= 1024) {
            size_str = std::to_string(msg_size / 1024) + " KB";
        } else {
            size_str = std::to_string(msg_size) + " B";
        }
        
        std::cout << "  " << std::left << std::setw(12) << size_str
                  << std::setw(15) << std::fixed << std::setprecision(0) << throughput
                  << std::setw(15) << std::setprecision(1) << data_rate_mb << " MB/s"
                  << std::setw(15) << stats.avg_us << " µs\n";
    }
}

// =============================================================================
// Benchmark 6: Consumer Group Performance
// =============================================================================

void benchmark_consumer_groups() {
    print_separator("Benchmark 6: Consumer Group Performance");
    
    TempDir temp;
    auto broker = std::make_shared<Broker>(temp.path());
    broker->create_topic("cg-perf", 8);
    
    // Pre-populate with messages
    const int total_messages = 20000;
    const std::string value(100, 'x');
    
    std::cout << "\n  Populating topic with " << total_messages << " messages...\n";
    
    for (int i = 0; i < total_messages; i++) {
        broker->produce("cg-perf", "key-" + std::to_string(i % 100), value);
    }
    
    const std::vector<int> consumer_counts = {1, 2, 4, 8};
    
    std::cout << "\n  " << std::left << std::setw(12) << "Consumers" 
              << std::setw(18) << "Total Throughput" 
              << std::setw(18) << "Per Consumer"
              << std::setw(15) << "Duration" << "\n";
    std::cout << "  " << std::string(63, '-') << "\n";
    
    for (int num_consumers : consumer_counts) {
        auto group = std::make_unique<ConsumerGroup>(broker, "perf-group-" + std::to_string(num_consumers), "cg-perf");
        
        std::vector<std::string> consumer_ids;
        for (int i = 0; i < num_consumers; i++) {
            consumer_ids.push_back(group->join());
        }
        
        std::atomic<int> total_consumed{0};
        
        auto start = std::chrono::high_resolution_clock::now();
        
        std::vector<std::thread> threads;
        for (int c = 0; c < num_consumers; c++) {
            threads.emplace_back([&, c]() {
                while (total_consumed < total_messages) {
                    auto msgs = group->poll(consumer_ids[c], 100, std::chrono::milliseconds(10));
                    total_consumed += static_cast<int>(msgs.size());
                    if (msgs.empty()) break;
                }
            });
        }
        
        for (auto& th : threads) th.join();
        
        auto end = std::chrono::high_resolution_clock::now();
        double duration_s = std::chrono::duration<double>(end - start).count();
        double throughput = total_consumed / duration_s;
        double per_consumer = throughput / num_consumers;
        
        std::cout << "  " << std::left << std::setw(12) << num_consumers
                  << std::setw(18) << std::fixed << std::setprecision(0) << throughput
                  << std::setw(18) << per_consumer
                  << std::setw(15) << std::setprecision(2) << duration_s << " s\n";
    }
}

// =============================================================================
// Benchmark 7: Storage Performance
// =============================================================================

void benchmark_storage() {
    print_separator("Benchmark 7: Storage Layer Performance");
    
    TempDir temp;
    auto broker = std::make_shared<Broker>(temp.path());
    broker->create_topic("storage-test", 1);
    
    const std::string value(1024, 'x');  // 1KB messages
    const int num_messages = 20000;
    
    // Write test
    auto start = std::chrono::high_resolution_clock::now();
    
    for (int i = 0; i < num_messages; i++) {
        broker->produce("storage-test", "key-" + std::to_string(i), value);
    }
    broker->flush();
    
    auto end = std::chrono::high_resolution_clock::now();
    double write_duration_s = std::chrono::duration<double>(end - start).count();
    double write_throughput = num_messages / write_duration_s;
    double write_mb_s = (num_messages * value.size()) / write_duration_s / 1024 / 1024;
    
    // Sequential read test
    start = std::chrono::high_resolution_clock::now();
    
    int read_count = 0;
    uint64_t offset = 0;
    while (true) {
        auto msgs = broker->consume_batch("storage-test", 0, offset, 100);
        if (msgs.empty()) break;
        read_count += msgs.size();
        offset = msgs.back()->offset + 1;
    }
    
    end = std::chrono::high_resolution_clock::now();
    double read_duration_s = std::chrono::duration<double>(end - start).count();
    double read_throughput = read_count / read_duration_s;
    double read_mb_s = (read_count * value.size()) / read_duration_s / 1024 / 1024;
    
    // Random read test
    std::vector<double> random_latencies;
    random_latencies.reserve(1000);
    
    for (int i = 0; i < 1000; i++) {
        uint64_t random_offset = static_cast<uint64_t>(rand()) % num_messages;
        auto t1 = std::chrono::high_resolution_clock::now();
        broker->consume("storage-test", 0, random_offset);
        auto t2 = std::chrono::high_resolution_clock::now();
        random_latencies.push_back(
            std::chrono::duration<double, std::micro>(t2 - t1).count()
        );
    }
    
    std::cout << "\n  Configuration:\n";
    std::cout << "    Messages:     " << num_messages << "\n";
    std::cout << "    Message size: " << value.size() << " bytes\n\n";
    
    std::cout << "  Write Performance:\n";
    print_metric("Throughput", write_throughput, "msg/s");
    print_metric("Data rate", write_mb_s, "MB/s");
    
    std::cout << "\n  Sequential Read Performance:\n";
    print_metric("Throughput", read_throughput, "msg/s");
    print_metric("Data rate", read_mb_s, "MB/s");
    
    std::cout << "\n  Random Read Performance:\n";
    print_latency_stats("Random read", calculate_latency_stats(random_latencies));
}

// =============================================================================
// Main
// =============================================================================

int main() {
    std::cout << "\n";
    std::cout << "╔══════════════════════════════════════════════════════════════════════╗\n";
    std::cout << "║           STREAM PROCESSOR PERFORMANCE BENCHMARK SUITE               ║\n";
    std::cout << "╚══════════════════════════════════════════════════════════════════════╝\n";
    
    auto total_start = std::chrono::high_resolution_clock::now();
    
    benchmark_broker_throughput();
    benchmark_concurrent_producers();
    benchmark_network_roundtrip();
    benchmark_multi_client_network();
    benchmark_message_sizes();
    benchmark_consumer_groups();
    benchmark_storage();
    
    auto total_end = std::chrono::high_resolution_clock::now();
    double total_duration = std::chrono::duration<double>(total_end - total_start).count();
    
    print_separator("BENCHMARK COMPLETE");
    std::cout << "\n  Total benchmark duration: " << std::fixed << std::setprecision(1) 
              << total_duration << " seconds\n\n";
    
    return 0;
}
