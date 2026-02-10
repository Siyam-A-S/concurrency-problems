/**
 * Simple Test Client for Stream Processor
 * 
 * Tests basic produce/fetch operations to verify connectivity
 */

#include "network/client.h"
#include <iostream>
#include <thread>
#include <chrono>

using namespace stream;
using namespace stream::network;

int main(int argc, char* argv[]) {
    std::string host = "localhost";
    uint16_t port = 9092;
    std::string topic = "test-topic";
    
    // Parse args
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg.find("--host=") == 0) host = arg.substr(7);
        else if (arg.find("--port=") == 0) port = static_cast<uint16_t>(std::stoi(arg.substr(7)));
        else if (arg.find("--topic=") == 0) topic = arg.substr(8);
    }
    
    std::cout << "=== Stream Processor Test Client ===" << std::endl;
    std::cout << "Host: " << host << ":" << port << std::endl;
    std::cout << "Topic: " << topic << std::endl << std::endl;
    
    // Connect
    ClientConfig config;
    config.host = host;
    config.port = port;
    
    StreamClient client(config);
    
    if (!client.connect()) {
        std::cerr << "Failed to connect!" << std::endl;
        return 1;
    }
    std::cout << "✓ Connected to server" << std::endl;
    
    // Create topic
    if (client.create_topic(topic, 1)) {
        std::cout << "✓ Created/verified topic: " << topic << std::endl;
    } else {
        std::cerr << "Failed to create topic" << std::endl;
    }
    
    // List topics
    auto topics = client.list_topics();
    std::cout << "✓ Topics on server: ";
    for (const auto& t : topics) {
        std::cout << t << " ";
    }
    std::cout << std::endl;
    
    // Produce some messages
    std::cout << "\n--- Producing 10 messages ---" << std::endl;
    for (int i = 0; i < 10; i++) {
        std::string key = "key-" + std::to_string(i);
        std::string value = R"({"price":"97000.00","best_bid":"96999.00","best_ask":"97001.00","last_size":"0.5"})";
        
        auto resp = client.produce(topic, key, value);
        if (resp) {
            std::cout << "  Produced message " << i << " -> partition " 
                      << resp->partition << " offset " << resp->offset << std::endl;
        } else {
            std::cerr << "  Failed to produce message " << i << std::endl;
        }
    }
    
    // Small delay
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Fetch messages
    std::cout << "\n--- Fetching messages ---" << std::endl;
    auto fetch_resp = client.fetch(topic, 0, 0, 100);
    if (fetch_resp) {
        std::cout << "✓ Fetched " << fetch_resp->messages.size() << " messages" << std::endl;
        std::cout << "  High watermark: " << fetch_resp->high_watermark << std::endl;
        
        for (size_t i = 0; i < std::min(size_t(5), fetch_resp->messages.size()); i++) {
            const auto& msg = fetch_resp->messages[i];
            std::cout << "  [" << msg->offset << "] key=" << msg->key 
                      << " value=" << msg->value.substr(0, 50) << "..." << std::endl;
        }
    } else {
        std::cerr << "Failed to fetch messages" << std::endl;
    }
    
    // Test consumer group
    std::cout << "\n--- Testing Consumer Group ---" << std::endl;
    auto join_resp = client.join_group("test-group", topic);
    if (join_resp) {
        std::cout << "✓ Joined group as: " << join_resp->consumer_id << std::endl;
        
        // Leave group
        client.leave_group("test-group", join_resp->consumer_id);
        std::cout << "✓ Left group" << std::endl;
    } else {
        std::cerr << "Failed to join group" << std::endl;
    }
    
    client.disconnect();
    std::cout << "\n✓ Test completed successfully!" << std::endl;
    
    return 0;
}
