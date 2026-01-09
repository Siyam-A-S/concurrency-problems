#include "network/protocol.h"
#include "network/tcp_server.h"
#include "network/client.h"
#include "broker.h"
#include <iostream>
#include <thread>
#include <atomic>
#include <cassert>
#include <filesystem>

using namespace stream;
using namespace stream::network;

namespace fs = std::filesystem;

// =============================================================================
// Test Utilities
// =============================================================================

#define TEST(name) void test_##name()
#define RUN_TEST(name) do { \
    std::cout << "Running " #name "... "; \
    std::cout.flush(); \
    test_##name(); \
    std::cout << "PASSED" << std::endl; \
} while(0)

class TempDir {
public:
    TempDir() {
        path_ = fs::temp_directory_path() / ("network_test_" + std::to_string(counter_++));
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

// =============================================================================
// Protocol Header Tests
// =============================================================================

TEST(protocol_header_serialize) {
    ProtocolHeader header;
    header.magic = MAGIC_NUMBER;
    header.version = PROTOCOL_VERSION;
    header.type = static_cast<uint32_t>(RequestType::PRODUCE);
    header.length = 100;
    
    uint8_t buffer[HEADER_SIZE];
    header.serialize(buffer);
    
    auto decoded = ProtocolHeader::deserialize(buffer);
    assert(decoded.magic == MAGIC_NUMBER);
    assert(decoded.version == PROTOCOL_VERSION);
    assert(decoded.type == static_cast<uint32_t>(RequestType::PRODUCE));
    assert(decoded.length == 100);
    assert(decoded.is_valid());
}

TEST(protocol_header_invalid) {
    ProtocolHeader header;
    header.magic = 0xDEADBEEF;  // Wrong magic
    header.version = PROTOCOL_VERSION;
    header.type = 0;
    header.length = 0;
    
    assert(!header.is_valid());
    
    header.magic = MAGIC_NUMBER;
    header.version = 999;  // Wrong version
    assert(!header.is_valid());
}

// =============================================================================
// Protocol Codec Tests
// =============================================================================

TEST(codec_integers) {
    std::vector<uint8_t> buffer;
    
    ProtocolCodec::write_u16(buffer, 0x1234);
    ProtocolCodec::write_u32(buffer, 0x12345678);
    ProtocolCodec::write_u64(buffer, 0x123456789ABCDEF0ULL);
    ProtocolCodec::write_i16(buffer, -1234);
    
    const uint8_t* ptr = buffer.data();
    size_t remaining = buffer.size();
    
    auto u16 = ProtocolCodec::read_u16(ptr, remaining);
    assert(u16 && *u16 == 0x1234);
    
    auto u32 = ProtocolCodec::read_u32(ptr, remaining);
    assert(u32 && *u32 == 0x12345678);
    
    auto u64 = ProtocolCodec::read_u64(ptr, remaining);
    assert(u64 && *u64 == 0x123456789ABCDEF0ULL);
    
    auto i16 = ProtocolCodec::read_i16(ptr, remaining);
    assert(i16 && *i16 == -1234);
    
    assert(remaining == 0);
}

TEST(codec_strings) {
    std::vector<uint8_t> buffer;
    
    ProtocolCodec::write_string(buffer, "hello");
    ProtocolCodec::write_string(buffer, "");
    ProtocolCodec::write_string(buffer, "world!");
    
    const uint8_t* ptr = buffer.data();
    size_t remaining = buffer.size();
    
    auto s1 = ProtocolCodec::read_string(ptr, remaining);
    assert(s1 && *s1 == "hello");
    
    auto s2 = ProtocolCodec::read_string(ptr, remaining);
    assert(s2 && *s2 == "");
    
    auto s3 = ProtocolCodec::read_string(ptr, remaining);
    assert(s3 && *s3 == "world!");
}

TEST(codec_encode_decode) {
    std::vector<uint8_t> payload = {1, 2, 3, 4, 5};
    auto encoded = ProtocolCodec::encode(RequestType::PRODUCE, payload);
    
    assert(encoded.size() == HEADER_SIZE + 5);
    
    auto header = ProtocolCodec::decode_header(encoded.data(), encoded.size());
    assert(header.has_value());
    assert(header->type == static_cast<uint32_t>(RequestType::PRODUCE));
    assert(header->length == 5);
}

// =============================================================================
// Request Serialization Tests
// =============================================================================

TEST(create_topic_request) {
    CreateTopicRequest req{"my-topic", 4};
    auto serialized = req.serialize();
    
    auto decoded = CreateTopicRequest::deserialize(serialized.data(), serialized.size());
    assert(decoded.has_value());
    assert(decoded->topic_name == "my-topic");
    assert(decoded->num_partitions == 4);
}

TEST(produce_request) {
    ProduceRequest req{"events", "user-123", "click-data", MessagePriority::HIGH};
    auto serialized = req.serialize();
    
    auto decoded = ProduceRequest::deserialize(serialized.data(), serialized.size());
    assert(decoded.has_value());
    assert(decoded->topic_name == "events");
    assert(decoded->key == "user-123");
    assert(decoded->value == "click-data");
    assert(decoded->priority == MessagePriority::HIGH);
}

TEST(fetch_request) {
    FetchRequest req{"logs", 2, 1000, 50};
    auto serialized = req.serialize();
    
    auto decoded = FetchRequest::deserialize(serialized.data(), serialized.size());
    assert(decoded.has_value());
    assert(decoded->topic_name == "logs");
    assert(decoded->partition == 2);
    assert(decoded->offset == 1000);
    assert(decoded->max_messages == 50);
}

TEST(join_group_request) {
    JoinGroupRequest req{"my-group", "events"};
    auto serialized = req.serialize();
    
    auto decoded = JoinGroupRequest::deserialize(serialized.data(), serialized.size());
    assert(decoded.has_value());
    assert(decoded->group_id == "my-group");
    assert(decoded->topic_name == "events");
}

TEST(commit_offset_request) {
    CommitOffsetRequest req{"group1", "topic1", 3, 12345};
    auto serialized = req.serialize();
    
    auto decoded = CommitOffsetRequest::deserialize(serialized.data(), serialized.size());
    assert(decoded.has_value());
    assert(decoded->group_id == "group1");
    assert(decoded->topic_name == "topic1");
    assert(decoded->partition == 3);
    assert(decoded->offset == 12345);
}

// =============================================================================
// Response Serialization Tests
// =============================================================================

TEST(generic_response) {
    GenericResponse resp{ErrorCode::TOPIC_NOT_FOUND, "Topic does not exist"};
    auto serialized = resp.serialize();
    
    auto decoded = GenericResponse::deserialize(serialized.data(), serialized.size());
    assert(decoded.has_value());
    assert(decoded->error == ErrorCode::TOPIC_NOT_FOUND);
    assert(decoded->error_message == "Topic does not exist");
}

TEST(produce_response) {
    ProduceResponse resp{ErrorCode::NONE, "", 2, 12345};
    auto serialized = resp.serialize();
    
    auto decoded = ProduceResponse::deserialize(serialized.data(), serialized.size());
    assert(decoded.has_value());
    assert(decoded->error == ErrorCode::NONE);
    assert(decoded->partition == 2);
    assert(decoded->offset == 12345);
}

TEST(list_topics_response) {
    ListTopicsResponse resp;
    resp.error = ErrorCode::NONE;
    resp.topics = {"topic1", "topic2", "topic3"};
    auto serialized = resp.serialize();
    
    auto decoded = ListTopicsResponse::deserialize(serialized.data(), serialized.size());
    assert(decoded.has_value());
    assert(decoded->error == ErrorCode::NONE);
    assert(decoded->topics.size() == 3);
    assert(decoded->topics[0] == "topic1");
    assert(decoded->topics[2] == "topic3");
}

TEST(join_group_response) {
    JoinGroupResponse resp{ErrorCode::NONE, "", "consumer-1", {0, 1, 2, 3}};
    auto serialized = resp.serialize();
    
    auto decoded = JoinGroupResponse::deserialize(serialized.data(), serialized.size());
    assert(decoded.has_value());
    assert(decoded->error == ErrorCode::NONE);
    assert(decoded->consumer_id == "consumer-1");
    assert(decoded->assigned_partitions.size() == 4);
    assert(decoded->assigned_partitions[0] == 0);
    assert(decoded->assigned_partitions[3] == 3);
}

TEST(metadata_response) {
    MetadataResponse resp;
    resp.error = ErrorCode::NONE;
    resp.topics = {{"events", 4}, {"logs", 2}};
    auto serialized = resp.serialize();
    
    auto decoded = MetadataResponse::deserialize(serialized.data(), serialized.size());
    assert(decoded.has_value());
    assert(decoded->error == ErrorCode::NONE);
    assert(decoded->topics.size() == 2);
    assert(decoded->topics[0].name == "events");
    assert(decoded->topics[0].partition_count == 4);
}

// =============================================================================
// TCP Server Tests
// =============================================================================

TEST(server_start_stop) {
    ServerConfig config;
    config.port = 0;  // Let OS assign port
    config.num_io_threads = 1;
    
    TcpServer server(config);
    
    bool handler_called = false;
    server.set_request_handler([&](auto, auto, auto) {
        handler_called = true;
        return GenericResponse{ErrorCode::NONE, ""}.serialize();
    });
    
    assert(server.start());
    assert(server.is_running());
    assert(server.bound_port() > 0);
    
    server.stop();
    assert(!server.is_running());
}

TEST(client_connect_disconnect) {
    TempDir temp;
    
    auto broker = std::make_shared<Broker>(temp.path());
    BrokerRequestHandler handler(broker);
    
    ServerConfig config;
    config.port = 0;
    config.num_io_threads = 1;
    
    TcpServer server(config);
    server.set_request_handler([&](auto conn, auto type, auto payload) {
        return handler.handle(conn, type, payload);
    });
    
    assert(server.start());
    
    ClientConfig client_config;
    client_config.host = "127.0.0.1";
    client_config.port = server.bound_port();
    
    TcpClient client(client_config);
    assert(client.connect());
    assert(client.is_connected());
    
    client.disconnect();
    assert(!client.is_connected());
    
    server.stop();
}

// =============================================================================
// End-to-End Tests
// =============================================================================

TEST(e2e_create_topic) {
    TempDir temp;
    
    auto broker = std::make_shared<Broker>(temp.path());
    BrokerRequestHandler handler(broker);
    
    ServerConfig server_config;
    server_config.port = 0;
    server_config.num_io_threads = 1;
    
    TcpServer server(server_config);
    server.set_request_handler([&](auto conn, auto type, auto payload) {
        return handler.handle(conn, type, payload);
    });
    assert(server.start());
    
    ClientConfig client_config;
    client_config.host = "127.0.0.1";
    client_config.port = server.bound_port();
    
    StreamClient client(client_config);
    assert(client.connect());
    
    // Create topic
    assert(client.create_topic("test-topic", 4));
    
    // Verify via metadata
    auto metadata = client.get_metadata();
    assert(metadata.has_value());
    assert(metadata->topics.size() == 1);
    assert(metadata->topics[0].name == "test-topic");
    assert(metadata->topics[0].partition_count == 4);
    
    server.stop();
}

TEST(e2e_produce_fetch) {
    TempDir temp;
    
    auto broker = std::make_shared<Broker>(temp.path());
    BrokerRequestHandler handler(broker);
    
    ServerConfig server_config;
    server_config.port = 0;
    server_config.num_io_threads = 1;
    
    TcpServer server(server_config);
    server.set_request_handler([&](auto conn, auto type, auto payload) {
        return handler.handle(conn, type, payload);
    });
    assert(server.start());
    
    ClientConfig client_config;
    client_config.host = "127.0.0.1";
    client_config.port = server.bound_port();
    
    StreamClient client(client_config);
    assert(client.connect());
    
    // Create topic
    assert(client.create_topic("events", 2));
    
    // Produce messages
    auto result1 = client.produce("events", "key1", "value1");
    assert(result1.has_value());
    assert(result1->error == ErrorCode::NONE);
    
    auto result2 = client.produce("events", "key2", "value2");
    assert(result2.has_value());
    assert(result2->error == ErrorCode::NONE);
    
    // Fetch from partition 0
    auto fetch0 = client.fetch("events", 0, 0, 10);
    assert(fetch0.has_value());
    assert(fetch0->error == ErrorCode::NONE);
    
    // Fetch from partition 1
    auto fetch1 = client.fetch("events", 1, 0, 10);
    assert(fetch1.has_value());
    assert(fetch1->error == ErrorCode::NONE);
    
    // Total messages should be 2
    size_t total = fetch0->messages.size() + fetch1->messages.size();
    assert(total == 2);
    
    server.stop();
}

TEST(e2e_list_topics) {
    TempDir temp;
    
    auto broker = std::make_shared<Broker>(temp.path());
    BrokerRequestHandler handler(broker);
    
    ServerConfig server_config;
    server_config.port = 0;
    server_config.num_io_threads = 1;
    
    TcpServer server(server_config);
    server.set_request_handler([&](auto conn, auto type, auto payload) {
        return handler.handle(conn, type, payload);
    });
    assert(server.start());
    
    ClientConfig client_config;
    client_config.host = "127.0.0.1";
    client_config.port = server.bound_port();
    
    StreamClient client(client_config);
    assert(client.connect());
    
    // Initially empty
    auto topics = client.list_topics();
    assert(topics.empty());
    
    // Create topics
    assert(client.create_topic("alpha", 1));
    assert(client.create_topic("beta", 2));
    assert(client.create_topic("gamma", 3));
    
    // List should return all
    topics = client.list_topics();
    assert(topics.size() == 3);
    
    // Delete one
    assert(client.delete_topic("beta"));
    
    topics = client.list_topics();
    assert(topics.size() == 2);
    
    server.stop();
}

TEST(e2e_consumer_groups) {
    TempDir temp;
    
    auto broker = std::make_shared<Broker>(temp.path());
    BrokerRequestHandler handler(broker);
    
    ServerConfig server_config;
    server_config.port = 0;
    server_config.num_io_threads = 1;
    
    TcpServer server(server_config);
    server.set_request_handler([&](auto conn, auto type, auto payload) {
        return handler.handle(conn, type, payload);
    });
    assert(server.start());
    
    ClientConfig client_config;
    client_config.host = "127.0.0.1";
    client_config.port = server.bound_port();
    
    StreamClient client(client_config);
    assert(client.connect());
    
    // Create topic
    assert(client.create_topic("cg-topic", 4));
    
    // Join group
    auto join_result = client.join_group("my-group", "cg-topic");
    assert(join_result.has_value());
    assert(join_result->error == ErrorCode::NONE);
    assert(!join_result->consumer_id.empty());
    assert(join_result->assigned_partitions.size() == 4);  // Single consumer gets all
    
    // Commit offset
    assert(client.commit_offset("my-group", "cg-topic", 0, 100));
    
    // Fetch committed offset
    auto offset = client.fetch_offset("my-group", "cg-topic", 0);
    assert(offset.has_value());
    assert(*offset == 100);
    
    // Leave group
    assert(client.leave_group("my-group", join_result->consumer_id));
    
    server.stop();
}

TEST(e2e_offset_persistence) {
    TempDir temp;
    
    auto broker = std::make_shared<Broker>(temp.path());
    BrokerRequestHandler handler(broker);
    
    ServerConfig server_config;
    server_config.port = 0;
    server_config.num_io_threads = 1;
    
    TcpServer server(server_config);
    server.set_request_handler([&](auto conn, auto type, auto payload) {
        return handler.handle(conn, type, payload);
    });
    assert(server.start());
    
    ClientConfig client_config;
    client_config.host = "127.0.0.1";
    client_config.port = server.bound_port();
    
    StreamClient client(client_config);
    assert(client.connect());
    
    assert(client.create_topic("persist-test", 1));
    
    // Produce and commit
    client.produce("persist-test", "k", "v");
    assert(client.commit_offset("persist-group", "persist-test", 0, 1));
    
    // Flush broker
    broker->flush();
    
    // Verify offset is committed
    auto offset = client.fetch_offset("persist-group", "persist-test", 0);
    assert(offset.has_value());
    assert(*offset == 1);
    
    server.stop();
}

TEST(e2e_multiple_clients) {
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
    assert(server.start());
    
    ClientConfig client_config;
    client_config.host = "127.0.0.1";
    client_config.port = server.bound_port();
    
    // Create topic with one client
    StreamClient setup_client(client_config);
    assert(setup_client.connect());
    assert(setup_client.create_topic("multi-client", 2));
    setup_client.disconnect();
    
    // Multiple concurrent producers
    const int num_clients = 4;
    const int msgs_per_client = 50;
    std::atomic<int> success_count{0};
    
    std::vector<std::thread> threads;
    for (int i = 0; i < num_clients; i++) {
        threads.emplace_back([&, i]() {
            StreamClient producer(client_config);
            if (!producer.connect()) return;
            
            for (int j = 0; j < msgs_per_client; j++) {
                auto result = producer.produce("multi-client", 
                    "client-" + std::to_string(i),
                    "msg-" + std::to_string(j));
                if (result && result->error == ErrorCode::NONE) {
                    success_count++;
                }
            }
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    assert(success_count == num_clients * msgs_per_client);
    
    // Verify all messages via consumer
    StreamClient consumer(client_config);
    assert(consumer.connect());
    
    size_t total_fetched = 0;
    for (uint32_t p = 0; p < 2; p++) {
        auto fetch_result = consumer.fetch("multi-client", p, 0, 1000);
        if (fetch_result) {
            total_fetched += fetch_result->messages.size();
        }
    }
    
    assert(total_fetched == static_cast<size_t>(num_clients * msgs_per_client));
    
    server.stop();
}

TEST(e2e_error_handling) {
    TempDir temp;
    
    auto broker = std::make_shared<Broker>(temp.path());
    BrokerRequestHandler handler(broker);
    
    ServerConfig server_config;
    server_config.port = 0;
    server_config.num_io_threads = 1;
    
    TcpServer server(server_config);
    server.set_request_handler([&](auto conn, auto type, auto payload) {
        return handler.handle(conn, type, payload);
    });
    assert(server.start());
    
    ClientConfig client_config;
    client_config.host = "127.0.0.1";
    client_config.port = server.bound_port();
    
    StreamClient client(client_config);
    assert(client.connect());
    
    // Produce to non-existent topic
    auto result = client.produce("nonexistent", "k", "v");
    assert(result.has_value());
    assert(result->error != ErrorCode::NONE);
    
    // Delete non-existent topic
    assert(!client.delete_topic("nonexistent"));
    
    // Create duplicate topic
    assert(client.create_topic("dup-test", 1));
    assert(!client.create_topic("dup-test", 1));  // Should fail
    
    server.stop();
}

// =============================================================================
// Connection Tests
// =============================================================================

TEST(connection_stats) {
    TempDir temp;
    
    auto broker = std::make_shared<Broker>(temp.path());
    BrokerRequestHandler handler(broker);
    
    ServerConfig server_config;
    server_config.port = 0;
    server_config.num_io_threads = 1;
    
    TcpServer server(server_config);
    server.set_request_handler([&](auto conn, auto type, auto payload) {
        return handler.handle(conn, type, payload);
    });
    assert(server.start());
    
    ClientConfig client_config;
    client_config.host = "127.0.0.1";
    client_config.port = server.bound_port();
    
    StreamClient client(client_config);
    assert(client.connect());
    
    assert(client.create_topic("stats-test", 1));
    client.produce("stats-test", "k", "v");
    
    // Check server stats (use const ref since atomics can't be copied)
    const auto& stats = server.stats();
    assert(stats.total_connections >= 1);
    assert(stats.total_requests > 0);
    
    server.stop();
}

// =============================================================================
// Error Code Tests
// =============================================================================

TEST(error_code_strings) {
    assert(std::string(error_code_to_string(ErrorCode::NONE)) == "Success");
    assert(std::string(error_code_to_string(ErrorCode::TOPIC_NOT_FOUND)) == "Topic not found");
    assert(std::string(error_code_to_string(ErrorCode::TIMEOUT)) == "Request timeout");
}

// =============================================================================
// Main
// =============================================================================

int main() {
    std::cout << "\n=== Network Tests ===\n" << std::endl;
    
    std::cout << "--- Protocol Header Tests ---" << std::endl;
    RUN_TEST(protocol_header_serialize);
    RUN_TEST(protocol_header_invalid);
    
    std::cout << "\n--- Protocol Codec Tests ---" << std::endl;
    RUN_TEST(codec_integers);
    RUN_TEST(codec_strings);
    RUN_TEST(codec_encode_decode);
    
    std::cout << "\n--- Request Serialization Tests ---" << std::endl;
    RUN_TEST(create_topic_request);
    RUN_TEST(produce_request);
    RUN_TEST(fetch_request);
    RUN_TEST(join_group_request);
    RUN_TEST(commit_offset_request);
    
    std::cout << "\n--- Response Serialization Tests ---" << std::endl;
    RUN_TEST(generic_response);
    RUN_TEST(produce_response);
    RUN_TEST(list_topics_response);
    RUN_TEST(join_group_response);
    RUN_TEST(metadata_response);
    
    std::cout << "\n--- TCP Server Tests ---" << std::endl;
    RUN_TEST(server_start_stop);
    RUN_TEST(client_connect_disconnect);
    
    std::cout << "\n--- End-to-End Tests ---" << std::endl;
    RUN_TEST(e2e_create_topic);
    RUN_TEST(e2e_produce_fetch);
    RUN_TEST(e2e_list_topics);
    RUN_TEST(e2e_consumer_groups);
    RUN_TEST(e2e_offset_persistence);
    RUN_TEST(e2e_multiple_clients);
    RUN_TEST(e2e_error_handling);
    
    std::cout << "\n--- Misc Tests ---" << std::endl;
    RUN_TEST(connection_stats);
    RUN_TEST(error_code_strings);
    
    std::cout << "\n=== Results ===" << std::endl;
    std::cout << "All tests PASSED!" << std::endl;
    
    return 0;
}
