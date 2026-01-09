#include "broker.h"
#include <fstream>
#include <algorithm>

namespace fs = std::filesystem;

namespace stream {

// ==================== Broker Implementation ====================

Broker::Broker(const std::filesystem::path& data_dir)
    : data_dir_(data_dir)
    , topic_manager_(TopicManager::Config{(data_dir / "topics").string()})
    , running_(true) {
    
    fs::create_directories(data_dir_);
    fs::create_directories(data_dir_ / "offsets");
    
    // Load existing topics
    topic_manager_.load_topics();
    
    // Load committed offsets
    load_offsets();
}

Broker::~Broker() {
    shutdown();
}

void Broker::shutdown() {
    if (running_.exchange(false)) {
        flush();
        save_offsets();
    }
}

bool Broker::create_topic(const std::string& name, uint32_t num_partitions) {
    return topic_manager_.create_topic(name, num_partitions) != nullptr;
}

bool Broker::delete_topic(const std::string& name) {
    return topic_manager_.delete_topic(name);
}

bool Broker::topic_exists(const std::string& name) const {
    return topic_manager_.topic_exists(name);
}

std::vector<std::string> Broker::list_topics() const {
    return topic_manager_.list_topics();
}

ProduceResult Broker::produce(const std::string& topic_name,
                              const std::string& key,
                              const std::string& value,
                              MessagePriority priority) {
    auto* topic = topic_manager_.get_topic(topic_name);
    if (!topic) {
        return ProduceResult{false, topic_name, 0, 0, 
            std::chrono::system_clock::now(), "Topic not found"};
    }
    
    // Create message
    Message msg;
    msg.key = key;
    msg.value = value;
    msg.priority = priority;
    msg.timestamp_ms = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
    
    auto result = topic->produce(msg);
    
    return ProduceResult{
        result.success,
        topic_name,
        result.partition_id,
        result.offset,
        std::chrono::system_clock::now(),
        result.error
    };
}

std::vector<ProduceResult> Broker::produce_batch(
    const std::string& topic_name,
    const std::vector<std::tuple<std::string, std::string, MessagePriority>>& messages) {
    
    auto* topic = topic_manager_.get_topic(topic_name);
    if (!topic) {
        std::vector<ProduceResult> results;
        auto now = std::chrono::system_clock::now();
        for (size_t i = 0; i < messages.size(); i++) {
            results.push_back({false, topic_name, 0, 0, now, "Topic not found"});
        }
        return results;
    }
    
    std::vector<ProduceResult> results;
    results.reserve(messages.size());
    
    for (const auto& [key, value, priority] : messages) {
        Message msg;
        msg.key = key;
        msg.value = value;
        msg.priority = priority;
        msg.timestamp_ms = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
        
        auto result = topic->produce(msg);
        results.push_back({
            result.success,
            topic_name,
            result.partition_id,
            result.offset,
            std::chrono::system_clock::now(),
            result.error
        });
    }
    
    return results;
}

MessagePtr Broker::consume(const std::string& topic_name,
                           uint32_t partition,
                           uint64_t offset) {
    auto* topic = topic_manager_.get_topic(topic_name);
    if (!topic) return nullptr;
    
    auto result = topic->fetch(partition, offset, 1);
    if (!result.success || result.messages.empty()) {
        return nullptr;
    }
    
    return result.messages[0];
}

std::vector<MessagePtr> Broker::consume_batch(const std::string& topic_name,
                                              uint32_t partition,
                                              uint64_t start_offset,
                                              size_t max_messages) {
    auto* topic = topic_manager_.get_topic(topic_name);
    if (!topic) return {};
    
    auto result = topic->fetch(partition, start_offset, max_messages);
    return result.messages;
}

uint64_t Broker::get_latest_offset(const std::string& topic_name,
                                   uint32_t partition) const {
    auto* topic = topic_manager_.get_topic(topic_name);
    if (!topic) return 0;
    
    auto offsets = topic->end_offsets();
    auto it = offsets.find(partition);
    if (it == offsets.end()) return 0;
    
    return it->second;
}

uint32_t Broker::get_partition_count(const std::string& topic_name) const {
    auto* topic = topic_manager_.get_topic(topic_name);
    if (!topic) return 0;
    return topic->num_partitions();
}

void Broker::commit_offset(const std::string& group_id,
                           const std::string& topic,
                           uint32_t partition,
                           uint64_t offset) {
    std::lock_guard<std::mutex> lock(offset_mutex_);
    committed_offsets_[group_id][topic][partition] = offset;
}

uint64_t Broker::get_committed_offset(const std::string& group_id,
                                      const std::string& topic,
                                      uint32_t partition) const {
    std::lock_guard<std::mutex> lock(offset_mutex_);
    
    auto group_it = committed_offsets_.find(group_id);
    if (group_it == committed_offsets_.end()) return 0;
    
    auto topic_it = group_it->second.find(topic);
    if (topic_it == group_it->second.end()) return 0;
    
    auto part_it = topic_it->second.find(partition);
    if (part_it == topic_it->second.end()) return 0;
    
    return part_it->second;
}

BrokerStats Broker::get_stats() const {
    BrokerStats broker_stats{};
    
    auto topics = topic_manager_.list_topics();
    broker_stats.topic_count = topics.size();
    
    for (const auto& topic_name : topics) {
        auto* topic = topic_manager_.get_topic(topic_name);
        if (!topic) continue;
        
        broker_stats.partition_count += topic->num_partitions();
        
        auto topic_stats = topic->stats();
        broker_stats.total_messages += topic_stats.total_messages;
        broker_stats.total_bytes += topic_stats.total_bytes;
    }
    
    return broker_stats;
}

void Broker::flush() {
    topic_manager_.flush_all();
    save_offsets();
}

void Broker::load_offsets() {
    auto offset_file = data_dir_ / "offsets" / "committed.dat";
    if (!fs::exists(offset_file)) return;
    
    std::ifstream file(offset_file);
    if (!file) return;
    
    std::string group, topic;
    uint32_t partition;
    uint64_t offset;
    
    while (file >> group >> topic >> partition >> offset) {
        committed_offsets_[group][topic][partition] = offset;
    }
}

void Broker::save_offsets() {
    auto offset_dir = data_dir_ / "offsets";
    fs::create_directories(offset_dir);
    
    std::ofstream file(offset_dir / "committed.dat");
    if (!file) return;
    
    std::lock_guard<std::mutex> lock(offset_mutex_);
    for (const auto& [group, topics] : committed_offsets_) {
        for (const auto& [topic, partitions] : topics) {
            for (const auto& [partition, offset] : partitions) {
                file << group << " " << topic << " " << partition << " " << offset << "\n";
            }
        }
    }
}

// ==================== Producer Implementation ====================

Producer::Producer(std::shared_ptr<Broker> broker, const std::string& topic)
    : broker_(std::move(broker))
    , topic_(topic)
    , batch_size_(100)
    , linger_ms_(5)
    , running_(true) {
    
    // Start background flush thread
    flush_thread_ = std::thread(&Producer::flush_loop, this);
}

Producer::~Producer() {
    running_ = false;
    buffer_cv_.notify_all();
    if (flush_thread_.joinable()) {
        flush_thread_.join();
    }
    flush();
}

ProduceResult Producer::send(const std::string& key, const std::string& value,
                             MessagePriority priority) {
    return broker_->produce(topic_, key, value, priority);
}

std::vector<ProduceResult> Producer::send_batch(
    const std::vector<std::pair<std::string, std::string>>& messages,
    MessagePriority priority) {
    
    std::vector<std::tuple<std::string, std::string, MessagePriority>> batch;
    batch.reserve(messages.size());
    
    for (const auto& [key, value] : messages) {
        batch.emplace_back(key, value, priority);
    }
    
    return broker_->produce_batch(topic_, batch);
}

void Producer::send_async(const std::string& key, const std::string& value,
                          ProduceCallback callback, MessagePriority priority) {
    std::lock_guard<std::mutex> lock(buffer_mutex_);
    buffer_.push_back({key, value, priority, std::move(callback)});
    
    if (buffer_.size() >= batch_size_) {
        buffer_cv_.notify_one();
    }
}

void Producer::flush() {
    std::vector<PendingMessage> to_send;
    {
        std::lock_guard<std::mutex> lock(buffer_mutex_);
        to_send.swap(buffer_);
    }
    
    for (auto& msg : to_send) {
        auto result = broker_->produce(topic_, msg.key, msg.value, msg.priority);
        if (msg.callback) {
            msg.callback(result);
        }
    }
}

void Producer::set_batch_size(size_t size) {
    batch_size_ = size;
}

void Producer::set_linger_ms(uint32_t ms) {
    linger_ms_ = ms;
}

void Producer::flush_loop() {
    while (running_) {
        std::unique_lock<std::mutex> lock(buffer_mutex_);
        buffer_cv_.wait_for(lock, std::chrono::milliseconds(linger_ms_), [this] {
            return !running_ || buffer_.size() >= batch_size_;
        });
        
        if (!buffer_.empty()) {
            std::vector<PendingMessage> to_send;
            to_send.swap(buffer_);
            lock.unlock();
            
            for (auto& msg : to_send) {
                auto result = broker_->produce(topic_, msg.key, msg.value, msg.priority);
                if (msg.callback) {
                    msg.callback(result);
                }
            }
        }
    }
}

// ==================== Consumer Implementation ====================

Consumer::Consumer(std::shared_ptr<Broker> broker,
                   const std::string& topic,
                   uint32_t partition,
                   const std::string& group_id)
    : broker_(std::move(broker))
    , topic_(topic)
    , partition_(partition)
    , group_id_(group_id)
    , current_offset_(0)
    , auto_commit_enabled_(false)
    , auto_commit_interval_ms_(5000)
    , running_(true) {
    
    // Start from committed offset if group_id is set
    if (!group_id_.empty()) {
        current_offset_ = broker_->get_committed_offset(group_id_, topic_, partition_);
    }
}

Consumer::~Consumer() {
    running_ = false;
    if (auto_commit_thread_.joinable()) {
        auto_commit_thread_.join();
    }
    if (auto_commit_enabled_ && !group_id_.empty()) {
        commit();
    }
}

MessagePtr Consumer::poll(std::chrono::milliseconds timeout) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    
    while (std::chrono::steady_clock::now() < deadline) {
        auto msg = broker_->consume(topic_, partition_, current_offset_);
        if (msg) {
            current_offset_ = msg->offset + 1;
            return msg;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    
    return nullptr;
}

std::vector<MessagePtr> Consumer::poll_batch(size_t max_messages,
                                             std::chrono::milliseconds timeout) {
    (void)timeout;  // Not used for batch - immediate return
    auto messages = broker_->consume_batch(topic_, partition_, current_offset_, max_messages);
    
    if (!messages.empty()) {
        current_offset_ = messages.back()->offset + 1;
    }
    
    return messages;
}

void Consumer::commit() {
    if (!group_id_.empty()) {
        broker_->commit_offset(group_id_, topic_, partition_, current_offset_);
    }
}

void Consumer::commit(uint64_t offset) {
    if (!group_id_.empty()) {
        broker_->commit_offset(group_id_, topic_, partition_, offset);
    }
}

void Consumer::seek(uint64_t offset) {
    current_offset_ = offset;
}

void Consumer::seek_to_beginning() {
    current_offset_ = 0;
}

void Consumer::seek_to_end() {
    current_offset_ = broker_->get_latest_offset(topic_, partition_);
}

void Consumer::seek_to_committed() {
    if (!group_id_.empty()) {
        current_offset_ = broker_->get_committed_offset(group_id_, topic_, partition_);
    }
}

uint64_t Consumer::position() const {
    return current_offset_;
}

void Consumer::enable_auto_commit(std::chrono::milliseconds interval) {
    auto_commit_enabled_ = true;
    auto_commit_interval_ms_ = static_cast<uint64_t>(interval.count());
    
    if (!auto_commit_thread_.joinable()) {
        auto_commit_thread_ = std::thread(&Consumer::auto_commit_loop, this);
    }
}

void Consumer::disable_auto_commit() {
    auto_commit_enabled_ = false;
}

void Consumer::auto_commit_loop() {
    while (running_ && auto_commit_enabled_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(auto_commit_interval_ms_));
        if (running_ && auto_commit_enabled_) {
            commit();
        }
    }
}

// ==================== ConsumerGroup Implementation ====================

ConsumerGroup::ConsumerGroup(std::shared_ptr<Broker> broker,
                             const std::string& group_id,
                             const std::string& topic)
    : broker_(std::move(broker))
    , group_id_(group_id)
    , topic_(topic)
    , partition_count_(broker_->get_partition_count(topic)) {
    
    // Load committed offsets
    for (uint32_t p = 0; p < partition_count_; p++) {
        partition_offsets_[p] = broker_->get_committed_offset(group_id_, topic_, p);
    }
}

ConsumerGroup::~ConsumerGroup() {
    commit_all();
}

std::string ConsumerGroup::join() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::string consumer_id = generate_consumer_id();
    members_.push_back(consumer_id);
    
    rebalance();
    
    return consumer_id;
}

void ConsumerGroup::leave(const std::string& consumer_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    members_.erase(
        std::remove(members_.begin(), members_.end(), consumer_id),
        members_.end()
    );
    
    assignments_.erase(consumer_id);
    
    rebalance();
}

std::vector<uint32_t> ConsumerGroup::get_assigned_partitions(
    const std::string& consumer_id) const {
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = assignments_.find(consumer_id);
    if (it == assignments_.end()) {
        return {};
    }
    return it->second;
}

std::vector<MessagePtr> ConsumerGroup::poll(const std::string& consumer_id,
                                            size_t max_messages,
                                            std::chrono::milliseconds timeout) {
    (void)timeout;  // Not used currently
    
    std::vector<uint32_t> partitions;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = assignments_.find(consumer_id);
        if (it == assignments_.end()) {
            return {};
        }
        partitions = it->second;
    }
    
    if (partitions.empty()) {
        return {};
    }
    
    std::vector<MessagePtr> result;
    size_t per_partition = std::max(size_t(1), max_messages / partitions.size());
    
    for (uint32_t partition : partitions) {
        uint64_t offset;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            offset = partition_offsets_[partition];
        }
        
        auto messages = broker_->consume_batch(topic_, partition, offset, per_partition);
        
        if (!messages.empty()) {
            std::lock_guard<std::mutex> lock(mutex_);
            partition_offsets_[partition] = messages.back()->offset + 1;
        }
        
        result.insert(result.end(), messages.begin(), messages.end());
        
        if (result.size() >= max_messages) break;
    }
    
    return result;
}

void ConsumerGroup::commit(const std::string& consumer_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = assignments_.find(consumer_id);
    if (it == assignments_.end()) return;
    
    for (uint32_t partition : it->second) {
        broker_->commit_offset(group_id_, topic_, partition, partition_offsets_[partition]);
    }
}

void ConsumerGroup::commit_all() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    for (const auto& [partition, offset] : partition_offsets_) {
        broker_->commit_offset(group_id_, topic_, partition, offset);
    }
}

void ConsumerGroup::rebalance() {
    // Clear current assignments
    assignments_.clear();
    
    if (members_.empty()) return;
    
    // Simple range assignment strategy
    size_t num_consumers = members_.size();
    size_t partitions_per_consumer = partition_count_ / num_consumers;
    size_t extra = partition_count_ % num_consumers;
    
    uint32_t current_partition = 0;
    
    for (size_t i = 0; i < num_consumers && current_partition < partition_count_; i++) {
        size_t count = partitions_per_consumer + (i < extra ? 1 : 0);
        
        std::vector<uint32_t> assigned;
        for (size_t j = 0; j < count && current_partition < partition_count_; j++) {
            assigned.push_back(current_partition++);
        }
        
        assignments_[members_[i]] = std::move(assigned);
    }
}

std::string ConsumerGroup::generate_consumer_id() {
    static std::atomic<uint64_t> counter{0};
    return group_id_ + "-consumer-" + std::to_string(counter++);
}

} // namespace stream
