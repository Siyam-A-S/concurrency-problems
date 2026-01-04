#include "topic.h"
#include <filesystem>
#include <algorithm>

namespace fs = std::filesystem;

namespace stream {

// =============================================================================
// Topic Implementation
// =============================================================================

Topic::Topic(const Config& config)
    : config_(config) {
    
    // Create partitions
    partitions_.reserve(config_.num_partitions);
    
    for (uint32_t i = 0; i < config_.num_partitions; i++) {
        Partition::Config part_config;
        part_config.topic_name = config_.name;
        part_config.partition_id = i;
        part_config.base_path = config_.base_path;
        part_config.segment_max_bytes = config_.segment_max_bytes;
        part_config.segment_max_messages = config_.segment_max_messages;
        part_config.retention_bytes = config_.retention_bytes;
        part_config.retention_ms = config_.retention_ms;
        
        partitions_.push_back(std::make_unique<Partition>(part_config));
    }
}

Topic::ProduceResult Topic::produce(Message& msg) {
    uint32_t partition_id = select_partition(msg);
    return produce(msg, partition_id);
}

Topic::ProduceResult Topic::produce(Message& msg, uint32_t partition_id) {
    ProduceResult result;
    result.partition_id = partition_id;
    
    if (partition_id >= partitions_.size()) {
        result.success = false;
        result.error = "Invalid partition ID: " + std::to_string(partition_id);
        return result;
    }
    
    // Set partition in message
    msg.partition = partition_id;
    
    uint64_t offset = partitions_[partition_id]->append(msg);
    
    // Append always succeeds in current implementation
    // Note: offset 0 is valid for first message
    result.success = true;
    result.offset = offset;
    
    return result;
}

std::vector<Topic::ProduceResult> Topic::produce_batch(std::vector<Message>& messages) {
    std::vector<ProduceResult> results;
    results.reserve(messages.size());
    
    // Group messages by partition
    std::unordered_map<uint32_t, std::vector<size_t>> partition_indices;
    
    for (size_t i = 0; i < messages.size(); i++) {
        uint32_t partition_id = select_partition(messages[i]);
        messages[i].partition = partition_id;
        partition_indices[partition_id].push_back(i);
    }
    
    // Initialize results
    results.resize(messages.size());
    
    // Append to each partition
    for (auto& [partition_id, indices] : partition_indices) {
        if (partition_id >= partitions_.size()) {
            for (size_t idx : indices) {
                results[idx].success = false;
                results[idx].partition_id = partition_id;
                results[idx].error = "Invalid partition";
            }
            continue;
        }
        
        // Append messages to this partition
        for (size_t idx : indices) {
            uint64_t offset = partitions_[partition_id]->append(messages[idx]);
            results[idx].partition_id = partition_id;
            results[idx].success = true;  // offset 0 is valid
            results[idx].offset = offset;
        }
    }
    
    return results;
}

Topic::FetchResult Topic::fetch(uint32_t partition_id, 
                                 uint64_t offset, 
                                 size_t max_messages) const {
    FetchResult result;
    
    if (partition_id >= partitions_.size()) {
        result.success = false;
        result.error = "Invalid partition ID: " + std::to_string(partition_id);
        return result;
    }
    
    result.messages = partitions_[partition_id]->read_batch(offset, max_messages);
    result.success = true;
    
    // Calculate next offset
    if (!result.messages.empty()) {
        result.next_offset = result.messages.back()->offset + 1;
    } else {
        result.next_offset = offset;
    }
    
    return result;
}

std::vector<Topic::FetchResult> Topic::fetch_all(
    const std::unordered_map<uint32_t, uint64_t>& offsets,
    size_t max_messages_per_partition) const {
    
    std::vector<FetchResult> results;
    results.reserve(offsets.size());
    
    for (const auto& [partition_id, offset] : offsets) {
        results.push_back(fetch(partition_id, offset, max_messages_per_partition));
    }
    
    return results;
}

Partition* Topic::partition(uint32_t id) {
    if (id >= partitions_.size()) return nullptr;
    return partitions_[id].get();
}

const Partition* Topic::partition(uint32_t id) const {
    if (id >= partitions_.size()) return nullptr;
    return partitions_[id].get();
}

uint32_t Topic::partition_for_key(const std::string& key) const {
    return Hasher::partition_for_key(key, static_cast<uint32_t>(partitions_.size()));
}

Topic::Stats Topic::stats() const {
    Stats s;
    s.name = config_.name;
    s.num_partitions = static_cast<uint32_t>(partitions_.size());
    s.partition_stats.reserve(partitions_.size());
    
    for (const auto& partition : partitions_) {
        auto ps = partition->stats();
        s.total_messages += ps.messages_in;
        s.total_bytes += ps.bytes_in;
        s.partition_stats.push_back(ps);
    }
    
    return s;
}

std::unordered_map<uint32_t, uint64_t> Topic::start_offsets() const {
    std::unordered_map<uint32_t, uint64_t> offsets;
    
    for (uint32_t i = 0; i < partitions_.size(); i++) {
        offsets[i] = partitions_[i]->start_offset();
    }
    
    return offsets;
}

std::unordered_map<uint32_t, uint64_t> Topic::end_offsets() const {
    std::unordered_map<uint32_t, uint64_t> offsets;
    
    for (uint32_t i = 0; i < partitions_.size(); i++) {
        offsets[i] = partitions_[i]->end_offset();
    }
    
    return offsets;
}

bool Topic::flush() {
    bool success = true;
    for (auto& partition : partitions_) {
        if (!partition->flush()) {
            success = false;
        }
    }
    return success;
}

uint32_t Topic::select_partition(const Message& msg) const {
    if (!msg.key.empty()) {
        // Use key hash for partition selection
        return Hasher::partition_for_key(msg.key, 
                                         static_cast<uint32_t>(partitions_.size()));
    }
    
    // Round-robin for messages without keys
    uint32_t idx = round_robin_counter_.fetch_add(1, std::memory_order_relaxed);
    return idx % static_cast<uint32_t>(partitions_.size());
}

// =============================================================================
// TopicManager Implementation
// =============================================================================

TopicManager::TopicManager(const Config& config)
    : config_(config) {
    
    // Create base directory if needed
    std::error_code ec;
    fs::create_directories(config_.base_path, ec);
}

Topic* TopicManager::create_topic(const std::string& name, uint32_t num_partitions) {
    Topic::Config config;
    config.name = name;
    config.num_partitions = (num_partitions > 0) ? num_partitions 
                                                  : config_.default_num_partitions;
    config.base_path = topic_path(name);
    config.segment_max_bytes = config_.default_segment_max_bytes;
    config.retention_bytes = config_.default_retention_bytes;
    config.retention_ms = config_.default_retention_ms;
    
    return create_topic(config);
}

Topic* TopicManager::create_topic(const Topic::Config& config) {
    std::unique_lock lock(mutex_);
    
    // Check if already exists
    if (topics_.count(config.name) > 0) {
        return topics_[config.name].get();
    }
    
    // Create topic directory
    std::error_code ec;
    fs::create_directories(config.base_path, ec);
    
    // Create topic
    auto topic = std::make_unique<Topic>(config);
    Topic* ptr = topic.get();
    topics_[config.name] = std::move(topic);
    
    return ptr;
}

Topic* TopicManager::get_topic(const std::string& name) {
    std::shared_lock lock(mutex_);
    
    auto it = topics_.find(name);
    if (it == topics_.end()) {
        return nullptr;
    }
    return it->second.get();
}

const Topic* TopicManager::get_topic(const std::string& name) const {
    std::shared_lock lock(mutex_);
    
    auto it = topics_.find(name);
    if (it == topics_.end()) {
        return nullptr;
    }
    return it->second.get();
}

bool TopicManager::topic_exists(const std::string& name) const {
    std::shared_lock lock(mutex_);
    return topics_.count(name) > 0;
}

bool TopicManager::delete_topic(const std::string& name) {
    std::unique_lock lock(mutex_);
    
    auto it = topics_.find(name);
    if (it == topics_.end()) {
        return false;
    }
    
    std::string path = it->second->path();
    topics_.erase(it);
    
    // Remove files
    std::error_code ec;
    fs::remove_all(path, ec);
    
    return !ec;
}

std::vector<std::string> TopicManager::list_topics() const {
    std::shared_lock lock(mutex_);
    
    std::vector<std::string> names;
    names.reserve(topics_.size());
    
    for (const auto& [name, _] : topics_) {
        names.push_back(name);
    }
    
    std::sort(names.begin(), names.end());
    return names;
}

Topic* TopicManager::get_or_create_topic(const std::string& name, 
                                          uint32_t num_partitions) {
    // Try read-only lookup first
    {
        std::shared_lock lock(mutex_);
        auto it = topics_.find(name);
        if (it != topics_.end()) {
            return it->second.get();
        }
    }
    
    // Create if not found
    return create_topic(name, num_partitions);
}

void TopicManager::flush_all() {
    std::shared_lock lock(mutex_);
    
    for (auto& [_, topic] : topics_) {
        topic->flush();
    }
}

void TopicManager::load_topics() {
    if (!fs::exists(config_.base_path)) {
        return;
    }
    
    std::unique_lock lock(mutex_);
    
    // Scan for topic directories
    for (const auto& entry : fs::directory_iterator(config_.base_path)) {
        if (!entry.is_directory()) continue;
        
        std::string dir_name = entry.path().filename().string();
        
        // Skip if already loaded
        if (topics_.count(dir_name) > 0) continue;
        
        // Count partitions by looking for partition directories
        uint32_t num_partitions = 0;
        for (const auto& sub : fs::directory_iterator(entry.path())) {
            if (sub.is_directory()) {
                std::string sub_name = sub.path().filename().string();
                // Check if it matches pattern: topicname-N
                if (sub_name.find(dir_name + "-") == 0) {
                    num_partitions++;
                }
            }
        }
        
        if (num_partitions == 0) {
            num_partitions = config_.default_num_partitions;
        }
        
        // Create topic config
        Topic::Config config;
        config.name = dir_name;
        config.num_partitions = num_partitions;
        config.base_path = entry.path().string();
        config.segment_max_bytes = config_.default_segment_max_bytes;
        config.retention_bytes = config_.default_retention_bytes;
        config.retention_ms = config_.default_retention_ms;
        
        // Load topic (don't hold lock during construction)
        lock.unlock();
        auto topic = std::make_unique<Topic>(config);
        lock.lock();
        
        topics_[dir_name] = std::move(topic);
    }
}

std::string TopicManager::topic_path(const std::string& name) const {
    return config_.base_path + "/" + name;
}

} // namespace stream
