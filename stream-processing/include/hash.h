#pragma once

#include <cstdint>
#include <cstddef>
#include <string>
#include <string_view>
#include <map>

namespace stream {

// =============================================================================
// MurmurHash3 - Fast, high-quality hash for partition routing
// =============================================================================
// Based on MurmurHash3 by Austin Appleby (public domain)
// Using 32-bit variant for partition selection

class Hasher {
public:
    // Hash a key and return partition index
    static uint32_t hash(const void* data, size_t len, uint32_t seed = 0) {
        const uint8_t* bytes = static_cast<const uint8_t*>(data);
        const size_t nblocks = len / 4;
        
        uint32_t h1 = seed;
        
        constexpr uint32_t c1 = 0xcc9e2d51;
        constexpr uint32_t c2 = 0x1b873593;
        
        // Body - process 4-byte blocks
        const uint32_t* blocks = reinterpret_cast<const uint32_t*>(bytes);
        
        for (size_t i = 0; i < nblocks; i++) {
            uint32_t k1 = blocks[i];
            
            k1 *= c1;
            k1 = rotl32(k1, 15);
            k1 *= c2;
            
            h1 ^= k1;
            h1 = rotl32(h1, 13);
            h1 = h1 * 5 + 0xe6546b64;
        }
        
        // Tail - handle remaining bytes
        const uint8_t* tail = bytes + nblocks * 4;
        uint32_t k1 = 0;
        
        switch (len & 3) {
            case 3: k1 ^= tail[2] << 16; [[fallthrough]];
            case 2: k1 ^= tail[1] << 8;  [[fallthrough]];
            case 1: k1 ^= tail[0];
                    k1 *= c1;
                    k1 = rotl32(k1, 15);
                    k1 *= c2;
                    h1 ^= k1;
        }
        
        // Finalization - avalanche
        h1 ^= static_cast<uint32_t>(len);
        h1 = fmix32(h1);
        
        return h1;
    }
    
    // Convenience overloads
    static uint32_t hash(const std::string& key, uint32_t seed = 0) {
        return hash(key.data(), key.size(), seed);
    }
    
    static uint32_t hash(std::string_view key, uint32_t seed = 0) {
        return hash(key.data(), key.size(), seed);
    }
    
    // Get partition index for a key
    static uint32_t partition_for_key(const std::string& key, uint32_t num_partitions) {
        if (num_partitions == 0) return 0;
        if (key.empty()) return 0;  // Empty key goes to partition 0
        return hash(key) % num_partitions;
    }
    
    static uint32_t partition_for_key(std::string_view key, uint32_t num_partitions) {
        if (num_partitions == 0) return 0;
        if (key.empty()) return 0;
        return hash(key) % num_partitions;
    }

private:
    static constexpr uint32_t rotl32(uint32_t x, int8_t r) {
        return (x << r) | (x >> (32 - r));
    }
    
    static constexpr uint32_t fmix32(uint32_t h) {
        h ^= h >> 16;
        h *= 0x85ebca6b;
        h ^= h >> 13;
        h *= 0xc2b2ae35;
        h ^= h >> 16;
        return h;
    }
};

// =============================================================================
// Consistent Hashing - For partition rebalancing (future use)
// =============================================================================

class ConsistentHashRing {
public:
    explicit ConsistentHashRing(uint32_t virtual_nodes = 150)
        : virtual_nodes_(virtual_nodes) {}
    
    // Add a partition to the ring
    void add_partition(uint32_t partition_id) {
        for (uint32_t i = 0; i < virtual_nodes_; i++) {
            uint32_t hash = hash_node(partition_id, i);
            ring_[hash] = partition_id;
        }
    }
    
    // Remove a partition from the ring
    void remove_partition(uint32_t partition_id) {
        for (uint32_t i = 0; i < virtual_nodes_; i++) {
            uint32_t hash = hash_node(partition_id, i);
            ring_.erase(hash);
        }
    }
    
    // Get partition for a key
    uint32_t get_partition(const std::string& key) const {
        if (ring_.empty()) return 0;
        
        uint32_t hash = Hasher::hash(key);
        auto it = ring_.lower_bound(hash);
        
        if (it == ring_.end()) {
            it = ring_.begin();  // Wrap around
        }
        
        return it->second;
    }
    
    size_t size() const { return ring_.size() / virtual_nodes_; }
    bool empty() const { return ring_.empty(); }

private:
    uint32_t hash_node(uint32_t partition_id, uint32_t replica) const {
        // Create a unique string for this virtual node
        std::string node_key = std::to_string(partition_id) + ":" + std::to_string(replica);
        return Hasher::hash(node_key);
    }
    
    uint32_t virtual_nodes_;
    std::map<uint32_t, uint32_t> ring_;  // hash -> partition_id
};

} // namespace stream
