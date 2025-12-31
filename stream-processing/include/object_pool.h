#pragma once

#include <memory>
#include <vector>
#include <mutex>
#include <functional>

namespace stream {

/**
 * @brief Thread-safe object pool for object reuse - MVP version
 * 
 * Design rationale for MVP:
 * - Uses std::mutex for thread safety (simple, correct)
 * - Returns std::shared_ptr with custom deleter for automatic recycling
 * - Falls back to new allocation when pool is empty
 * - No complex lock-free algorithms
 * 
 * Benefits over raw new/delete:
 * - Object reuse reduces allocation overhead
 * - Custom deleter automatically returns objects to pool
 * - Shared_ptr provides thread-safe reference counting
 * - Memory locality (reused objects likely in cache)
 * 
 * Performance characteristics:
 * - Acquire: ~100-500ns (mutex + pool access)
 * - Release: ~100-500ns (mutex + pool return)
 * - Good enough for most use cases
 * 
 * Future optimization path (profile first!):
 * - Thread-local pools to eliminate contention
 * - Lock-free free list with tagged pointers
 * - Custom allocator for arena allocation
 * 
 * When to upgrade:
 * - If profiling shows >5% time in pool operations
 * - If lock contention is measured and significant
 * - If >1M alloc/free per second needed
 */
template<typename T>
class ObjectPool : public std::enable_shared_from_this<ObjectPool<T>> {
public:
    using Ptr = std::shared_ptr<T>;
    
    /**
     * @brief Construct pool with optional pre-allocation
     * @param initial_size Number of objects to pre-allocate (0 = on-demand)
     */
    explicit ObjectPool(size_t initial_size = 0) {
        if (initial_size > 0) {
            std::lock_guard<std::mutex> lock(mutex_);
            pool_.reserve(initial_size);
            for (size_t i = 0; i < initial_size; ++i) {
                pool_.push_back(std::make_unique<T>());
            }
        }
    }
    
    // Non-copyable (contains mutex and unique_ptrs)
    ObjectPool(const ObjectPool&) = delete;
    ObjectPool& operator=(const ObjectPool&) = delete;
    
    /**
     * @brief Acquire an object from the pool
     * 
     * Returns a shared_ptr with a custom deleter that returns
     * the object to the pool when the last reference is released.
     * 
     * @param args Constructor arguments for new objects
     * @return Shared pointer to object (never null)
     */
    template<typename... Args>
    Ptr acquire(Args&&... args) {
        std::unique_ptr<T> obj;
        
        {
            std::lock_guard<std::mutex> lock(mutex_);
            ++total_acquisitions_;
            
            if (!pool_.empty()) {
                obj = std::move(pool_.back());
                pool_.pop_back();
                ++pool_hits_;
            }
        }
        
        if (!obj) {
            // Pool was empty - create new object
            obj = std::make_unique<T>(std::forward<Args>(args)...);
        } else if constexpr (sizeof...(args) > 0) {
            // Reset existing object with new arguments
            *obj = T(std::forward<Args>(args)...);
        }
        
        // Create shared_ptr with custom deleter that returns to pool
        // Use weak_ptr to avoid preventing pool destruction
        std::weak_ptr<ObjectPool<T>> weak_pool = this->shared_from_this();
        
        return Ptr(obj.release(), [weak_pool](T* ptr) {
            if (auto pool = weak_pool.lock()) {
                pool->release(ptr);
            } else {
                // Pool was destroyed - just delete
                delete ptr;
            }
        });
    }
    
    /**
     * @brief Get pool statistics
     */
    struct Stats {
        size_t pool_size;           // Current objects in pool
        size_t total_acquisitions;  // Total acquire() calls
        size_t pool_hits;           // Times object was reused from pool
        
        double hit_rate() const {
            return total_acquisitions > 0 
                ? static_cast<double>(pool_hits) / total_acquisitions 
                : 0.0;
        }
    };
    
    Stats get_stats() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return Stats{pool_.size(), total_acquisitions_, pool_hits_};
    }
    
    /**
     * @brief Pre-allocate objects
     */
    void reserve(size_t count) {
        std::lock_guard<std::mutex> lock(mutex_);
        pool_.reserve(pool_.size() + count);
        for (size_t i = 0; i < count; ++i) {
            pool_.push_back(std::make_unique<T>());
        }
    }
    
    /**
     * @brief Clear all pooled objects
     */
    void clear() {
        std::lock_guard<std::mutex> lock(mutex_);
        pool_.clear();
    }
    
    /**
     * @brief Set maximum pool size (excess objects are deleted)
     */
    void set_max_size(size_t max_size) {
        std::lock_guard<std::mutex> lock(mutex_);
        max_pool_size_ = max_size;
        while (pool_.size() > max_pool_size_) {
            pool_.pop_back();
        }
    }
    
private:
    /**
     * @brief Return object to pool (called by custom deleter)
     */
    void release(T* ptr) {
        if (ptr == nullptr) return;
        
        std::lock_guard<std::mutex> lock(mutex_);
        
        if (pool_.size() < max_pool_size_) {
            pool_.push_back(std::unique_ptr<T>(ptr));
        } else {
            // Pool is full - just delete
            delete ptr;
        }
    }
    
    std::vector<std::unique_ptr<T>> pool_;
    size_t max_pool_size_{10000};  // Prevent unbounded growth
    
    // Statistics
    size_t total_acquisitions_{0};
    size_t pool_hits_{0};
    
    mutable std::mutex mutex_;
};

/**
 * @brief Factory function to create a managed pool
 */
template<typename T>
std::shared_ptr<ObjectPool<T>> make_object_pool(size_t initial_size = 0) {
    return std::make_shared<ObjectPool<T>>(initial_size);
}

/**
 * @brief Simple per-thread pool using thread_local
 * 
 * Benefits:
 * - No contention between threads
 * - Best cache locality
 * 
 * Drawbacks:
 * - Higher memory usage (one pool per thread)
 * - Objects not shared between threads
 */
template<typename T>
class ThreadLocalPool {
public:
    template<typename... Args>
    static std::shared_ptr<T> acquire(Args&&... args) {
        return get_pool()->acquire(std::forward<Args>(args)...);
    }
    
    static typename ObjectPool<T>::Stats get_stats() {
        return get_pool()->get_stats();
    }
    
private:
    static std::shared_ptr<ObjectPool<T>>& get_pool() {
        thread_local auto pool = make_object_pool<T>(100);
        return pool;
    }
};

/**
 * @brief Example usage:
 * 
 * // Create a shared pool
 * auto pool = make_object_pool<Message>(1000);
 * 
 * // Acquire objects (automatically returned to pool when done)
 * {
 *     auto msg = pool->acquire("key", "value");
 *     process(msg);
 * } // msg automatically returned to pool here
 * 
 * // Check statistics
 * auto stats = pool->get_stats();
 * std::cout << "Pool hit rate: " << stats.hit_rate() * 100 << "%" << std::endl;
 * 
 * // Per-thread pool (no contention)
 * auto msg = ThreadLocalPool<Message>::acquire("key", "value");
 */

} // namespace stream
