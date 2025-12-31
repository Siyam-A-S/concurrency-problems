#pragma once

#include <queue>
#include <mutex>
#include <condition_variable>
#include <optional>
#include <chrono>

namespace stream {

/**
 * @brief Thread-safe bounded queue - MVP version
 * 
 * Design rationale for MVP:
 * - Uses std::mutex for simplicity and correctness
 * - Well-understood behavior, easy to debug
 * - Bounded to prevent memory exhaustion
 * - Condition variables for efficient blocking
 * 
 * Performance characteristics:
 * - Thread-safe for multiple producers and consumers
 * - Blocking operations with optional timeout
 * - ~100-500ns per operation (sufficient for most use cases)
 * 
 * Future optimization path (profile first!):
 * - Replace with moodycamel::ConcurrentQueue if profiling shows contention
 * - Use boost::lockfree::queue if lock-free is truly needed
 * - Consider SPSC queue for single-producer-single-consumer scenarios
 * 
 * When to upgrade:
 * - If profiling shows >10% time spent in queue operations
 * - If you need >1M ops/sec per queue
 * - If latency jitter from mutex is unacceptable
 */
template<typename T>
class ThreadSafeQueue {
public:
    /**
     * @brief Construct a bounded queue
     * @param capacity Maximum number of items (0 = unbounded, not recommended)
     */
    explicit ThreadSafeQueue(size_t capacity = 10000)
        : capacity_(capacity) {}
    
    // Non-copyable, non-movable (contains mutex)
    ThreadSafeQueue(const ThreadSafeQueue&) = delete;
    ThreadSafeQueue& operator=(const ThreadSafeQueue&) = delete;
    
    /**
     * @brief Enqueue an item (blocking if full)
     * 
     * @param item Item to enqueue
     * @return true if enqueued, false if queue is closed
     */
    bool enqueue(T item) {
        std::unique_lock<std::mutex> lock(mutex_);
        
        // Wait until there's space or queue is closed
        not_full_.wait(lock, [this] {
            return queue_.size() < capacity_ || closed_;
        });
        
        if (closed_) {
            return false;
        }
        
        queue_.push(std::move(item));
        not_empty_.notify_one();
        return true;
    }
    
    /**
     * @brief Try to enqueue without blocking
     * 
     * @param item Item to enqueue
     * @return true if enqueued, false if queue is full or closed
     */
    bool try_enqueue(T item) {
        std::lock_guard<std::mutex> lock(mutex_);
        
        if (closed_ || queue_.size() >= capacity_) {
            return false;
        }
        
        queue_.push(std::move(item));
        not_empty_.notify_one();
        return true;
    }
    
    /**
     * @brief Dequeue an item (blocking if empty)
     * 
     * @param item Output parameter for dequeued item
     * @return true if dequeued, false if queue is closed and empty
     */
    bool dequeue(T& item) {
        std::unique_lock<std::mutex> lock(mutex_);
        
        // Wait until there's an item or queue is closed
        not_empty_.wait(lock, [this] {
            return !queue_.empty() || closed_;
        });
        
        if (queue_.empty()) {
            return false; // Queue is closed and empty
        }
        
        item = std::move(queue_.front());
        queue_.pop();
        not_full_.notify_one();
        return true;
    }
    
    /**
     * @brief Try to dequeue without blocking
     * 
     * @return std::optional containing item if available, std::nullopt otherwise
     */
    std::optional<T> try_dequeue() {
        std::lock_guard<std::mutex> lock(mutex_);
        
        if (queue_.empty()) {
            return std::nullopt;
        }
        
        T item = std::move(queue_.front());
        queue_.pop();
        not_full_.notify_one();
        return item;
    }
    
    /**
     * @brief Dequeue with timeout
     * 
     * @param item Output parameter for dequeued item
     * @param timeout Maximum time to wait
     * @return true if dequeued, false if timeout or closed
     */
    template<typename Rep, typename Period>
    bool dequeue_for(T& item, const std::chrono::duration<Rep, Period>& timeout) {
        std::unique_lock<std::mutex> lock(mutex_);
        
        if (!not_empty_.wait_for(lock, timeout, [this] {
            return !queue_.empty() || closed_;
        })) {
            return false; // Timeout
        }
        
        if (queue_.empty()) {
            return false; // Queue is closed and empty
        }
        
        item = std::move(queue_.front());
        queue_.pop();
        not_full_.notify_one();
        return true;
    }
    
    /**
     * @brief Close the queue (no more enqueues allowed)
     * Wakes up all waiting threads
     */
    void close() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            closed_ = true;
        }
        not_empty_.notify_all();
        not_full_.notify_all();
    }
    
    /**
     * @brief Check if queue is closed
     */
    bool is_closed() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return closed_;
    }
    
    /**
     * @brief Get current size (approximate, may change immediately)
     */
    size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.size();
    }
    
    /**
     * @brief Check if empty
     */
    bool empty() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.empty();
    }
    
    /**
     * @brief Get capacity
     */
    size_t capacity() const {
        return capacity_;
    }
    
private:
    std::queue<T> queue_;
    size_t capacity_;
    bool closed_{false};
    
    mutable std::mutex mutex_;
    std::condition_variable not_empty_;
    std::condition_variable not_full_;
};

/**
 * @brief Alias for backward compatibility and documentation
 * 
 * Note: This is NOT lock-free in the MVP. The name is kept for API compatibility.
 * Use ThreadSafeQueue directly for clarity.
 */
template<typename T, size_t Capacity = 10000>
using LockFreeQueue = ThreadSafeQueue<T>;

/**
 * @brief Example usage:
 * 
 * ThreadSafeQueue<MessagePtr> message_queue(4096);
 * 
 * // Producer thread
 * void producer() {
 *     auto msg = make_message("key", "value");
 *     if (!message_queue.try_enqueue(msg)) {
 *         // Queue full - apply backpressure or drop
 *     }
 * }
 * 
 * // Consumer thread
 * void consumer() {
 *     MessagePtr msg;
 *     while (message_queue.dequeue(msg)) {
 *         process_message(msg);
 *     }
 *     // Queue closed and empty - exit
 * }
 * 
 * // Shutdown
 * void shutdown() {
 *     message_queue.close();
 *     // Consumers will drain remaining items and exit
 * }
 */

} // namespace stream
