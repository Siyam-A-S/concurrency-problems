#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <memory>
#include <fstream>
#include <mutex>
#include <optional>

#include "message.h"

namespace stream {

/**
 * @brief Append-only log segment for message persistence
 * 
 * Design rationale:
 * - Append-only for simplicity and performance (sequential writes)
 * - Each segment is a single file with a base offset
 * - Messages are stored with a simple framing format
 * - Index maintained in memory for fast offset lookups
 * 
 * File format:
 * [4 bytes: magic number]
 * [4 bytes: version]
 * [8 bytes: base_offset]
 * [8 bytes: message_count] (updated on close)
 * --- messages ---
 * [4 bytes: message_size][N bytes: serialized message]
 * [4 bytes: message_size][N bytes: serialized message]
 * ...
 * 
 * Performance characteristics:
 * - Append: O(1) - sequential write
 * - Read by offset: O(log n) - binary search in index
 * - Read sequential: O(1) per message
 * 
 * Thread safety:
 * - Single writer, multiple readers
 * - Mutex protects append operations
 * - Reads are lock-free once index is built
 */
class LogSegment {
public:
    // Magic number to identify log files: "SLOG" in ASCII
    static constexpr uint32_t MAGIC = 0x474F4C53;
    static constexpr uint32_t VERSION = 1;
    static constexpr size_t HEADER_SIZE = 24; // magic + version + base_offset + count
    
    /**
     * @brief Create a new log segment
     * 
     * @param path File path for the segment
     * @param base_offset Starting offset for messages in this segment
     * @return LogSegment or nullptr on failure
     */
    static std::unique_ptr<LogSegment> create(const std::string& path, uint64_t base_offset);
    
    /**
     * @brief Open an existing log segment
     * 
     * @param path File path for the segment
     * @return LogSegment or nullptr on failure
     */
    static std::unique_ptr<LogSegment> open(const std::string& path);
    
    ~LogSegment();
    
    // Non-copyable
    LogSegment(const LogSegment&) = delete;
    LogSegment& operator=(const LogSegment&) = delete;
    
    /**
     * @brief Append a message to the segment
     * 
     * @param msg Message to append
     * @return Offset assigned to the message, or 0 on failure
     * 
     * The message's offset field will be updated with the assigned offset.
     */
    uint64_t append(Message& msg);
    
    /**
     * @brief Read a message by offset
     * 
     * @param offset The message offset to read
     * @return Message or nullptr if not found
     */
    MessagePtr read(uint64_t offset) const;
    
    /**
     * @brief Read multiple messages starting from an offset
     * 
     * @param start_offset Starting offset (inclusive)
     * @param max_messages Maximum number of messages to read
     * @return Vector of messages
     */
    std::vector<MessagePtr> read_batch(uint64_t start_offset, size_t max_messages) const;
    
    /**
     * @brief Flush buffered data to disk
     */
    bool flush();
    
    /**
     * @brief Close the segment (flushes and updates header)
     */
    void close();
    
    /**
     * @brief Check if segment contains an offset
     */
    bool contains_offset(uint64_t offset) const {
        return offset >= base_offset_ && offset < next_offset_;
    }
    
    // Accessors
    const std::string& path() const { return path_; }
    uint64_t base_offset() const { return base_offset_; }
    uint64_t next_offset() const { return next_offset_; }
    uint64_t message_count() const { return index_.size(); }
    size_t file_size() const { return file_size_; }
    bool is_closed() const { return closed_; }
    
    /**
     * @brief Check if segment should be rotated (too large)
     */
    bool should_rotate(size_t max_segment_bytes) const {
        return file_size_ >= max_segment_bytes;
    }
    
private:
    LogSegment() = default;
    
    /**
     * @brief Index entry: maps offset to file position
     */
    struct IndexEntry {
        uint64_t offset;
        uint64_t file_position;
        uint32_t message_size;
    };
    
    /**
     * @brief Build index by scanning the file
     */
    bool build_index();
    
    /**
     * @brief Find index entry for an offset
     */
    const IndexEntry* find_entry(uint64_t offset) const;
    
    /**
     * @brief Write file header
     */
    bool write_header();
    
    /**
     * @brief Read and validate file header
     */
    bool read_header();
    
    /**
     * @brief Update message count in header
     */
    bool update_header_count();
    
    std::string path_;
    uint64_t base_offset_{0};
    uint64_t next_offset_{0};
    size_t file_size_{0};
    bool closed_{false};
    
    // File handles
    mutable std::fstream file_;
    mutable std::mutex write_mutex_;
    
    // In-memory index for fast lookups
    std::vector<IndexEntry> index_;
};

/**
 * @brief Manages multiple log segments for a partition
 * 
 * Handles:
 * - Segment rotation when max size reached
 * - Finding correct segment for an offset
 * - Cleaning up old segments (retention)
 */
class LogManager {
public:
    struct Config {
        std::string base_path;                           // Directory for log files
        size_t max_segment_bytes{1024 * 1024 * 1024};   // 1GB per segment
        uint64_t retention_ms{7 * 24 * 3600 * 1000ULL}; // 7 days
    };
    
    explicit LogManager(const Config& config);
    ~LogManager();
    
    // Non-copyable
    LogManager(const LogManager&) = delete;
    LogManager& operator=(const LogManager&) = delete;
    
    /**
     * @brief Append a message (handles segment rotation)
     */
    uint64_t append(Message& msg);
    
    /**
     * @brief Read a message by offset
     */
    MessagePtr read(uint64_t offset) const;
    
    /**
     * @brief Read batch of messages
     */
    std::vector<MessagePtr> read_batch(uint64_t start_offset, size_t max_messages) const;
    
    /**
     * @brief Flush all segments
     */
    bool flush();
    
    /**
     * @brief Get the earliest available offset
     */
    uint64_t earliest_offset() const;
    
    /**
     * @brief Get the next offset to be written
     */
    uint64_t latest_offset() const;
    
    /**
     * @brief Apply retention policy (delete old segments)
     */
    void apply_retention();
    
private:
    /**
     * @brief Create a new active segment
     */
    bool rotate_segment();
    
    /**
     * @brief Load existing segments from disk
     */
    bool load_segments();
    
    /**
     * @brief Find segment containing an offset
     */
    LogSegment* find_segment(uint64_t offset) const;
    
    /**
     * @brief Generate segment filename
     */
    std::string segment_filename(uint64_t base_offset) const;
    
    Config config_;
    std::vector<std::unique_ptr<LogSegment>> segments_;
    LogSegment* active_segment_{nullptr};
    mutable std::mutex mutex_;
};

} // namespace stream
