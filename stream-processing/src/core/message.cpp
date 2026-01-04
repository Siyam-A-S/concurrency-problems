/**
 * @file message.cpp
 * @brief Message serialization and checksum implementation
 */

#include "message.h"
#include <cstring>
#include <stdexcept>

namespace stream {

// =============================================================================
// CRC32 Implementation
// =============================================================================

namespace {

/**
 * @brief CRC32 lookup table (IEEE polynomial)
 * 
 * Pre-computed table for fast CRC32 calculation.
 * Polynomial: 0xEDB88320 (IEEE 802.3, reversed)
 */
constexpr uint32_t crc32_table[256] = {
    0x00000000, 0x77073096, 0xee0e612c, 0x990951ba, 0x076dc419, 0x706af48f,
    0xe963a535, 0x9e6495a3, 0x0edb8832, 0x79dcb8a4, 0xe0d5e91e, 0x97d2d988,
    0x09b64c2b, 0x7eb17cbd, 0xe7b82d07, 0x90bf1d91, 0x1db71064, 0x6ab020f2,
    0xf3b97148, 0x84be41de, 0x1adad47d, 0x6ddde4eb, 0xf4d4b551, 0x83d385c7,
    0x136c9856, 0x646ba8c0, 0xfd62f97a, 0x8a65c9ec, 0x14015c4f, 0x63066cd9,
    0xfa0f3d63, 0x8d080df5, 0x3b6e20c8, 0x4c69105e, 0xd56041e4, 0xa2677172,
    0x3c03e4d1, 0x4b04d447, 0xd20d85fd, 0xa50ab56b, 0x35b5a8fa, 0x42b2986c,
    0xdbbbc9d6, 0xacbcf940, 0x32d86ce3, 0x45df5c75, 0xdcd60dcf, 0xabd13d59,
    0x26d930ac, 0x51de003a, 0xc8d75180, 0xbfd06116, 0x21b4f4b5, 0x56b3c423,
    0xcfba9599, 0xb8bda50f, 0x2802b89e, 0x5f058808, 0xc60cd9b2, 0xb10be924,
    0x2f6f7c87, 0x58684c11, 0xc1611dab, 0xb6662d3d, 0x76dc4190, 0x01db7106,
    0x98d220bc, 0xefd5102a, 0x71b18589, 0x06b6b51f, 0x9fbfe4a5, 0xe8b8d433,
    0x7807c9a2, 0x0f00f934, 0x9609a88e, 0xe10e9818, 0x7f6a0dbb, 0x086d3d2d,
    0x91646c97, 0xe6635c01, 0x6b6b51f4, 0x1c6c6162, 0x856530d8, 0xf262004e,
    0x6c0695ed, 0x1b01a57b, 0x8208f4c1, 0xf50fc457, 0x65b0d9c6, 0x12b7e950,
    0x8bbeb8ea, 0xfcb9887c, 0x62dd1ddf, 0x15da2d49, 0x8cd37cf3, 0xfbd44c65,
    0x4db26158, 0x3ab551ce, 0xa3bc0074, 0xd4bb30e2, 0x4adfa541, 0x3dd895d7,
    0xa4d1c46d, 0xd3d6f4fb, 0x4369e96a, 0x346ed9fc, 0xad678846, 0xda60b8d0,
    0x44042d73, 0x33031de5, 0xaa0a4c5f, 0xdd0d7a9b, 0x5005713c, 0x270241aa,
    0xbe0b1010, 0xc90c2086, 0x5768b525, 0x206f85b3, 0xb966d409, 0xce61e49f,
    0x5edef90e, 0x29d9c998, 0xb0d09822, 0xc7d7a8b4, 0x59b33d17, 0x2eb40d81,
    0xb7bd5c3b, 0xc0ba6cad, 0xedb88320, 0x9abfb3b6, 0x03b6e20c, 0x74b1d29a,
    0xead54739, 0x9dd277af, 0x04db2615, 0x73dc1683, 0xe3630b12, 0x94643b84,
    0x0d6d6a3e, 0x7a6a5aa8, 0xe40ecf0b, 0x9309ff9d, 0x0a00ae27, 0x7d079eb1,
    0xf00f9344, 0x8708a3d2, 0x1e01f268, 0x6906c2fe, 0xf762575d, 0x806567cb,
    0x196c3671, 0x6e6b06e7, 0xfed41b76, 0x89d32be0, 0x10da7a5a, 0x67dd4acc,
    0xf9b9df6f, 0x8ebeeff9, 0x17b7be43, 0x60b08ed5, 0xd6d6a3e8, 0xa1d1937e,
    0x38d8c2c4, 0x4fdff252, 0xd1bb67f1, 0xa6bc5767, 0x3fb506dd, 0x48b2364b,
    0xd80d2bda, 0xaf0a1b4c, 0x36034af6, 0x41047a60, 0xdf60efc3, 0xa867df55,
    0x316e8eef, 0x4669be79, 0xcb61b38c, 0xbc66831a, 0x256fd2a0, 0x5268e236,
    0xcc0c7795, 0xbb0b4703, 0x220216b9, 0x5505262f, 0xc5ba3bbe, 0xb2bd0b28,
    0x2bb45a92, 0x5cb36a04, 0xc2d7ffa7, 0xb5d0cf31, 0x2cd99e8b, 0x5bdeae1d,
    0x9b64c2b0, 0xec63f226, 0x756aa39c, 0x026d930a, 0x9c0906a9, 0xeb0e363f,
    0x72076785, 0x05005713, 0x95bf4a82, 0xe2b87a14, 0x7bb12bae, 0x0cb61b38,
    0x92d28e9b, 0xe5d5be0d, 0x7cdcefb7, 0x0bdbdf21, 0x86d3d2d4, 0xf1d4e242,
    0x68ddb3f8, 0x1fda836e, 0x81be16cd, 0xf6b9265b, 0x6fb077e1, 0x18b74777,
    0x88085ae6, 0xff0f6a70, 0x66063bca, 0x11010b5c, 0x8f659eff, 0xf862ae69,
    0x616bffd3, 0x166ccf45, 0xa00ae278, 0xd70dd2ee, 0x4e048354, 0x3903b3c2,
    0xa7672661, 0xd06016f7, 0x4969474d, 0x3e6e77db, 0xaed16a4a, 0xd9d65adc,
    0x40df0b66, 0x37d83bf0, 0xa9bcae53, 0xdede86c5, 0x47d7367f, 0x30d006e9,
    0xbdd3ad1c, 0xcad48c8a, 0x53dda430, 0x24daa4a6, 0xbad16705, 0xcdd64793,
    0x54df1629, 0x23d967bf, 0xb3667a2e, 0xc4614ab8, 0x5d681b02, 0x2a6f2b94,
    0xb40bbe37, 0xc30c8ea1, 0x5a05df1b, 0x2d02ef8d
};

/**
 * @brief Calculate CRC32 checksum
 */
uint32_t calculate_crc32(const void* data, size_t length) {
    const uint8_t* bytes = static_cast<const uint8_t*>(data);
    uint32_t crc = 0xFFFFFFFF;
    
    for (size_t i = 0; i < length; ++i) {
        crc = crc32_table[(crc ^ bytes[i]) & 0xFF] ^ (crc >> 8);
    }
    
    return crc ^ 0xFFFFFFFF;
}

/**
 * @brief Update CRC32 with additional data
 */
uint32_t update_crc32(uint32_t crc, const void* data, size_t length) {
    const uint8_t* bytes = static_cast<const uint8_t*>(data);
    crc = ~crc; // Un-finalize
    
    for (size_t i = 0; i < length; ++i) {
        crc = crc32_table[(crc ^ bytes[i]) & 0xFF] ^ (crc >> 8);
    }
    
    return ~crc; // Re-finalize
}

// =============================================================================
// Serialization Helpers
// =============================================================================

/**
 * @brief Write a value to buffer in little-endian format
 */
template<typename T>
void write_le(uint8_t*& ptr, T value) {
    for (size_t i = 0; i < sizeof(T); ++i) {
        *ptr++ = static_cast<uint8_t>(value >> (i * 8));
    }
}

/**
 * @brief Read a value from buffer in little-endian format
 */
template<typename T>
T read_le(const uint8_t*& ptr) {
    T value = 0;
    for (size_t i = 0; i < sizeof(T); ++i) {
        value |= static_cast<T>(*ptr++) << (i * 8);
    }
    return value;
}

} // anonymous namespace

// =============================================================================
// Message Implementation
// =============================================================================

void Message::compute_checksum() {
    // Compute checksum over: offset, timestamp, priority, codec, key, value
    // (excluding checksum itself)
    
    uint32_t crc = calculate_crc32(&offset, sizeof(offset));
    crc = update_crc32(crc, &timestamp_ms, sizeof(timestamp_ms));
    
    uint8_t prio = static_cast<uint8_t>(priority);
    uint8_t comp = static_cast<uint8_t>(codec);
    crc = update_crc32(crc, &prio, sizeof(prio));
    crc = update_crc32(crc, &comp, sizeof(comp));
    
    crc = update_crc32(crc, key.data(), key.size());
    crc = update_crc32(crc, value.data(), value.size());
    
    checksum = crc;
}

bool Message::verify_checksum() const {
    // Save current checksum
    uint32_t saved_checksum = checksum;
    
    // Compute expected checksum (same algorithm as compute_checksum)
    uint32_t crc = calculate_crc32(&offset, sizeof(offset));
    crc = update_crc32(crc, &timestamp_ms, sizeof(timestamp_ms));
    
    uint8_t prio = static_cast<uint8_t>(priority);
    uint8_t comp = static_cast<uint8_t>(codec);
    crc = update_crc32(crc, &prio, sizeof(prio));
    crc = update_crc32(crc, &comp, sizeof(comp));
    
    crc = update_crc32(crc, key.data(), key.size());
    crc = update_crc32(crc, value.data(), value.size());
    
    return saved_checksum == crc;
}

std::vector<uint8_t> Message::serialize() const {
    /**
     * Wire format:
     * [8 bytes: offset]
     * [8 bytes: timestamp_ms]
     * [4 bytes: partition]
     * [4 bytes: checksum]
     * [1 byte: priority]
     * [1 byte: codec]
     * [4 bytes: key_size]
     * [N bytes: key]
     * [4 bytes: value_size]
     * [M bytes: value]
     * 
     * Total header: 34 bytes + key.size() + value.size()
     */
    
    const size_t header_size = 8 + 8 + 4 + 4 + 1 + 1 + 4 + 4; // 34 bytes
    const size_t total_size = header_size + key.size() + value.size();
    
    std::vector<uint8_t> buffer(total_size);
    uint8_t* ptr = buffer.data();
    
    // Write header
    write_le(ptr, offset);
    write_le(ptr, timestamp_ms);
    write_le(ptr, partition);
    write_le(ptr, checksum);
    write_le(ptr, static_cast<uint8_t>(priority));
    write_le(ptr, static_cast<uint8_t>(codec));
    
    // Write key
    write_le(ptr, static_cast<uint32_t>(key.size()));
    if (!key.empty()) {
        std::memcpy(ptr, key.data(), key.size());
        ptr += key.size();
    }
    
    // Write value
    write_le(ptr, static_cast<uint32_t>(value.size()));
    if (!value.empty()) {
        std::memcpy(ptr, value.data(), value.size());
        ptr += value.size();
    }
    
    return buffer;
}

std::shared_ptr<Message> Message::deserialize(const uint8_t* data, size_t size) {
    const size_t min_header_size = 8 + 8 + 4 + 4 + 1 + 1 + 4 + 4; // 34 bytes
    
    if (data == nullptr || size < min_header_size) {
        return nullptr;
    }
    
    const uint8_t* ptr = data;
    const uint8_t* end = data + size;
    
    auto msg = std::make_shared<Message>();
    
    // Read header
    msg->offset = read_le<uint64_t>(ptr);
    msg->timestamp_ms = read_le<uint64_t>(ptr);
    msg->partition = read_le<uint32_t>(ptr);
    msg->checksum = read_le<uint32_t>(ptr);
    msg->priority = static_cast<MessagePriority>(read_le<uint8_t>(ptr));
    msg->codec = static_cast<CompressionCodec>(read_le<uint8_t>(ptr));
    
    // Read key
    uint32_t key_size = read_le<uint32_t>(ptr);
    if (ptr + key_size > end) {
        return nullptr; // Buffer too small
    }
    if (key_size > 0) {
        msg->key.assign(reinterpret_cast<const char*>(ptr), key_size);
        ptr += key_size;
    }
    
    // Read value
    if (ptr + sizeof(uint32_t) > end) {
        return nullptr; // Buffer too small
    }
    uint32_t value_size = read_le<uint32_t>(ptr);
    if (ptr + value_size > end) {
        return nullptr; // Buffer too small
    }
    if (value_size > 0) {
        msg->value.assign(reinterpret_cast<const char*>(ptr), value_size);
        ptr += value_size;
    }
    
    return msg;
}

} // namespace stream
