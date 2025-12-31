cmake_minimum_required(VERSION 3.14)
project(StreamProcessor VERSION 1.0 LANGUAGES CXX)

# Set C++17 standard (required for std::shared_mutex, std::optional, etc.)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Compiler flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wpedantic")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -march=native -DNDEBUG")
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g3")

# Separate sanitizer configurations (cannot use both together!)
# Use: cmake -DCMAKE_BUILD_TYPE=Asan .. for AddressSanitizer
# Use: cmake -DCMAKE_BUILD_TYPE=Tsan .. for ThreadSanitizer
set(CMAKE_CXX_FLAGS_ASAN "-O1 -g -fsanitize=address -fno-omit-frame-pointer")
set(CMAKE_CXX_FLAGS_TSAN "-O1 -g -fsanitize=thread")

# Find required packages
find_package(Threads REQUIRED)

# Include directories
include_directories(${CMAKE_SOURCE_DIR}/include)

# =============================================================================
# MVP Source Files - Start simple, add as needed
# =============================================================================

# Core sources - minimal for MVP
set(CORE_SOURCES
    src/core/message.cpp
    src/core/config.cpp
)

# Add more source groups as you implement them:
# set(STORAGE_SOURCES
#     src/storage/log_segment.cpp
# )
# 
# set(NETWORK_SOURCES
#     src/network/server.cpp
# )

# =============================================================================
# Main Library
# =============================================================================

# Create main library (header-only for MVP until we have implementations)
add_library(streamprocessor INTERFACE)
target_include_directories(streamprocessor INTERFACE 
    ${CMAKE_SOURCE_DIR}/include
)
target_link_libraries(streamprocessor INTERFACE Threads::Threads)

# When you have source files, change to:
# add_library(streamprocessor STATIC ${CORE_SOURCES})
# target_link_libraries(streamprocessor Threads::Threads)

# =============================================================================
# Executables (uncomment as you implement)
# =============================================================================

# Main broker executable
# add_executable(broker src/main.cpp)
# target_link_libraries(broker streamprocessor)

# =============================================================================
# Tests
# =============================================================================

# Simple test executable
add_executable(test_basic tests/test_basic.cpp)
target_link_libraries(test_basic streamprocessor Threads::Threads)

# Performance test
# add_executable(perf_test tests/performance_test.cpp)
# target_link_libraries(perf_test streamprocessor)

# =============================================================================
# Install targets
# =============================================================================

install(DIRECTORY include/ DESTINATION include)

# =============================================================================
# Build Info
# =============================================================================

message(STATUS "")
message(STATUS "Stream Processor MVP Configuration:")
message(STATUS "  C++ Standard: ${CMAKE_CXX_STANDARD}")
message(STATUS "  Build Type: ${CMAKE_BUILD_TYPE}")
message(STATUS "")
message(STATUS "Build types available:")
message(STATUS "  Release - Optimized build")
message(STATUS "  Debug   - Debug symbols, no optimization")
message(STATUS "  Asan    - AddressSanitizer (memory errors)")
message(STATUS "  Tsan    - ThreadSanitizer (race conditions)")
message(STATUS "")
message(STATUS "Usage: cmake -DCMAKE_BUILD_TYPE=<type> ..")
message(STATUS "")
