# Multi-stage build for stream processor
FROM ubuntu:24.04 AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy source code
WORKDIR /build
COPY stream-processing/ ./stream-processing/
COPY stream-processing-deploy/src/ ./server-src/

# Clean existing build artifacts and rebuild
WORKDIR /build/stream-processing
RUN rm -rf build && mkdir build
WORKDIR /build/stream-processing/build
RUN cmake .. -DCMAKE_BUILD_TYPE=Release && \
    make -j$(nproc)

# Build the server binary linking against the library
WORKDIR /build
RUN g++ -std=c++17 -O3 -o stream-server \
    server-src/main.cpp \
    -I stream-processing/include \
    stream-processing/build/libstreamprocessor.a \
    -lpthread

# Build the Coinbase bridge
RUN g++ -std=c++17 -O3 -o coinbase-bridge \
    server-src/coinbase_bridge.cpp \
    server-src/coinbase_connector.cpp \
    -I stream-processing/include \
    -I server-src \
    stream-processing/build/libstreamprocessor.a \
    -lpthread -lssl -lcrypto

# Build the Coinbase WebSocket bridge (no auth required!)
RUN g++ -std=c++17 -O3 -o coinbase-ws-bridge \
    server-src/coinbase_ws_bridge.cpp \
    -I stream-processing/include \
    -I server-src \
    stream-processing/build/libstreamprocessor.a \
    -lpthread -lssl -lcrypto

# Build the ML inference pipeline
RUN g++ -std=c++17 -O3 -o ml-pipeline \
    server-src/pipeline_main.cpp \
    -I stream-processing/include \
    -I server-src \
    stream-processing/build/libstreamprocessor.a \
    -lpthread -lssl -lcrypto

# Runtime image
FROM ubuntu:24.04 AS runtime

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    netcat-openbsd \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -s /bin/bash streamuser

# Create data directory
RUN mkdir -p /data && chown streamuser:streamuser /data

# Copy binaries from builder
COPY --from=builder /build/stream-server /usr/local/bin/stream-server
COPY --from=builder /build/coinbase-bridge /usr/local/bin/coinbase-bridge
COPY --from=builder /build/coinbase-ws-bridge /usr/local/bin/coinbase-ws-bridge
COPY --from=builder /build/ml-pipeline /usr/local/bin/ml-pipeline
RUN chmod +x /usr/local/bin/stream-server /usr/local/bin/coinbase-bridge /usr/local/bin/coinbase-ws-bridge /usr/local/bin/ml-pipeline

# Switch to non-root user
USER streamuser
WORKDIR /home/streamuser

# Expose the stream processor port
EXPOSE 9092

# Data volume for persistence
VOLUME ["/data"]

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD nc -zw1 localhost 9092 || exit 1

# Run the server
ENTRYPOINT ["/usr/local/bin/stream-server"]
CMD ["--data-dir=/data", "--port=9092", "--bind=0.0.0.0"]
