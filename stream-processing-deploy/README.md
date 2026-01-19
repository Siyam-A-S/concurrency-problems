# Stream Processor Docker Deployment

Deploy the high-performance stream processor server using Docker, with optional Coinbase FIX API integration for real-time cryptocurrency market data.

## Quick Start

### Build and Run

```bash
# Build the Docker image
docker-compose build

# Start the stream processor server
docker-compose up -d

# View logs
docker-compose logs -f

# Stop the server
docker-compose down
```

### Connect from External Clients

The server exposes port **9092** for TCP connections.

```
Host: <your-server-ip>
Port: 9092
Protocol: Binary (see protocol.h)
```

---

## Coinbase FIX API Integration

Connect to Coinbase Exchange to stream real-time market data into the stream processor.

### Coinbase FIX Endpoints

| Environment | URL |
|-------------|-----|
| **Production** | `tcp+ssl://fix-ord.exchange.coinbase.com:6121` |
| **Sandbox** | `tcp+ssl://fix-ord.sandbox.exchange.coinbase.com:6121` |

### Setup Coinbase Credentials

1. **Get API credentials** from Coinbase:
   - Production: https://exchange.coinbase.com/profile/api
   - Sandbox: https://public.sandbox.exchange.coinbase.com/profile/api

2. **Copy the environment template:**
   ```bash
   cp .env.example .env
   ```

3. **Edit `.env` with your credentials:**
   ```bash
   COINBASE_API_KEY=your_api_key
   COINBASE_API_SECRET=your_base64_secret
   COINBASE_PASSPHRASE=your_passphrase
   COINBASE_SENDER_COMP_ID=your_sender_comp_id
   COINBASE_USE_SANDBOX=true  # Use false for production
   ```

4. **Start with Coinbase bridge:**
   ```bash
   docker-compose --profile coinbase up -d
   ```

### Market Data Topics

When connected, the bridge creates these topics in the stream processor:

| Topic | Content |
|-------|---------|
| `coinbase.BTC_USD` | BTC-USD order book and trades |
| `coinbase.ETH_USD` | ETH-USD order book and trades |
| `coinbase.executions` | Order execution reports |

### Message Format

**Market Data:**
```
KEY: order_id
VALUE: TYPE|PRICE|SIZE|TIMESTAMP
Example: BID|45230.50|1.5|1736845200000
```

**Execution Reports:**
```
KEY: client_order_id
VALUE: ORDER_ID|SYMBOL|SIDE|STATUS|PRICE|QTY|FILLED|AVG_PRICE
Example: abc123|BTC-USD|BUY|2|45230.50|1.0|1.0|45230.50
```

---

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `STREAM_PORT` | 9092 | Server listen port |
| `STREAM_BIND` | 0.0.0.0 | Bind address |
| `STREAM_IO_THREADS` | 4 | Number of I/O threads |
| `STREAM_WORKER_THREADS` | 8 | Number of worker threads |
| `STREAM_MAX_CONNECTIONS` | 10000 | Maximum concurrent connections |
| `STREAM_DATA_DIR` | /data | Data persistence directory |

### Command Line Arguments

```bash
./stream-server \
    --data-dir=/data \
    --port=9092 \
    --bind=0.0.0.0 \
    --io-threads=4 \
    --max-connections=10000
```

## API Reference

### Wire Protocol

All requests use a binary protocol with the following header:

```
+--------+----------+--------+----------+
| Magic  | Version  | Type   | Length   |
| 4 bytes| 4 bytes  | 4 bytes| 4 bytes  |
+--------+----------+--------+----------+
|              Payload                  |
|          (Length bytes)               |
+---------------------------------------+
```

### Request Types

| Type | ID | Description |
|------|-----|-------------|
| `METADATA` | 1 | Get broker metadata and topic info |
| `CREATE_TOPIC` | 10 | Create a new topic |
| `DELETE_TOPIC` | 11 | Delete an existing topic |
| `LIST_TOPICS` | 12 | List all available topics |
| `PRODUCE` | 20 | Send messages to a topic |
| `FETCH` | 21 | Consume messages from a topic |
| `JOIN_GROUP` | 30 | Join a consumer group |
| `LEAVE_GROUP` | 31 | Leave a consumer group |
| `COMMIT_OFFSET` | 32 | Commit consumer offset |
| `FETCH_OFFSET` | 33 | Get committed offset |
| `HEARTBEAT` | 40 | Keep connection alive |

## Usage Examples

### Python Client Example

```python
import socket
import struct

class StreamClient:
    MAGIC = 0x53545250  # "STRP"
    VERSION = 1
    
    def __init__(self, host='localhost', port=9092):
        self.host = host
        self.port = port
        self.sock = None
    
    def connect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.host, self.port))
    
    def send_request(self, req_type, payload):
        header = struct.pack('<IIII', self.MAGIC, self.VERSION, req_type, len(payload))
        self.sock.sendall(header + payload)
        
        # Read response header
        resp_header = self.sock.recv(16)
        _, _, _, length = struct.unpack('<IIII', resp_header)
        
        # Read response payload
        return self.sock.recv(length)
    
    def list_topics(self):
        return self.send_request(12, b'')  # LIST_TOPICS = 12
    
    def close(self):
        if self.sock:
            self.sock.close()

# Usage
client = StreamClient('your-server-ip', 9092)
client.connect()
topics = client.list_topics()
print(topics)
client.close()
```

### C++ Client Example

Use the provided `StreamClient` class from the library:

```cpp
#include "network/client.h"

using namespace stream::network;

int main() {
    ClientConfig config;
    config.host = "your-server-ip";
    config.port = 9092;
    
    StreamClient client(config);
    client.connect();
    
    // Create a topic
    client.create_topic("my-events", 4);
    
    // Produce messages
    client.produce("my-events", "key1", "Hello, World!");
    
    // Consume messages
    auto response = client.fetch("my-events", 0, 0, 100);
    
    return 0;
}
```

## Cloud Deployment

### AWS ECS

```json
{
  "containerDefinitions": [{
    "name": "stream-processor",
    "image": "your-registry/stream-processor:latest",
    "portMappings": [{
      "containerPort": 9092,
      "hostPort": 9092,
      "protocol": "tcp"
    }],
    "environment": [
      {"name": "STREAM_PORT", "value": "9092"},
      {"name": "STREAM_IO_THREADS", "value": "4"}
    ],
    "mountPoints": [{
      "sourceVolume": "stream-data",
      "containerPath": "/data"
    }]
  }]
}
```

### Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: stream-processor
spec:
  replicas: 1
  selector:
    matchLabels:
      app: stream-processor
  template:
    metadata:
      labels:
        app: stream-processor
    spec:
      containers:
      - name: stream-processor
        image: your-registry/stream-processor:latest
        ports:
        - containerPort: 9092
        env:
        - name: STREAM_PORT
          value: "9092"
        volumeMounts:
        - name: data
          mountPath: /data
      volumes:
      - name: data
        persistentVolumeClaim:
          claimName: stream-data-pvc
---
apiVersion: v1
kind: Service
metadata:
  name: stream-processor
spec:
  type: LoadBalancer
  ports:
  - port: 9092
    targetPort: 9092
  selector:
    app: stream-processor
```

### Google Cloud Run

```bash
gcloud run deploy stream-processor \
    --image your-registry/stream-processor:latest \
    --port 9092 \
    --allow-unauthenticated \
    --cpu 4 \
    --memory 4Gi
```

## Security Considerations

⚠️ **Important**: This deployment exposes the server to the internet. Consider:

1. **Firewall Rules**: Restrict access to known IPs
2. **VPN/Private Network**: Deploy within a VPC
3. **TLS**: Add TLS termination via a reverse proxy
4. **Authentication**: Implement client authentication

### Adding TLS with Nginx

```nginx
stream {
    upstream stream_processor {
        server 127.0.0.1:9092;
    }
    
    server {
        listen 9093 ssl;
        ssl_certificate /etc/ssl/certs/server.crt;
        ssl_certificate_key /etc/ssl/private/server.key;
        
        proxy_pass stream_processor;
    }
}
```

## Performance Tuning

| Workload | IO Threads | Memory | Notes |
|----------|------------|--------|-------|
| Light (<1K msg/s) | 2 | 512MB | Development |
| Medium (1K-10K msg/s) | 4 | 2GB | Small production |
| Heavy (10K-100K msg/s) | 8 | 4GB | Production |
| Extreme (>100K msg/s) | 16 | 8GB+ | High-throughput |

## Troubleshooting

### Server won't start
```bash
# Check if port is in use
netstat -tlnp | grep 9092

# Check Docker logs
docker-compose logs stream-processor
```

### Connection refused
```bash
# Verify server is running
docker-compose ps

# Check firewall
sudo ufw status
```

### High latency
- Increase `STREAM_IO_THREADS`
- Check network connectivity
- Monitor CPU usage
