#!/usr/bin/env python3
"""
Test Data Pump for Coinbase Inference Pipeline

Generates synthetic BTC market data and publishes to stream-processor
at configurable rates to test the C++ inference pipeline.
"""

import socket
import struct
import json
import time
import random
import math
import argparse
from datetime import datetime

# Protocol constants (match stream-processing/include/network/protocol.h)
MAGIC_NUMBER = 0x53545250  # "STRP"
PROTOCOL_VERSION = 1
HEADER_SIZE = 16

class RequestType:
    API_VERSIONS = 0
    METADATA = 1
    CREATE_TOPIC = 10
    DELETE_TOPIC = 11
    LIST_TOPICS = 12
    PRODUCE = 20
    FETCH = 21
    JOIN_GROUP = 30
    LEAVE_GROUP = 31
    COMMIT_OFFSET = 32
    FETCH_OFFSET = 33

def encode_string(s: str) -> bytes:
    """Encode string as length-prefixed bytes"""
    data = s.encode('utf-8')
    return struct.pack('<I', len(data)) + data

def decode_string(data: bytes, offset: int) -> tuple:
    """Decode length-prefixed string, return (string, new_offset)"""
    length = struct.unpack('<I', data[offset:offset+4])[0]
    s = data[offset+4:offset+4+length].decode('utf-8')
    return s, offset + 4 + length

class StreamClient:
    def __init__(self, host='localhost', port=9092):
        self.host = host
        self.port = port
        self.sock = None
    
    def connect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(5.0)
        self.sock.connect((self.host, self.port))
        print(f"[Client] Connected to {self.host}:{self.port}")
    
    def disconnect(self):
        if self.sock:
            self.sock.close()
            self.sock = None
    
    def send_request(self, req_type: int, payload: bytes) -> bytes:
        """Send request and receive response"""
        # Build header
        header = struct.pack('<IIII', 
            MAGIC_NUMBER,
            PROTOCOL_VERSION,
            req_type,
            len(payload)
        )
        
        # Send
        self.sock.sendall(header + payload)
        
        # Receive response header
        resp_header = self._recv_exact(HEADER_SIZE)
        magic, version, resp_type, length = struct.unpack('<IIII', resp_header)
        
        if magic != MAGIC_NUMBER:
            raise ValueError(f"Invalid magic: {magic:08x}")
        
        # Receive payload
        resp_payload = self._recv_exact(length)
        return resp_payload
    
    def _recv_exact(self, n: int) -> bytes:
        """Receive exactly n bytes"""
        data = b''
        while len(data) < n:
            chunk = self.sock.recv(n - len(data))
            if not chunk:
                raise ConnectionError("Connection closed")
            data += chunk
        return data
    
    def create_topic(self, name: str, partitions: int = 1):
        """Create a topic"""
        payload = encode_string(name) + struct.pack('<I', partitions)
        resp = self.send_request(RequestType.CREATE_TOPIC, payload)
        error_code = struct.unpack('<h', resp[:2])[0]
        if error_code == 0:
            print(f"[Client] Created topic: {name}")
        elif error_code == -6:  # TOPIC_ALREADY_EXISTS
            print(f"[Client] Topic already exists: {name}")
        else:
            print(f"[Client] Create topic error: {error_code}")
        return error_code == 0 or error_code == -6
    
    def produce(self, topic: str, key: str, value: str, priority: int = 2):
        """Produce a message to a topic"""
        payload = (
            encode_string(topic) +
            encode_string(key) +
            encode_string(value) +
            struct.pack('<B', priority)
        )
        resp = self.send_request(RequestType.PRODUCE, payload)
        error_code = struct.unpack('<h', resp[:2])[0]
        return error_code == 0


class MarketDataGenerator:
    """Generates realistic BTC market data"""
    
    def __init__(self, initial_price=97000.0):
        self.price = initial_price
        self.volatility = 0.0001  # Per-tick volatility
        self.trend = 0.0
        self.tick_count = 0
    
    def next_tick(self) -> dict:
        """Generate next market tick"""
        self.tick_count += 1
        
        # Random walk with momentum
        self.trend = 0.98 * self.trend + 0.02 * random.gauss(0, 1)
        change = self.volatility * self.price * (random.gauss(0, 1) + 0.1 * self.trend)
        self.price += change
        
        # Keep price in reasonable range
        self.price = max(50000, min(150000, self.price))
        
        # Generate realistic spread
        spread_bps = random.uniform(1, 5)  # 1-5 basis points
        spread = self.price * spread_bps / 10000
        
        bid = self.price - spread / 2
        ask = self.price + spread / 2
        
        # Volume follows power law
        volume = random.paretovariate(1.5) * 0.01
        
        return {
            "type": "ticker",
            "product_id": "BTC-USD",
            "price": f"{self.price:.2f}",
            "best_bid": f"{bid:.2f}",
            "best_ask": f"{ask:.2f}",
            "last_size": f"{volume:.8f}",
            "time": datetime.utcnow().isoformat() + "Z",
            "trade_id": self.tick_count
        }


def main():
    parser = argparse.ArgumentParser(description='Stream Processor Test Data Pump')
    parser.add_argument('--host', default='localhost', help='Server host')
    parser.add_argument('--port', type=int, default=9092, help='Server port')
    parser.add_argument('--topic', default='coinbase.BTC_USD', help='Topic name')
    parser.add_argument('--rate', type=int, default=10, help='Ticks per second')
    parser.add_argument('--count', type=int, default=1000, help='Number of ticks (0=infinite)')
    parser.add_argument('--initial-price', type=float, default=97000.0, help='Initial BTC price')
    args = parser.parse_args()
    
    print(f"""
╔════════════════════════════════════════════════════════════╗
║           Test Data Pump for Inference Pipeline            ║
╠════════════════════════════════════════════════════════════╣
║  Host:    {args.host:10}  Port:  {args.port:<6}                    ║
║  Topic:   {args.topic:20}                         ║
║  Rate:    {args.rate:3} ticks/sec  Count: {args.count if args.count else 'infinite':<10}        ║
╚════════════════════════════════════════════════════════════╝
""")
    
    client = StreamClient(args.host, args.port)
    client.connect()
    
    # Create topic
    if not client.create_topic(args.topic):
        print("[ERROR] Failed to create topic")
        return
    
    generator = MarketDataGenerator(args.initial_price)
    interval = 1.0 / args.rate
    
    print(f"[Pump] Starting data generation at {args.rate} Hz...")
    
    sent = 0
    start_time = time.time()
    
    try:
        while args.count == 0 or sent < args.count:
            tick = generator.next_tick()
            tick_json = json.dumps(tick)
            
            success = client.produce(args.topic, "BTC-USD", tick_json)
            if success:
                sent += 1
                if sent % 100 == 0:
                    elapsed = time.time() - start_time
                    actual_rate = sent / elapsed if elapsed > 0 else 0
                    print(f"[Pump] Sent {sent} ticks | "
                          f"Price: ${float(tick['price']):,.2f} | "
                          f"Rate: {actual_rate:.1f}/s")
            
            time.sleep(interval)
            
    except KeyboardInterrupt:
        print("\n[Pump] Interrupted")
    finally:
        elapsed = time.time() - start_time
        print(f"\n[Pump] Summary:")
        print(f"  Total ticks: {sent}")
        print(f"  Duration: {elapsed:.1f}s")
        print(f"  Avg rate: {sent/elapsed:.1f} ticks/sec" if elapsed > 0 else "")
        client.disconnect()


if __name__ == '__main__':
    main()
