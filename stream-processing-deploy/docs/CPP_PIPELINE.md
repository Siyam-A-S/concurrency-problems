# C++ Coinbase Inference Pipeline

Real-time market data inference using PatchTST model with OpenVINO optimization for AMD EPYC 7763.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     Real-Time Inference Pipeline (C++)                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────┐    ┌────────────────┐    ┌─────────────────────────────┐  │
│  │   Coinbase   │    │    Docker      │    │      C++ Pipeline           │  │
│  │  WebSocket   │───▶│ stream-server  │───▶│  (coinbase-inference)       │  │
│  │  (10-100Hz)  │    │    :9092       │    │                             │  │
│  └──────────────┘    └────────────────┘    │  ┌─────────────────────────┐│  │
│                                            │  │ MarketDataConsumer      ││  │
│                                            │  │ - TCP client            ││  │
│                                            │  │ - JSON tick parsing     ││  │
│                                            │  └──────────┬──────────────┘│  │
│                                            │             │               │  │
│                                            │             ▼               │  │
│                                            │  ┌─────────────────────────┐│  │
│                                            │  │ FeatureWindow           ││  │
│                                            │  │ - 512 timestep buffer   ││  │
│                                            │  │ - 7 feature channels    ││  │
│                                            │  │ - Thread-safe deque     ││  │
│                                            │  └──────────┬──────────────┘│  │
│                                            │             │               │  │
│                                            │             ▼               │  │
│                                            │  ┌─────────────────────────┐│  │
│                                            │  │ OpenVINOEngine          ││  │
│                                            │  │ - FP16 inference        ││  │
│                                            │  │ - AVX2 optimized        ││  │
│                                            │  │ - ~13ms latency         ││  │
│                                            │  └──────────┬──────────────┘│  │
│                                            │             │               │  │
│                                            │             ▼               │  │
│                                            │  ┌─────────────────────────┐│  │
│                                            │  │ Trading Signals         ││  │
│                                            │  │ 📈 BUY  |  ➡️ HOLD  |  📉│  │
│                                            │  └─────────────────────────┘│  │
│                                            └─────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 7-Channel Feature Vector

| Channel | Description | Formula |
|---------|-------------|---------|
| 0 | Normalized log price | `log(price / 45000.0)` |
| 1 | Bid-ask spread ratio | `(ask - bid) / price` |
| 2 | Log volume | `log1p(volume)` |
| 3 | Order book imbalance | `OBI ∈ [-1, 1]` |
| 4 | Price momentum | `(P_t - P_{t-5}) / P_{t-5} * 100` |
| 5 | Volatility estimate | `σ * 100` |
| 6 | Time cyclical | `sin(ms_of_day / day_ms * 2π)` |

## Usage

### Inside Docker Container

```bash
# Start stream-processor
docker-compose up -d stream-processor

# Run the C++ inference pipeline
docker exec -it stream-processor \
  /usr/local/bin/coinbase-inference \
  --host localhost \
  --port 9092 \
  --topic coinbase.BTC_USD \
  --model-dir /models/openvino \
  --interval 200
```

### Command Line Options

| Option | Default | Description |
|--------|---------|-------------|
| `--host` | localhost | Stream processor host |
| `--port` | 9092 | Stream processor port |
| `--topic` | coinbase.BTC_USD | Topic to consume |
| `--model-dir` | models/openvino | OpenVINO model directory |
| `--interval` | 500 | Inference interval (ms) |

## Files

| File | Description |
|------|-------------|
| [coinbase_pipeline.h](src/pipeline/coinbase_pipeline.h) | Main pipeline: FeatureWindow, OpenVINOEngine, MarketDataConsumer |
| [coinbase_inference_main.cpp](src/coinbase_inference_main.cpp) | CLI entry point with signal handling |
| [feature_types.h](src/pipeline/feature_types.h) | Model configuration constants |

## Building

```bash
# Rebuild Docker image with coinbase-inference
docker-compose build stream-processor

# Verify binary
docker exec stream-processor ls -la /usr/local/bin/coinbase-inference
```

## Performance Targets

| Metric | Target | AMD EPYC 7763 |
|--------|--------|---------------|
| Inference latency | < 15ms | ~13ms |
| Throughput | > 60 qps | ~77 qps |
| Window fill time | < 60s | 51.2s @ 10Hz |
| Memory | < 500MB | ~200MB |

## Environment Variables

```bash
# AMD EPYC 7763 optimization (set automatically)
export ONEDNN_MAX_CPU_ISA=AVX2
export OMP_NUM_THREADS=2
export KMP_AFFINITY=granularity=fine,compact,1,0
```

## Dependencies

- C++17 compiler (GCC 9+)
- OpenVINO Runtime (for full inference)
- libpthread
- Stream processor library (libstreamprocessor.a)

## Testing

```bash
# Syntax check only
g++ -std=c++17 -O0 -c -fsyntax-only \
  src/coinbase_inference_main.cpp \
  -I src -I src/pipeline -I ../stream-processing/include

# Full test with Python data pump
python3 scripts/test_cpp_pipeline.py
```
