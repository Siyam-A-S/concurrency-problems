# OpenVINO Model Conversion & Optimization Guide

## Granite-PatchTST for AMD EPYC 7763

This guide documents the OpenVINO conversion and optimization setup for the Granite-PatchTST time series forecasting model, targeting the AMD EPYC 7763 CPU (Zen 3 architecture).

---

## Quick Start

```bash
# 1. Run the conversion script
cd /workspaces/codespaces-blank/stream-processing-deploy
bash scripts/convert_to_openvino.sh

# 2. Test inference
python scripts/inference_openvino.py
```

---

## Model Specifications

| Parameter | Value | Description |
|-----------|-------|-------------|
| Architecture | PatchTSTForPrediction | Transformer-based time series model |
| Context Length | 512 | Historical timesteps (input window) |
| Prediction Length | 96 | Forecast horizon (output timesteps) |
| Input Channels | 7 | Number of input features |
| Patch Length | 12 | Subseries patch size |
| d_model | 128 | Hidden dimension |
| Attention Heads | 16 | Multi-head attention |
| Hidden Layers | 3 | Transformer encoder layers |

### Input Shape
```
[batch_size, context_length, num_channels] = [1, 512, 7]
```

### Output Shape
```
[batch_size, prediction_length, num_targets] = [1, 96, 1]
```

---

## Files Created

| File | Purpose |
|------|---------|
| `scripts/convert_to_openvino.sh` | Model conversion script |
| `scripts/inference_openvino.py` | Python inference with AMD optimizations |
| `scripts/inference_pipeline_integration.py` | C++ pipeline integration layer |
| `scripts/requirements-openvino.txt` | Python dependencies |
| `models/openvino/` | Output directory for IR files |
| `OPENVINO_SETUP.md` | This documentation |

---

## Conversion Command

The conversion script uses `optimum-cli` to export the HuggingFace model to OpenVINO IR format:

```bash
optimum-cli export openvino \
    --model /workspaces/codespaces-blank/ibm-granitegranite-timeseries-patchtst \
    --task feature-extraction \
    --fp16 \
    /workspaces/codespaces-blank/stream-processing-deploy/models/openvino
```

### Key Options:
- `--fp16`: Half-precision for reduced memory bandwidth
- `--task feature-extraction`: Export mode for encoder-style models

---

## AMD EPYC 7763 Optimizations

### CPU Specifications
- Architecture: Zen 3
- Visible Cores: 2
- Instruction Set: AVX2 (no AVX-512)
- L3 Cache: Shared

### Environment Variables

Set these **before** importing OpenVINO:

```bash
# Limit to AVX2 (Zen 3 doesn't support AVX-512)
export ONEDNN_MAX_CPU_ISA=AVX2

# Thread configuration
export OMP_NUM_THREADS=2
export OMP_WAIT_POLICY=ACTIVE

# Thread affinity (Intel OpenMP / oneDNN)
export KMP_AFFINITY="granularity=fine,compact,1,0"
export KMP_BLOCKTIME=1

# GCC OpenMP affinity
export GOMP_CPU_AFFINITY="0-1"
```

### OpenVINO Runtime Configuration

```python
config = {
    "PERFORMANCE_HINT": "LATENCY",      # Real-time inference
    "INFERENCE_NUM_THREADS": 2,          # Match visible cores
    "AFFINITY": "CORE",                  # Pin threads to cores
    "NUM_STREAMS": 1,                    # Single stream for latency
}
```

### Why These Settings?

1. **ONEDNN_MAX_CPU_ISA=AVX2**: AMD Zen 3 supports AVX2 but not AVX-512. Setting this prevents oneDNN from attempting AVX-512 operations.

2. **PERFORMANCE_HINT=LATENCY**: For real-time Coinbase data inference, we optimize for single-query latency rather than throughput.

3. **Thread Pinning**: Reduces thread migration and cache misses on the NUMA architecture.

4. **NUM_STREAMS=1**: Multiple streams are for throughput optimization; single stream minimizes latency.

---

## Integration with Stream Processing Pipeline

### Option 1: JSON Line Protocol (Recommended)

Run the inference server as a subprocess:

```bash
python scripts/inference_pipeline_integration.py --mode server
```

Input (stdin):
```json
{"request_id": "1", "timestamp_ms": 1705678900000, "past_values": [[0.1, 0.2, ...]]}
```

Output (stdout):
```json
{"request_id": "1", "timestamp_ms": 1705678900000, "predictions": [[...]], "inference_time_ms": 5.2, "success": true}
```

### Option 2: Direct Python Import

```python
from scripts.inference_openvino import PatchTSTOpenVINOInference

engine = PatchTSTOpenVINOInference("models/openvino")
predictions = engine.predict(past_values)
```

---

## Performance Benchmarks

Expected performance on AMD EPYC 7763 (2 cores):

| Metric | Expected Value |
|--------|----------------|
| Mean Latency | 5-15 ms |
| P95 Latency | 10-25 ms |
| Throughput | 50-150 QPS |

Run benchmarks:
```bash
python scripts/inference_openvino.py
```

---

## Troubleshooting

### Model Not Found
```
ERROR: OpenVINO model not found!
```
**Solution**: Run the conversion script first:
```bash
bash scripts/convert_to_openvino.sh
```

### AVX-512 Warnings
```
Warning: AVX-512 not available
```
**Solution**: Ensure `ONEDNN_MAX_CPU_ISA=AVX2` is set before importing OpenVINO.

### High Latency
**Solutions**:
1. Verify thread affinity is working: `taskset -c 0-1 python ...`
2. Check for CPU throttling: `cat /proc/cpuinfo | grep MHz`
3. Ensure no other processes competing for cores

### Out of Memory
**Solutions**:
1. Reduce batch size to 1
2. Verify FP16 conversion completed
3. Check available memory: `free -h`

---

## Coinbase Market Data Mapping

Map Coinbase WebSocket data to model input channels:

| Channel | Data Source | Normalization |
|---------|-------------|---------------|
| 0 | Price (close) | StandardScaler |
| 1 | Price (high) | StandardScaler |
| 2 | Price (low) | StandardScaler |
| 3 | Volume | Log + StandardScaler |
| 4 | RSI | MinMax [0, 1] |
| 5 | MACD | StandardScaler |
| 6 | Volatility | StandardScaler |

**Note**: The original model was trained on ETTh1 data (electrical transformer). For best results with Coinbase data, consider fine-tuning the model.

---

## Next Steps

1. **Run conversion**: `bash scripts/convert_to_openvino.sh`
2. **Validate inference**: `python scripts/inference_openvino.py`
3. **Integrate with pipeline**: Update C++ `inference_engine.h` to call Python subprocess
4. **Fine-tune for Coinbase data**: Collect labeled data and retrain
