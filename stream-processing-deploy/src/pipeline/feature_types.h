#pragma once

/**
 * Feature Types and Model Configuration for PatchTST Inference
 * 
 * These constants match the model configuration in:
 * models/ibm-granitegranite-timeseries-patchtst/config.json
 */

#include <cstddef>
#include <cstdint>

namespace pipeline {

// =============================================================================
// PatchTST Model Configuration
// =============================================================================

// Input dimensions
constexpr size_t PATCHTST_CONTEXT_LENGTH = 512;   // Historical timesteps
constexpr size_t PATCHTST_NUM_CHANNELS = 7;       // Feature channels
constexpr size_t PATCHTST_BATCH_SIZE = 1;         // Single sample inference

// Output dimensions  
constexpr size_t PATCHTST_PREDICTION_LENGTH = 96; // Forecast horizon
constexpr size_t PATCHTST_NUM_TARGETS = 1;        // Single target output

// Model architecture
constexpr size_t PATCHTST_PATCH_LENGTH = 12;      // Patch size
constexpr size_t PATCHTST_D_MODEL = 128;          // Hidden dimension
constexpr size_t PATCHTST_NUM_HEADS = 16;         // Attention heads
constexpr size_t PATCHTST_NUM_LAYERS = 3;         // Encoder layers

// =============================================================================
// Feature Channel Indices
// =============================================================================

enum class FeatureChannel : uint8_t {
    NORMALIZED_LOG_PRICE = 0,   // log(price / 45000.0)
    BID_ASK_SPREAD_RATIO = 1,   // (ask - bid) / price
    LOG_VOLUME = 2,             // log1p(volume)
    ORDER_BOOK_IMBALANCE = 3,   // OBI ∈ [-1, 1]
    PRICE_MOMENTUM = 4,         // (P_t - P_{t-5}) / P_{t-5} * 100
    VOLATILITY_ESTIMATE = 5,    // σ * 100
    TIME_CYCLICAL = 6           // sin(ms_of_day / day_ms * 2π)
};

// =============================================================================
// Trading Signals
// =============================================================================

enum class TradingSignal {
    BUY,
    HOLD,
    SELL
};

inline const char* signal_to_string(TradingSignal signal) {
    switch (signal) {
        case TradingSignal::BUY:  return "📈 BUY";
        case TradingSignal::HOLD: return "➡️  HOLD";
        case TradingSignal::SELL: return "📉 SELL";
        default: return "UNKNOWN";
    }
}

// =============================================================================
// Inference Result
// =============================================================================

struct InferenceResult {
    uint64_t prediction_num{0};      // Prediction counter
    double current_price{0.0};        // Current BTC price
    double forecast_price{0.0};       // Predicted price
    double change_pct{0.0};           // Percentage change
    TradingSignal signal{TradingSignal::HOLD};
    double latency_ms{0.0};           // Inference latency
    uint64_t timestamp_ms{0};         // Prediction timestamp
};

// =============================================================================
// Market Tick Data
// =============================================================================

struct MarketTick {
    double price{0.0};
    double bid{0.0};
    double ask{0.0};
    double volume{0.0};
    uint64_t timestamp_ms{0};
    
    bool is_valid() const {
        return price > 0.0 && bid > 0.0 && ask > 0.0;
    }
};

} // namespace pipeline
