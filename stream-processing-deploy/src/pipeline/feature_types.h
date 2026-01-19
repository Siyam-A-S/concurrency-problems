/**
 * Feature Types and Data Structures
 * 
 * Shared types for the ML inference pipeline
 */

#pragma once

#include <string>
#include <vector>
#include <chrono>
#include <cstdint>
#include <cmath>
#include <deque>
#include <mutex>
#include <atomic>

namespace pipeline {

// ============================================================================
// Market Data Types
// ============================================================================

struct OHLCV {
    double open{0.0};
    double high{0.0};
    double low{0.0};
    double close{0.0};
    double volume{0.0};
    int64_t timestamp_ms{0};
    
    bool is_valid() const {
        return open > 0 && high > 0 && low > 0 && close > 0;
    }
};

struct Tick {
    double price{0.0};
    double size{0.0};
    std::string side;  // "buy" or "sell"
    int64_t timestamp_ms{0};
};

struct OrderBookLevel {
    double price{0.0};
    double size{0.0};
};

struct OrderBookSnapshot {
    std::vector<OrderBookLevel> bids;  // Sorted descending by price
    std::vector<OrderBookLevel> asks;  // Sorted ascending by price
    int64_t timestamp_ms{0};
    
    double mid_price() const {
        if (bids.empty() || asks.empty()) return 0.0;
        return (bids[0].price + asks[0].price) / 2.0;
    }
    
    double spread() const {
        if (bids.empty() || asks.empty()) return 0.0;
        return asks[0].price - bids[0].price;
    }
    
    double spread_bps() const {
        double mid = mid_price();
        if (mid <= 0) return 0.0;
        return (spread() / mid) * 10000.0;
    }
};

// ============================================================================
// Technical Indicators
// ============================================================================

struct TechnicalIndicators {
    // Moving Averages
    double sma_5{0.0};
    double sma_20{0.0};
    double ema_12{0.0};
    double ema_26{0.0};
    
    // RSI
    double rsi_14{50.0};
    
    // MACD
    double macd_line{0.0};
    double macd_signal{0.0};
    double macd_histogram{0.0};
    
    // Bollinger Bands
    double bb_upper{0.0};
    double bb_middle{0.0};
    double bb_lower{0.0};
    double bb_width{0.0};
    double bb_percent{0.5};
    
    // Volatility
    double atr_14{0.0};
    double volatility_1m{0.0};
    
    // Volume
    double volume_sma{0.0};
    double volume_ratio{1.0};
    
    // Order Book Imbalance
    double obi{0.0};           // Order Book Imbalance [-1, 1]
    double obi_depth_5{0.0};   // OBI at 5 levels
    double obi_weighted{0.0};  // Volume-weighted OBI
    
    // Trade Flow Imbalance
    double tfi{0.0};           // Trade Flow Imbalance [-1, 1]
    double buy_pressure{0.0};
    double sell_pressure{0.0};
    
    int64_t timestamp_ms{0};
};

// ============================================================================
// Feature Vector for Model Input
// ============================================================================

struct FeatureVector {
    static constexpr size_t PATCH_SIZE = 64;      // PatchTST patch size
    static constexpr size_t NUM_FEATURES = 32;    // Features per timestep
    
    std::string symbol;
    int64_t timestamp_ms{0};
    
    // Price series (normalized returns)
    std::vector<double> price_returns;    // Last PATCH_SIZE returns
    
    // Volume series
    std::vector<double> volume_normalized;
    
    // Technical indicators time series
    std::vector<double> rsi_series;
    std::vector<double> macd_series;
    std::vector<double> bb_percent_series;
    
    // Order flow features
    std::vector<double> obi_series;
    std::vector<double> tfi_series;
    
    // Current snapshot features
    TechnicalIndicators current_indicators;
    
    // Flatten to model input format
    std::vector<float> to_tensor() const {
        std::vector<float> tensor;
        tensor.reserve(PATCH_SIZE * NUM_FEATURES);
        
        for (size_t i = 0; i < PATCH_SIZE; ++i) {
            // Price features
            tensor.push_back(static_cast<float>(
                i < price_returns.size() ? price_returns[i] : 0.0));
            
            // Volume features
            tensor.push_back(static_cast<float>(
                i < volume_normalized.size() ? volume_normalized[i] : 0.0));
            
            // Technical indicators
            tensor.push_back(static_cast<float>(
                i < rsi_series.size() ? (rsi_series[i] - 50.0) / 50.0 : 0.0));
            tensor.push_back(static_cast<float>(
                i < macd_series.size() ? macd_series[i] : 0.0));
            tensor.push_back(static_cast<float>(
                i < bb_percent_series.size() ? bb_percent_series[i] - 0.5 : 0.0));
            
            // Order flow
            tensor.push_back(static_cast<float>(
                i < obi_series.size() ? obi_series[i] : 0.0));
            tensor.push_back(static_cast<float>(
                i < tfi_series.size() ? tfi_series[i] : 0.0));
            
            // Pad remaining features
            for (size_t j = 7; j < NUM_FEATURES; ++j) {
                tensor.push_back(0.0f);
            }
        }
        
        return tensor;
    }
    
    bool is_ready() const {
        return price_returns.size() >= PATCH_SIZE;
    }
};

// ============================================================================
// Prediction Output
// ============================================================================

struct Prediction {
    std::string symbol;
    int64_t timestamp_ms{0};
    
    // Price predictions
    double predicted_return_1s{0.0};   // 1 second ahead
    double predicted_return_5s{0.0};   // 5 seconds ahead
    double predicted_return_30s{0.0};  // 30 seconds ahead
    
    // Direction probability
    double prob_up{0.5};
    double prob_down{0.5};
    
    // Confidence
    double confidence{0.0};
    
    // Model metadata
    std::string model_version;
    double inference_latency_ms{0.0};
    
    // Trading signal
    int signal() const {
        if (confidence < 0.6) return 0;  // No signal
        if (prob_up > 0.65) return 1;    // Buy
        if (prob_down > 0.65) return -1; // Sell
        return 0;
    }
    
    std::string signal_str() const {
        int sig = signal();
        if (sig > 0) return "BUY";
        if (sig < 0) return "SELL";
        return "HOLD";
    }
};

// ============================================================================
// Trading Types
// ============================================================================

enum class OrderSide { BUY, SELL };
enum class OrderType { MARKET, LIMIT, STOP_LIMIT };
enum class OrderStatus { PENDING, OPEN, FILLED, CANCELLED, REJECTED };

struct Order {
    std::string order_id;
    std::string symbol;
    OrderSide side;
    OrderType type;
    double price{0.0};
    double size{0.0};
    double filled_size{0.0};
    double filled_price{0.0};
    OrderStatus status{OrderStatus::PENDING};
    int64_t created_at_ms{0};
    int64_t updated_at_ms{0};
};

struct Position {
    std::string symbol;
    double size{0.0};          // Positive = long, negative = short
    double avg_entry_price{0.0};
    double unrealized_pnl{0.0};
    double realized_pnl{0.0};
    int64_t opened_at_ms{0};
};

struct RiskLimits {
    double max_position_size{0.01};      // Max BTC position
    double max_order_size{0.001};        // Max single order size
    double max_daily_loss{100.0};        // Max daily loss in USD
    double max_drawdown_pct{5.0};        // Max drawdown percentage
    int max_open_orders{3};              // Max concurrent orders
    double min_order_interval_ms{1000};  // Min time between orders
};

struct AccountState {
    double balance_usd{0.0};
    double balance_btc{0.0};
    double equity{0.0};
    double daily_pnl{0.0};
    double peak_equity{0.0};
    double drawdown_pct{0.0};
    std::vector<Position> positions;
    std::vector<Order> open_orders;
    int64_t last_order_time_ms{0};
};

// ============================================================================
// Utility Functions
// ============================================================================

inline int64_t now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

inline double clamp(double value, double min_val, double max_val) {
    return std::max(min_val, std::min(max_val, value));
}

inline double normalize(double value, double mean, double std) {
    if (std <= 0) return 0.0;
    return (value - mean) / std;
}

} // namespace pipeline
