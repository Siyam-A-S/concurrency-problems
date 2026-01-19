/**
 * Feature Engineer - Stateful Feature Computation
 * 
 * Consumes raw market data and produces feature vectors for ML inference.
 * Maintains sliding windows and computes technical indicators in real-time.
 */

#pragma once

#include "feature_types.h"
#include "sliding_window.h"
#include "network/client.h"

#include <iostream>
#include <thread>
#include <atomic>
#include <unordered_map>
#include <functional>
#include <chrono>

namespace pipeline {

using namespace stream::network;

// Callback for when features are ready
using FeatureCallback = std::function<void(const FeatureVector&)>;
using IndicatorCallback = std::function<void(const std::string&, const TechnicalIndicators&)>;

class FeatureEngineer {
public:
    struct Config {
        // Stream processor connection
        std::string stream_host{"localhost"};
        uint16_t stream_port{9092};
        
        // Window sizes
        size_t price_window_size{120};      // ~2 minutes of data
        size_t trade_window_size{200};
        size_t indicator_history_size{64};  // For time series features
        
        // Feature computation
        size_t feature_patch_size{64};      // PatchTST input size
        int feature_emit_interval_ms{100};  // Emit features every 100ms
        
        // Symbols to process
        std::vector<std::string> symbols{"BTC-USD", "ETH-USD"};
    };
    
    explicit FeatureEngineer(const Config& config) : config_(config) {}
    
    ~FeatureEngineer() {
        stop();
    }
    
    void set_feature_callback(FeatureCallback cb) {
        feature_callback_ = std::move(cb);
    }
    
    void set_indicator_callback(IndicatorCallback cb) {
        indicator_callback_ = std::move(cb);
    }
    
    bool start() {
        if (running_) return true;
        
        // Initialize windows for each symbol
        for (const auto& symbol : config_.symbols) {
            price_windows_[symbol] = std::make_unique<PriceWindow>(config_.price_window_size);
            trade_flows_[symbol] = std::make_unique<TradeFlowWindow>(config_.trade_window_size);
            order_books_[symbol] = std::make_unique<OrderBookState>();
            indicator_history_[symbol] = std::make_unique<IndicatorHistory>(config_.indicator_history_size);
        }
        
        // Connect to stream processor
        ClientConfig client_config;
        client_config.host = config_.stream_host;
        client_config.port = config_.stream_port;
        
        client_ = std::make_unique<StreamClient>(client_config);
        if (!client_->connect()) {
            std::cerr << "[FEATURE] Failed to connect to stream processor\n";
            return false;
        }
        
        running_ = true;
        
        // Start consumer threads for each symbol
        for (const auto& symbol : config_.symbols) {
            consumer_threads_.emplace_back(&FeatureEngineer::consume_symbol, this, symbol);
        }
        
        // Start feature emission thread
        emit_thread_ = std::thread(&FeatureEngineer::emit_loop, this);
        
        std::cout << "[FEATURE] Feature engineer started\n";
        return true;
    }
    
    void stop() {
        running_ = false;
        
        for (auto& t : consumer_threads_) {
            if (t.joinable()) t.join();
        }
        consumer_threads_.clear();
        
        if (emit_thread_.joinable()) {
            emit_thread_.join();
        }
        
        if (client_) {
            client_->disconnect();
        }
        
        std::cout << "[FEATURE] Feature engineer stopped\n";
    }
    
    // Get current indicators for a symbol
    TechnicalIndicators get_indicators(const std::string& symbol) const {
        TechnicalIndicators indicators;
        
        auto pw_it = price_windows_.find(symbol);
        auto tf_it = trade_flows_.find(symbol);
        auto ob_it = order_books_.find(symbol);
        
        if (pw_it == price_windows_.end()) return indicators;
        
        const auto& pw = *pw_it->second;
        
        // Moving averages
        indicators.sma_5 = pw.sma(5);
        indicators.sma_20 = pw.sma(20);
        indicators.ema_12 = pw.ema(12);
        indicators.ema_26 = pw.ema(26);
        
        // RSI
        indicators.rsi_14 = pw.rsi(14);
        
        // MACD
        auto macd = pw.macd();
        indicators.macd_line = macd.line;
        indicators.macd_signal = macd.signal;
        indicators.macd_histogram = macd.histogram;
        
        // Bollinger Bands
        auto bb = pw.bollinger(20, 2.0);
        indicators.bb_upper = bb.upper;
        indicators.bb_middle = bb.middle;
        indicators.bb_lower = bb.lower;
        indicators.bb_width = bb.width;
        indicators.bb_percent = bb.percent_b;
        
        // Volatility
        indicators.atr_14 = pw.atr(14);
        indicators.volatility_1m = pw.volatility(60);
        
        // Trade flow
        if (tf_it != trade_flows_.end()) {
            const auto& tf = *tf_it->second;
            indicators.tfi = tf.tfi();
            indicators.buy_pressure = tf.buy_pressure();
            indicators.sell_pressure = tf.sell_pressure();
        }
        
        // Order book
        if (ob_it != order_books_.end()) {
            const auto& ob = *ob_it->second;
            indicators.obi = ob.obi(5);
            indicators.obi_depth_5 = ob.obi(5);
            indicators.obi_weighted = ob.obi_weighted(10);
        }
        
        indicators.timestamp_ms = now_ms();
        return indicators;
    }
    
    // Get feature vector for a symbol
    FeatureVector get_features(const std::string& symbol) const {
        FeatureVector features;
        features.symbol = symbol;
        features.timestamp_ms = now_ms();
        
        auto pw_it = price_windows_.find(symbol);
        auto tf_it = trade_flows_.find(symbol);
        auto ih_it = indicator_history_.find(symbol);
        
        if (pw_it == price_windows_.end()) return features;
        
        const auto& pw = *pw_it->second;
        
        // Price returns
        features.price_returns = pw.get_returns(config_.feature_patch_size);
        
        // Normalized volumes
        features.volume_normalized = pw.get_normalized_volumes(config_.feature_patch_size);
        
        // Historical indicators
        if (ih_it != indicator_history_.end()) {
            const auto& ih = *ih_it->second;
            features.rsi_series = ih.get_rsi_series(config_.feature_patch_size);
            features.macd_series = ih.get_macd_series(config_.feature_patch_size);
            features.bb_percent_series = ih.get_bb_percent_series(config_.feature_patch_size);
            features.obi_series = ih.get_obi_series(config_.feature_patch_size);
        }
        
        // Trade flow series
        if (tf_it != trade_flows_.end()) {
            features.tfi_series = tf_it->second->get_tfi_series(config_.feature_patch_size);
        }
        
        // Current indicators
        features.current_indicators = get_indicators(symbol);
        
        return features;
    }
    
    bool is_ready(const std::string& symbol) const {
        auto it = price_windows_.find(symbol);
        return it != price_windows_.end() && it->second->is_ready();
    }
    
private:
    // Store indicator history for time series
    class IndicatorHistory {
    public:
        explicit IndicatorHistory(size_t max_size) : max_size_(max_size) {}
        
        void add(const TechnicalIndicators& ind) {
            std::lock_guard<std::mutex> lock(mutex_);
            
            rsi_.push_back(ind.rsi_14);
            if (rsi_.size() > max_size_) rsi_.pop_front();
            
            macd_.push_back(ind.macd_line);
            if (macd_.size() > max_size_) macd_.pop_front();
            
            bb_percent_.push_back(ind.bb_percent);
            if (bb_percent_.size() > max_size_) bb_percent_.pop_front();
            
            obi_.push_back(ind.obi);
            if (obi_.size() > max_size_) obi_.pop_front();
        }
        
        std::vector<double> get_rsi_series(size_t count) const {
            return get_series(rsi_, count);
        }
        
        std::vector<double> get_macd_series(size_t count) const {
            return get_series(macd_, count);
        }
        
        std::vector<double> get_bb_percent_series(size_t count) const {
            return get_series(bb_percent_, count);
        }
        
        std::vector<double> get_obi_series(size_t count) const {
            return get_series(obi_, count);
        }
        
    private:
        std::vector<double> get_series(const std::deque<double>& data, size_t count) const {
            std::lock_guard<std::mutex> lock(mutex_);
            std::vector<double> result;
            
            if (data.empty()) {
                result.resize(count, 0.0);
                return result;
            }
            
            size_t start = data.size() > count ? data.size() - count : 0;
            for (size_t i = start; i < data.size(); ++i) {
                result.push_back(data[i]);
            }
            
            // Pad front if needed
            while (result.size() < count) {
                result.insert(result.begin(), result.empty() ? 0.0 : result.front());
            }
            
            return result;
        }
        
        mutable std::mutex mutex_;
        size_t max_size_;
        std::deque<double> rsi_;
        std::deque<double> macd_;
        std::deque<double> bb_percent_;
        std::deque<double> obi_;
    };
    
    void consume_symbol(const std::string& symbol) {
        std::string topic = "coinbase." + symbol;
        // Replace - with _
        for (auto& c : topic) {
            if (c == '-') c = '_';
        }
        
        std::cout << "[FEATURE] Starting consumer for " << topic << "\n";
        
        // Join consumer group
        std::string group = "feature-engineer";
        client_->join_group(group, topic);
        
        uint64_t offset = 0;
        
        while (running_) {
            // Fetch messages using fetch API
            auto fetch_result = client_->fetch(topic, 0, offset, 100);
            
            if (fetch_result && !fetch_result->messages.empty()) {
                for (const auto& msg : fetch_result->messages) {
                    process_message(symbol, msg->key, msg->value);
                    offset = msg->offset + 1;
                }
            } else {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        }
    }
    
    void process_message(const std::string& symbol, 
                         const std::string& key, 
                         const std::string& value) {
        // Parse message format: TYPE|field1|field2|...
        if (value.empty()) return;
        
        size_t pos = value.find('|');
        if (pos == std::string::npos) return;
        
        std::string type = value.substr(0, pos);
        std::string data = value.substr(pos + 1);
        
        if (type == "TICKER") {
            process_ticker(symbol, data);
        } else if (type == "TRADE") {
            process_trade(symbol, data);
        } else if (type == "L2UPDATE") {
            process_l2update(symbol, data);
        }
    }
    
    void process_ticker(const std::string& symbol, const std::string& data) {
        // Format: price|bid|ask|24h_vol|time
        std::vector<std::string> parts;
        size_t start = 0, end = 0;
        while ((end = data.find('|', start)) != std::string::npos) {
            parts.push_back(data.substr(start, end - start));
            start = end + 1;
        }
        parts.push_back(data.substr(start));
        
        if (parts.size() < 4) return;
        
        try {
            double price = std::stod(parts[0]);
            double bid = std::stod(parts[1]);
            double ask = std::stod(parts[2]);
            double volume = std::stod(parts[3]);
            
            // Update price window
            auto pw_it = price_windows_.find(symbol);
            if (pw_it != price_windows_.end()) {
                pw_it->second->add_tick(price, volume, now_ms());
            }
            
            // Update order book with best bid/ask
            auto ob_it = order_books_.find(symbol);
            if (ob_it != order_books_.end()) {
                ob_it->second->update_bid(bid, 1.0);  // Placeholder size
                ob_it->second->update_ask(ask, 1.0);
            }
            
            ticks_processed_++;
        } catch (...) {
            // Parse error, ignore
        }
    }
    
    void process_trade(const std::string& symbol, const std::string& data) {
        // Format: side|price|size|time|trade_id
        std::vector<std::string> parts;
        size_t start = 0, end = 0;
        while ((end = data.find('|', start)) != std::string::npos) {
            parts.push_back(data.substr(start, end - start));
            start = end + 1;
        }
        parts.push_back(data.substr(start));
        
        if (parts.size() < 3) return;
        
        try {
            std::string side = parts[0];
            double price = std::stod(parts[1]);
            double size = std::stod(parts[2]);
            bool is_buy = (side == "buy");
            
            // Update price window
            auto pw_it = price_windows_.find(symbol);
            if (pw_it != price_windows_.end()) {
                pw_it->second->add_tick(price, size, now_ms());
            }
            
            // Update trade flow
            auto tf_it = trade_flows_.find(symbol);
            if (tf_it != trade_flows_.end()) {
                tf_it->second->add_trade(size, is_buy, now_ms());
            }
            
            trades_processed_++;
        } catch (...) {
            // Parse error, ignore
        }
    }
    
    void process_l2update(const std::string& symbol, const std::string& data) {
        // Format: side|price|size
        std::vector<std::string> parts;
        size_t start = 0, end = 0;
        while ((end = data.find('|', start)) != std::string::npos) {
            parts.push_back(data.substr(start, end - start));
            start = end + 1;
        }
        parts.push_back(data.substr(start));
        
        if (parts.size() < 3) return;
        
        try {
            std::string side = parts[0];
            double price = std::stod(parts[1]);
            double size = std::stod(parts[2]);
            
            auto ob_it = order_books_.find(symbol);
            if (ob_it != order_books_.end()) {
                if (side == "buy") {
                    ob_it->second->update_bid(price, size);
                } else {
                    ob_it->second->update_ask(price, size);
                }
            }
        } catch (...) {
            // Parse error, ignore
        }
    }
    
    void emit_loop() {
        auto last_emit = std::chrono::steady_clock::now();
        auto last_indicator_update = std::chrono::steady_clock::now();
        
        while (running_) {
            auto now = std::chrono::steady_clock::now();
            auto emit_elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                now - last_emit).count();
            
            // Update indicator history every 100ms
            auto indicator_elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                now - last_indicator_update).count();
            
            if (indicator_elapsed >= 100) {
                for (const auto& symbol : config_.symbols) {
                    if (is_ready(symbol)) {
                        auto indicators = get_indicators(symbol);
                        
                        auto ih_it = indicator_history_.find(symbol);
                        if (ih_it != indicator_history_.end()) {
                            ih_it->second->add(indicators);
                        }
                        
                        if (indicator_callback_) {
                            indicator_callback_(symbol, indicators);
                        }
                    }
                }
                last_indicator_update = now;
            }
            
            // Emit features at configured interval
            if (emit_elapsed >= config_.feature_emit_interval_ms) {
                for (const auto& symbol : config_.symbols) {
                    if (is_ready(symbol)) {
                        auto features = get_features(symbol);
                        
                        if (features.is_ready() && feature_callback_) {
                            feature_callback_(features);
                            features_emitted_++;
                        }
                    }
                }
                last_emit = now;
            }
            
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
    
    Config config_;
    std::unique_ptr<StreamClient> client_;
    std::atomic<bool> running_{false};
    
    // Per-symbol state
    std::unordered_map<std::string, std::unique_ptr<PriceWindow>> price_windows_;
    std::unordered_map<std::string, std::unique_ptr<TradeFlowWindow>> trade_flows_;
    std::unordered_map<std::string, std::unique_ptr<OrderBookState>> order_books_;
    std::unordered_map<std::string, std::unique_ptr<IndicatorHistory>> indicator_history_;
    
    // Threads
    std::vector<std::thread> consumer_threads_;
    std::thread emit_thread_;
    
    // Callbacks
    FeatureCallback feature_callback_;
    IndicatorCallback indicator_callback_;
    
    // Stats
    std::atomic<uint64_t> ticks_processed_{0};
    std::atomic<uint64_t> trades_processed_{0};
    std::atomic<uint64_t> features_emitted_{0};
};

} // namespace pipeline
