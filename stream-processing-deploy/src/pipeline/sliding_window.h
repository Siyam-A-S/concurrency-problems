/**
 * Sliding Window - Thread-safe sliding window for time series data
 * 
 * Maintains a fixed-size window of recent data points for feature calculation.
 */

#pragma once

#include "feature_types.h"
#include <deque>
#include <map>
#include <mutex>
#include <optional>
#include <numeric>
#include <algorithm>

namespace pipeline {

template<typename T>
class SlidingWindow {
public:
    explicit SlidingWindow(size_t max_size) : max_size_(max_size) {}
    
    void push(const T& value) {
        std::lock_guard<std::mutex> lock(mutex_);
        data_.push_back(value);
        if (data_.size() > max_size_) {
            data_.pop_front();
        }
    }
    
    void push(T&& value) {
        std::lock_guard<std::mutex> lock(mutex_);
        data_.push_back(std::move(value));
        if (data_.size() > max_size_) {
            data_.pop_front();
        }
    }
    
    size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return data_.size();
    }
    
    bool is_full() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return data_.size() >= max_size_;
    }
    
    std::optional<T> front() const {
        std::lock_guard<std::mutex> lock(mutex_);
        if (data_.empty()) return std::nullopt;
        return data_.front();
    }
    
    std::optional<T> back() const {
        std::lock_guard<std::mutex> lock(mutex_);
        if (data_.empty()) return std::nullopt;
        return data_.back();
    }
    
    std::optional<T> at(size_t index) const {
        std::lock_guard<std::mutex> lock(mutex_);
        if (index >= data_.size()) return std::nullopt;
        return data_[index];
    }
    
    std::vector<T> to_vector() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return std::vector<T>(data_.begin(), data_.end());
    }
    
    std::vector<T> last_n(size_t n) const {
        std::lock_guard<std::mutex> lock(mutex_);
        if (data_.empty()) return {};
        size_t count = std::min(n, data_.size());
        return std::vector<T>(data_.end() - count, data_.end());
    }
    
    void clear() {
        std::lock_guard<std::mutex> lock(mutex_);
        data_.clear();
    }
    
    template<typename Func>
    void for_each(Func&& func) const {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& item : data_) {
            func(item);
        }
    }
    
private:
    mutable std::mutex mutex_;
    std::deque<T> data_;
    size_t max_size_;
};

// Specialized sliding window for price data with indicator calculations
class PriceWindow {
public:
    explicit PriceWindow(size_t window_size = 120)
        : window_size_(window_size), prices_(window_size), volumes_(window_size) {}
    
    void add_tick(double price, double volume, int64_t timestamp_ms) {
        std::lock_guard<std::mutex> lock(mutex_);
        
        prices_.push_back(price);
        if (prices_.size() > window_size_) prices_.pop_front();
        
        volumes_.push_back(volume);
        if (volumes_.size() > window_size_) volumes_.pop_front();
        
        last_timestamp_ = timestamp_ms;
        tick_count_++;
    }
    
    // Simple Moving Average
    double sma(size_t period) const {
        std::lock_guard<std::mutex> lock(mutex_);
        if (prices_.size() < period) return 0.0;
        
        double sum = 0.0;
        auto it = prices_.end() - period;
        for (size_t i = 0; i < period; ++i, ++it) {
            sum += *it;
        }
        return sum / period;
    }
    
    // Exponential Moving Average
    double ema(size_t period) const {
        std::lock_guard<std::mutex> lock(mutex_);
        if (prices_.empty()) return 0.0;
        
        double multiplier = 2.0 / (period + 1);
        double ema_val = prices_.front();
        
        for (size_t i = 1; i < prices_.size(); ++i) {
            ema_val = (prices_[i] - ema_val) * multiplier + ema_val;
        }
        return ema_val;
    }
    
    // Relative Strength Index
    double rsi(size_t period = 14) const {
        std::lock_guard<std::mutex> lock(mutex_);
        if (prices_.size() < period + 1) return 50.0;
        
        double gains = 0.0, losses = 0.0;
        
        auto start = prices_.end() - period - 1;
        for (size_t i = 0; i < period; ++i) {
            double change = *(start + i + 1) - *(start + i);
            if (change > 0) gains += change;
            else losses -= change;
        }
        
        if (losses == 0) return 100.0;
        double rs = gains / losses;
        return 100.0 - (100.0 / (1.0 + rs));
    }
    
    // MACD
    struct MACD {
        double line;
        double signal;
        double histogram;
    };
    
    MACD macd(size_t fast = 12, size_t slow = 26, size_t signal_period = 9) const {
        MACD result{0.0, 0.0, 0.0};
        
        double ema_fast = ema(fast);
        double ema_slow = ema(slow);
        result.line = ema_fast - ema_slow;
        
        // Simplified signal calculation
        result.signal = result.line * 0.8;  // Approximation
        result.histogram = result.line - result.signal;
        
        return result;
    }
    
    // Bollinger Bands
    struct BollingerBands {
        double upper;
        double middle;
        double lower;
        double width;
        double percent_b;
    };
    
    BollingerBands bollinger(size_t period = 20, double num_std = 2.0) const {
        std::lock_guard<std::mutex> lock(mutex_);
        BollingerBands bb{0.0, 0.0, 0.0, 0.0, 0.5};
        
        if (prices_.size() < period) return bb;
        
        // Calculate SMA
        double sum = 0.0;
        auto start = prices_.end() - period;
        for (size_t i = 0; i < period; ++i) {
            sum += *(start + i);
        }
        bb.middle = sum / period;
        
        // Calculate standard deviation
        double sq_sum = 0.0;
        for (size_t i = 0; i < period; ++i) {
            double diff = *(start + i) - bb.middle;
            sq_sum += diff * diff;
        }
        double std_dev = std::sqrt(sq_sum / period);
        
        bb.upper = bb.middle + num_std * std_dev;
        bb.lower = bb.middle - num_std * std_dev;
        bb.width = (bb.upper - bb.lower) / bb.middle;
        
        double current_price = prices_.back();
        if (bb.upper != bb.lower) {
            bb.percent_b = (current_price - bb.lower) / (bb.upper - bb.lower);
        }
        
        return bb;
    }
    
    // Average True Range (approximation without high/low)
    double atr(size_t period = 14) const {
        std::lock_guard<std::mutex> lock(mutex_);
        if (prices_.size() < period + 1) return 0.0;
        
        double sum = 0.0;
        auto start = prices_.end() - period - 1;
        for (size_t i = 0; i < period; ++i) {
            sum += std::abs(*(start + i + 1) - *(start + i));
        }
        return sum / period;
    }
    
    // Price volatility (standard deviation of returns)
    double volatility(size_t period) const {
        std::lock_guard<std::mutex> lock(mutex_);
        if (prices_.size() < period + 1) return 0.0;
        
        std::vector<double> returns;
        auto start = prices_.end() - period - 1;
        for (size_t i = 0; i < period; ++i) {
            double ret = (*(start + i + 1) - *(start + i)) / *(start + i);
            returns.push_back(ret);
        }
        
        double mean = std::accumulate(returns.begin(), returns.end(), 0.0) / returns.size();
        double sq_sum = 0.0;
        for (double r : returns) {
            sq_sum += (r - mean) * (r - mean);
        }
        return std::sqrt(sq_sum / returns.size());
    }
    
    // Get recent returns for model input
    std::vector<double> get_returns(size_t count) const {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<double> returns;
        
        if (prices_.size() < 2) return returns;
        
        size_t start_idx = prices_.size() > count + 1 ? prices_.size() - count - 1 : 0;
        for (size_t i = start_idx; i < prices_.size() - 1; ++i) {
            double ret = (prices_[i + 1] - prices_[i]) / prices_[i];
            returns.push_back(ret);
        }
        return returns;
    }
    
    // Get normalized volumes
    std::vector<double> get_normalized_volumes(size_t count) const {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<double> result;
        
        if (volumes_.empty()) return result;
        
        // Calculate mean volume
        double mean = std::accumulate(volumes_.begin(), volumes_.end(), 0.0) / volumes_.size();
        if (mean <= 0) mean = 1.0;
        
        size_t start_idx = volumes_.size() > count ? volumes_.size() - count : 0;
        for (size_t i = start_idx; i < volumes_.size(); ++i) {
            result.push_back(volumes_[i] / mean);
        }
        return result;
    }
    
    double last_price() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return prices_.empty() ? 0.0 : prices_.back();
    }
    
    size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return prices_.size();
    }
    
    bool is_ready() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return prices_.size() >= window_size_ / 2;  // At least half full
    }
    
private:
    mutable std::mutex mutex_;
    size_t window_size_;
    std::deque<double> prices_;
    std::deque<double> volumes_;
    int64_t last_timestamp_{0};
    uint64_t tick_count_{0};
};

// Trade flow window for calculating trade imbalance
class TradeFlowWindow {
public:
    explicit TradeFlowWindow(size_t window_size = 100) 
        : window_size_(window_size) {}
    
    void add_trade(double size, bool is_buy, int64_t timestamp_ms) {
        std::lock_guard<std::mutex> lock(mutex_);
        
        Trade t{size, is_buy, timestamp_ms};
        trades_.push_back(t);
        if (trades_.size() > window_size_) {
            trades_.pop_front();
        }
        
        // Update running totals
        if (is_buy) {
            buy_volume_ += size;
        } else {
            sell_volume_ += size;
        }
        
        // Remove old volume from totals
        if (trades_.size() > window_size_) {
            const auto& old = trades_.front();
            if (old.is_buy) buy_volume_ -= old.size;
            else sell_volume_ -= old.size;
        }
    }
    
    // Trade Flow Imbalance [-1, 1]
    double tfi() const {
        std::lock_guard<std::mutex> lock(mutex_);
        double total = buy_volume_ + sell_volume_;
        if (total <= 0) return 0.0;
        return (buy_volume_ - sell_volume_) / total;
    }
    
    double buy_pressure() const {
        std::lock_guard<std::mutex> lock(mutex_);
        double total = buy_volume_ + sell_volume_;
        if (total <= 0) return 0.5;
        return buy_volume_ / total;
    }
    
    double sell_pressure() const {
        std::lock_guard<std::mutex> lock(mutex_);
        double total = buy_volume_ + sell_volume_;
        if (total <= 0) return 0.5;
        return sell_volume_ / total;
    }
    
    // Get TFI time series
    std::vector<double> get_tfi_series(size_t count) const {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<double> series;
        
        if (trades_.empty()) return series;
        
        double buy_sum = 0.0, sell_sum = 0.0;
        size_t bucket_size = trades_.size() / count;
        if (bucket_size < 1) bucket_size = 1;
        
        size_t bucket_count = 0;
        for (const auto& t : trades_) {
            if (t.is_buy) buy_sum += t.size;
            else sell_sum += t.size;
            
            if (++bucket_count >= bucket_size) {
                double total = buy_sum + sell_sum;
                double tfi = (total > 0) ? (buy_sum - sell_sum) / total : 0.0;
                series.push_back(tfi);
                buy_sum = sell_sum = 0.0;
                bucket_count = 0;
            }
        }
        
        // Pad to requested size
        while (series.size() < count) {
            series.insert(series.begin(), 0.0);
        }
        
        return series;
    }
    
private:
    struct Trade {
        double size;
        bool is_buy;
        int64_t timestamp_ms;
    };
    
    mutable std::mutex mutex_;
    size_t window_size_;
    std::deque<Trade> trades_;
    double buy_volume_{0.0};
    double sell_volume_{0.0};
};

// Order book for calculating imbalance
class OrderBookState {
public:
    void update_bid(double price, double size) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (size <= 0) {
            bids_.erase(price);
        } else {
            bids_[price] = size;
        }
        trim_book(bids_, 50, false);
    }
    
    void update_ask(double price, double size) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (size <= 0) {
            asks_.erase(price);
        } else {
            asks_[price] = size;
        }
        trim_book(asks_, 50, true);
    }
    
    // Order Book Imbalance at top N levels [-1, 1]
    double obi(size_t levels = 5) const {
        std::lock_guard<std::mutex> lock(mutex_);
        
        double bid_volume = 0.0, ask_volume = 0.0;
        
        size_t count = 0;
        for (auto it = bids_.rbegin(); it != bids_.rend() && count < levels; ++it, ++count) {
            bid_volume += it->second;
        }
        
        count = 0;
        for (auto it = asks_.begin(); it != asks_.end() && count < levels; ++it, ++count) {
            ask_volume += it->second;
        }
        
        double total = bid_volume + ask_volume;
        if (total <= 0) return 0.0;
        return (bid_volume - ask_volume) / total;
    }
    
    // Volume-weighted OBI
    double obi_weighted(size_t levels = 10) const {
        std::lock_guard<std::mutex> lock(mutex_);
        
        if (bids_.empty() || asks_.empty()) return 0.0;
        
        double mid = (bids_.rbegin()->first + asks_.begin()->first) / 2.0;
        double bid_weighted = 0.0, ask_weighted = 0.0;
        
        size_t count = 0;
        for (auto it = bids_.rbegin(); it != bids_.rend() && count < levels; ++it, ++count) {
            double distance = mid - it->first;
            double weight = 1.0 / (1.0 + distance / mid * 100.0);
            bid_weighted += it->second * weight;
        }
        
        count = 0;
        for (auto it = asks_.begin(); it != asks_.end() && count < levels; ++it, ++count) {
            double distance = it->first - mid;
            double weight = 1.0 / (1.0 + distance / mid * 100.0);
            ask_weighted += it->second * weight;
        }
        
        double total = bid_weighted + ask_weighted;
        if (total <= 0) return 0.0;
        return (bid_weighted - ask_weighted) / total;
    }
    
    double best_bid() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return bids_.empty() ? 0.0 : bids_.rbegin()->first;
    }
    
    double best_ask() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return asks_.empty() ? 0.0 : asks_.begin()->first;
    }
    
    double mid_price() const {
        double bid = best_bid();
        double ask = best_ask();
        if (bid <= 0 || ask <= 0) return 0.0;
        return (bid + ask) / 2.0;
    }
    
    double spread_bps() const {
        double bid = best_bid();
        double ask = best_ask();
        if (bid <= 0 || ask <= 0) return 0.0;
        double mid = (bid + ask) / 2.0;
        return (ask - bid) / mid * 10000.0;
    }
    
private:
    void trim_book(std::map<double, double>& book, size_t max_levels, bool keep_low) {
        while (book.size() > max_levels) {
            if (keep_low) {
                book.erase(std::prev(book.end()));
            } else {
                book.erase(book.begin());
            }
        }
    }
    
    mutable std::mutex mutex_;
    std::map<double, double> bids_;  // price -> size
    std::map<double, double> asks_;
};

} // namespace pipeline
