/**
 * Actuator - Trading & Alerting System
 * 
 * Consumes predictions and executes trading actions with risk management.
 * Supports paper trading and live trading via Coinbase Advanced Trade API.
 */

#pragma once

#include "feature_types.h"
#include "network/client.h"

#include <iostream>
#include <thread>
#include <atomic>
#include <mutex>
#include <map>
#include <deque>
#include <functional>
#include <chrono>
#include <cmath>
#include <iomanip>
#include <sstream>

namespace pipeline {

using namespace stream::network;

// ============================================================================
// Risk Manager - Position and risk limit enforcement
// ============================================================================

class RiskManager {
public:
    explicit RiskManager(const RiskLimits& limits) : limits_(limits) {}
    
    // Check if a trade is allowed
    struct TradeCheck {
        bool allowed{false};
        std::string reason;
    };
    
    TradeCheck can_trade(const std::string& symbol, OrderSide side, 
                         double size, double price) {
        std::lock_guard<std::mutex> lock(mutex_);
        
        // Check order size limit
        if (size > limits_.max_order_size) {
            return {false, "Order size exceeds limit"};
        }
        
        // Check position limit
        double current_position = get_position_size(symbol);
        double new_position = current_position + (side == OrderSide::BUY ? size : -size);
        if (std::abs(new_position) > limits_.max_position_size) {
            return {false, "Position size would exceed limit"};
        }
        
        // Check open orders limit
        if (open_orders_.size() >= static_cast<size_t>(limits_.max_open_orders)) {
            return {false, "Too many open orders"};
        }
        
        // Check order interval
        int64_t now = now_ms();
        if (now - last_order_time_ < limits_.min_order_interval_ms) {
            return {false, "Order too soon after previous"};
        }
        
        // Check daily loss limit
        if (daily_pnl_ < -limits_.max_daily_loss) {
            return {false, "Daily loss limit reached"};
        }
        
        // Check drawdown
        if (drawdown_pct_ > limits_.max_drawdown_pct) {
            return {false, "Drawdown limit reached"};
        }
        
        return {true, "OK"};
    }
    
    void record_order(const Order& order) {
        std::lock_guard<std::mutex> lock(mutex_);
        open_orders_[order.order_id] = order;
        last_order_time_ = now_ms();
    }
    
    void update_order(const Order& order) {
        std::lock_guard<std::mutex> lock(mutex_);
        
        if (order.status == OrderStatus::FILLED) {
            // Update position
            std::string symbol = order.symbol;
            double size = order.filled_size;
            double price = order.filled_price;
            
            if (order.side == OrderSide::BUY) {
                update_position(symbol, size, price);
            } else {
                update_position(symbol, -size, price);
            }
            
            open_orders_.erase(order.order_id);
        } else if (order.status == OrderStatus::CANCELLED || 
                   order.status == OrderStatus::REJECTED) {
            open_orders_.erase(order.order_id);
        } else {
            open_orders_[order.order_id] = order;
        }
    }
    
    void update_market_price(const std::string& symbol, double price) {
        std::lock_guard<std::mutex> lock(mutex_);
        
        market_prices_[symbol] = price;
        update_pnl();
    }
    
    double get_position_size(const std::string& symbol) const {
        auto it = positions_.find(symbol);
        return it != positions_.end() ? it->second.size : 0.0;
    }
    
    AccountState get_account_state() const {
        std::lock_guard<std::mutex> lock(mutex_);
        
        AccountState state;
        state.balance_usd = balance_usd_;
        state.balance_btc = get_position_size("BTC-USD");
        state.equity = equity_;
        state.daily_pnl = daily_pnl_;
        state.peak_equity = peak_equity_;
        state.drawdown_pct = drawdown_pct_;
        
        for (const auto& [symbol, pos] : positions_) {
            state.positions.push_back(pos);
        }
        
        for (const auto& [id, order] : open_orders_) {
            state.open_orders.push_back(order);
        }
        
        return state;
    }
    
    void reset_daily_pnl() {
        std::lock_guard<std::mutex> lock(mutex_);
        daily_pnl_ = 0.0;
    }
    
    void set_initial_balance(double usd) {
        std::lock_guard<std::mutex> lock(mutex_);
        balance_usd_ = usd;
        equity_ = usd;
        peak_equity_ = usd;
    }

private:
    void update_position(const std::string& symbol, double size_delta, double price) {
        auto& pos = positions_[symbol];
        pos.symbol = symbol;
        
        double old_size = pos.size;
        double old_value = old_size * pos.avg_entry_price;
        
        pos.size += size_delta;
        
        if (pos.size == 0) {
            // Closed position - realize PnL
            double pnl = (price - pos.avg_entry_price) * old_size;
            pos.realized_pnl += pnl;
            daily_pnl_ += pnl;
            balance_usd_ += pnl;
            pos.avg_entry_price = 0.0;
        } else if ((old_size >= 0 && size_delta > 0) || 
                   (old_size <= 0 && size_delta < 0)) {
            // Increasing position
            double new_value = old_value + std::abs(size_delta) * price;
            pos.avg_entry_price = new_value / std::abs(pos.size);
        } else {
            // Reducing position
            double closed_size = std::min(std::abs(size_delta), std::abs(old_size));
            double pnl = (price - pos.avg_entry_price) * closed_size * 
                         (old_size > 0 ? 1.0 : -1.0);
            pos.realized_pnl += pnl;
            daily_pnl_ += pnl;
            balance_usd_ += pnl;
        }
        
        if (pos.opened_at_ms == 0 && pos.size != 0) {
            pos.opened_at_ms = now_ms();
        }
        
        update_pnl();
    }
    
    void update_pnl() {
        equity_ = balance_usd_;
        
        for (auto& [symbol, pos] : positions_) {
            auto price_it = market_prices_.find(symbol);
            if (price_it != market_prices_.end() && pos.size != 0) {
                pos.unrealized_pnl = (price_it->second - pos.avg_entry_price) * pos.size;
                equity_ += pos.unrealized_pnl;
            }
        }
        
        if (equity_ > peak_equity_) {
            peak_equity_ = equity_;
        }
        
        if (peak_equity_ > 0) {
            drawdown_pct_ = (peak_equity_ - equity_) / peak_equity_ * 100.0;
        }
    }
    
    mutable std::mutex mutex_;
    RiskLimits limits_;
    
    std::map<std::string, Position> positions_;
    std::map<std::string, Order> open_orders_;
    std::map<std::string, double> market_prices_;
    
    double balance_usd_{10000.0};  // Starting balance
    double equity_{10000.0};
    double peak_equity_{10000.0};
    double daily_pnl_{0.0};
    double drawdown_pct_{0.0};
    int64_t last_order_time_{0};
};

// ============================================================================
// Order Executor Interface
// ============================================================================

class IOrderExecutor {
public:
    virtual ~IOrderExecutor() = default;
    virtual Order submit_order(const std::string& symbol, OrderSide side, 
                              OrderType type, double size, double price = 0.0) = 0;
    virtual bool cancel_order(const std::string& order_id) = 0;
    virtual Order get_order(const std::string& order_id) = 0;
    virtual std::string name() const = 0;
};

// ============================================================================
// Paper Trading Executor (Simulated)
// ============================================================================

class PaperTradingExecutor : public IOrderExecutor {
public:
    explicit PaperTradingExecutor(RiskManager& risk_manager) 
        : risk_manager_(risk_manager), rng_(std::random_device{}()) {}
    
    void set_price_feed(std::function<double(const std::string&)> price_fn) {
        price_feed_ = std::move(price_fn);
    }
    
    Order submit_order(const std::string& symbol, OrderSide side, 
                       OrderType type, double size, double price = 0.0) override {
        std::lock_guard<std::mutex> lock(mutex_);
        
        Order order;
        order.order_id = generate_order_id();
        order.symbol = symbol;
        order.side = side;
        order.type = type;
        order.size = size;
        order.price = price;
        order.created_at_ms = now_ms();
        order.status = OrderStatus::PENDING;
        
        // Get current market price
        double market_price = price_feed_ ? price_feed_(symbol) : price;
        if (market_price <= 0) {
            order.status = OrderStatus::REJECTED;
            std::cout << "[PAPER] Order rejected: no market price\n";
            return order;
        }
        
        // Simulate order execution
        if (type == OrderType::MARKET) {
            // Immediate fill with slippage
            std::uniform_real_distribution<double> slippage(-0.0001, 0.0001);
            order.filled_price = market_price * (1.0 + slippage(rng_));
            order.filled_size = size;
            order.status = OrderStatus::FILLED;
            order.updated_at_ms = now_ms();
        } else if (type == OrderType::LIMIT) {
            // Check if limit can be filled immediately
            bool fillable = (side == OrderSide::BUY && price >= market_price) ||
                           (side == OrderSide::SELL && price <= market_price);
            
            if (fillable) {
                order.filled_price = price;
                order.filled_size = size;
                order.status = OrderStatus::FILLED;
            } else {
                order.status = OrderStatus::OPEN;
            }
            order.updated_at_ms = now_ms();
        }
        
        orders_[order.order_id] = order;
        risk_manager_.update_order(order);
        
        std::cout << "[PAPER] " << (side == OrderSide::BUY ? "BUY" : "SELL")
                  << " " << size << " " << symbol 
                  << " @ " << (type == OrderType::MARKET ? market_price : price)
                  << " -> " << (order.status == OrderStatus::FILLED ? "FILLED" : "OPEN")
                  << "\n";
        
        return order;
    }
    
    bool cancel_order(const std::string& order_id) override {
        std::lock_guard<std::mutex> lock(mutex_);
        
        auto it = orders_.find(order_id);
        if (it == orders_.end()) return false;
        
        if (it->second.status == OrderStatus::OPEN) {
            it->second.status = OrderStatus::CANCELLED;
            it->second.updated_at_ms = now_ms();
            risk_manager_.update_order(it->second);
            return true;
        }
        return false;
    }
    
    Order get_order(const std::string& order_id) override {
        std::lock_guard<std::mutex> lock(mutex_);
        
        auto it = orders_.find(order_id);
        if (it != orders_.end()) {
            return it->second;
        }
        return Order{};
    }
    
    std::string name() const override {
        return "PaperTrading";
    }

private:
    std::string generate_order_id() {
        static std::atomic<uint64_t> counter{0};
        return "PAPER-" + std::to_string(++counter);
    }
    
    std::mutex mutex_;
    RiskManager& risk_manager_;
    std::map<std::string, Order> orders_;
    std::function<double(const std::string&)> price_feed_;
    std::mt19937 rng_;
};

// ============================================================================
// Coinbase Advanced Trade API Executor
// ============================================================================

class CoinbaseExecutor : public IOrderExecutor {
public:
    struct Config {
        std::string api_key;
        std::string api_secret;
        std::string passphrase;
        bool sandbox{true};
    };
    
    explicit CoinbaseExecutor(const Config& config, RiskManager& risk_manager)
        : config_(config), risk_manager_(risk_manager) {
        // In production, initialize Coinbase API client:
        // - Authenticate with API credentials
        // - Setup WebSocket for order updates
        
        std::cout << "[COINBASE] Executor initialized (sandbox=" 
                  << (config.sandbox ? "true" : "false") << ")\n";
    }
    
    Order submit_order(const std::string& symbol, OrderSide side, 
                       OrderType type, double size, double price = 0.0) override {
        Order order;
        order.order_id = generate_order_id();
        order.symbol = symbol;
        order.side = side;
        order.type = type;
        order.size = size;
        order.price = price;
        order.created_at_ms = now_ms();
        
        // In production, call Coinbase API:
        // POST /orders
        // {
        //   "product_id": symbol,
        //   "side": side == OrderSide::BUY ? "buy" : "sell",
        //   "type": type == OrderType::MARKET ? "market" : "limit",
        //   "size": std::to_string(size),
        //   "price": std::to_string(price)  // for limit orders
        // }
        
        order.status = OrderStatus::PENDING;
        std::cout << "[COINBASE] Order submitted: " << order.order_id << "\n";
        
        return order;
    }
    
    bool cancel_order(const std::string& order_id) override {
        // DELETE /orders/{order_id}
        std::cout << "[COINBASE] Cancel request: " << order_id << "\n";
        return true;
    }
    
    Order get_order(const std::string& order_id) override {
        // GET /orders/{order_id}
        return Order{};
    }
    
    std::string name() const override {
        return "CoinbaseAdvancedTrade";
    }

private:
    std::string generate_order_id() {
        static std::atomic<uint64_t> counter{0};
        return "CB-" + std::to_string(now_ms()) + "-" + std::to_string(++counter);
    }
    
    Config config_;
    RiskManager& risk_manager_;
};

// ============================================================================
// Actuator - Main trading controller
// ============================================================================

class Actuator {
public:
    struct Config {
        // Stream processor
        std::string stream_host{"localhost"};
        uint16_t stream_port{9092};
        
        // Symbols to trade
        std::vector<std::string> symbols{"BTC-USD"};
        
        // Trading parameters
        double base_order_size{0.0001};  // Base position size in BTC
        double min_confidence{0.7};       // Minimum confidence to trade
        double signal_threshold{0.65};    // Direction probability threshold
        
        // Risk limits
        RiskLimits risk_limits;
        
        // Mode
        bool paper_trading{true};
        std::string coinbase_api_key;
        std::string coinbase_api_secret;
        std::string coinbase_passphrase;
    };
    
    explicit Actuator(const Config& config) 
        : config_(config), 
          risk_manager_(config.risk_limits) {
        
        // Initialize executor
        if (config.paper_trading) {
            auto paper = std::make_unique<PaperTradingExecutor>(risk_manager_);
            paper->set_price_feed([this](const std::string& symbol) {
                return get_last_price(symbol);
            });
            executor_ = std::move(paper);
        } else {
            CoinbaseExecutor::Config cb_config;
            cb_config.api_key = config.coinbase_api_key;
            cb_config.api_secret = config.coinbase_api_secret;
            cb_config.passphrase = config.coinbase_passphrase;
            cb_config.sandbox = true;
            executor_ = std::make_unique<CoinbaseExecutor>(cb_config, risk_manager_);
        }
        
        risk_manager_.set_initial_balance(10000.0);  // $10k paper balance
    }
    
    ~Actuator() {
        stop();
    }
    
    bool start() {
        if (running_) return true;
        
        // Connect to stream processor
        ClientConfig client_config;
        client_config.host = config_.stream_host;
        client_config.port = config_.stream_port;
        
        client_ = std::make_unique<StreamClient>(client_config);
        if (!client_->connect()) {
            std::cerr << "[ACTUATOR] Failed to connect to stream processor\n";
            return false;
        }
        
        running_ = true;
        
        // Start prediction consumer
        prediction_thread_ = std::thread(&Actuator::consume_predictions, this);
        
        // Start price feed consumer
        price_thread_ = std::thread(&Actuator::consume_prices, this);
        
        // Start stats reporter
        stats_thread_ = std::thread(&Actuator::report_stats, this);
        
        std::cout << "[ACTUATOR] Started in " 
                  << (config_.paper_trading ? "PAPER" : "LIVE") 
                  << " mode with " << executor_->name() << "\n";
        return true;
    }
    
    void stop() {
        running_ = false;
        
        if (prediction_thread_.joinable()) prediction_thread_.join();
        if (price_thread_.joinable()) price_thread_.join();
        if (stats_thread_.joinable()) stats_thread_.join();
        
        if (client_) {
            client_->disconnect();
        }
        
        print_summary();
    }
    
    // Manual trade (for testing)
    void execute_trade(const std::string& symbol, OrderSide side, double size) {
        auto check = risk_manager_.can_trade(symbol, side, size, get_last_price(symbol));
        if (check.allowed) {
            executor_->submit_order(symbol, side, OrderType::MARKET, size);
        } else {
            std::cout << "[ACTUATOR] Trade blocked: " << check.reason << "\n";
        }
    }

private:
    void consume_predictions() {
        std::cout << "[ACTUATOR] Starting prediction consumer\n";
        
        // Subscribe to prediction topics
        for (const auto& symbol : config_.symbols) {
            std::string topic = "predictions." + symbol;
            for (auto& c : topic) if (c == '-') c = '_';
            client_->create_topic(topic, 4);
        }
        
        std::string group = "actuator";
        std::map<std::string, uint64_t> offsets;
        
        while (running_) {
            for (const auto& symbol : config_.symbols) {
                std::string topic = "predictions." + symbol;
                for (auto& c : topic) if (c == '-') c = '_';
                
                uint64_t offset = offsets[topic];
                auto fetch_result = client_->fetch(topic, 0, offset, 10);
                
                if (fetch_result && !fetch_result->messages.empty()) {
                    for (const auto& msg : fetch_result->messages) {
                        process_prediction(symbol, msg->value);
                        offsets[topic] = msg->offset + 1;
                    }
                }
            }
            
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
    
    void process_prediction(const std::string& symbol, const std::string& data) {
        // Parse: timestamp|prob_up|prob_down|ret_1s|ret_5s|ret_30s|confidence|signal
        std::vector<std::string> parts;
        size_t start = 0, end = 0;
        while ((end = data.find('|', start)) != std::string::npos) {
            parts.push_back(data.substr(start, end - start));
            start = end + 1;
        }
        parts.push_back(data.substr(start));
        
        if (parts.size() < 7) return;
        
        try {
            Prediction pred;
            pred.symbol = symbol;
            pred.timestamp_ms = std::stoll(parts[0]);
            pred.prob_up = std::stod(parts[1]);
            pred.prob_down = std::stod(parts[2]);
            pred.predicted_return_1s = std::stod(parts[3]);
            pred.predicted_return_5s = std::stod(parts[4]);
            pred.predicted_return_30s = std::stod(parts[5]);
            pred.confidence = std::stod(parts[6]);
            
            predictions_received_++;
            act_on_prediction(pred);
            
        } catch (...) {
            // Parse error
        }
    }
    
    void act_on_prediction(const Prediction& pred) {
        // Check confidence threshold
        if (pred.confidence < config_.min_confidence) {
            return;
        }
        
        // Determine action
        OrderSide side;
        double current_position = risk_manager_.get_position_size(pred.symbol);
        
        if (pred.prob_up > config_.signal_threshold && current_position <= 0) {
            side = OrderSide::BUY;
        } else if (pred.prob_down > config_.signal_threshold && current_position >= 0) {
            side = OrderSide::SELL;
        } else {
            return;  // No clear signal
        }
        
        // Calculate order size based on confidence
        double size = config_.base_order_size * (0.5 + pred.confidence);
        
        // Risk check
        double price = get_last_price(pred.symbol);
        auto check = risk_manager_.can_trade(pred.symbol, side, size, price);
        
        if (check.allowed) {
            auto order = executor_->submit_order(pred.symbol, side, OrderType::MARKET, size);
            
            if (order.status == OrderStatus::FILLED) {
                trades_executed_++;
                
                std::cout << "[TRADE] " << (side == OrderSide::BUY ? "BUY" : "SELL")
                          << " " << size << " " << pred.symbol
                          << " @ " << order.filled_price
                          << " | Confidence: " << std::fixed << std::setprecision(2) 
                          << (pred.confidence * 100) << "%"
                          << " | Prob: " << (pred.prob_up * 100) << "% up\n";
            }
        } else {
            std::cout << "[ACTUATOR] Trade blocked: " << check.reason << "\n";
        }
    }
    
    void consume_prices() {
        std::map<std::string, uint64_t> offsets;
        
        while (running_) {
            for (const auto& symbol : config_.symbols) {
                std::string topic = "coinbase." + symbol;
                for (auto& c : topic) if (c == '-') c = '_';
                
                uint64_t offset = offsets[topic];
                auto fetch_result = client_->fetch(topic, 0, offset, 100);
                
                if (fetch_result && !fetch_result->messages.empty()) {
                    for (const auto& msg : fetch_result->messages) {
                        // Parse ticker message
                        if (msg->value.find("TICKER|") == 0) {
                            size_t pos = msg->value.find('|');
                            if (pos != std::string::npos) {
                                std::string data = msg->value.substr(pos + 1);
                                pos = data.find('|');
                                if (pos != std::string::npos) {
                                    try {
                                        double price = std::stod(data.substr(0, pos));
                                        set_last_price(symbol, price);
                                        risk_manager_.update_market_price(symbol, price);
                                    } catch (...) {}
                                }
                            }
                        }
                        offsets[topic] = msg->offset + 1;
                    }
                }
            }
            
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }
    
    void report_stats() {
        while (running_) {
            std::this_thread::sleep_for(std::chrono::seconds(30));
            
            auto state = risk_manager_.get_account_state();
            
            std::cout << "\n[STATS] ═══════════════════════════════════════\n"
                      << "  Predictions: " << predictions_received_ << "\n"
                      << "  Trades: " << trades_executed_ << "\n"
                      << "  Balance: $" << std::fixed << std::setprecision(2) 
                      << state.balance_usd << "\n"
                      << "  Equity: $" << state.equity << "\n"
                      << "  Daily P&L: $" << state.daily_pnl << "\n"
                      << "  Drawdown: " << state.drawdown_pct << "%\n"
                      << "  Positions:\n";
            
            for (const auto& pos : state.positions) {
                if (std::abs(pos.size) > 0.00000001) {
                    std::cout << "    " << pos.symbol << ": " << pos.size 
                              << " @ $" << pos.avg_entry_price
                              << " (P&L: $" << pos.unrealized_pnl << ")\n";
                }
            }
            
            std::cout << "═══════════════════════════════════════════════\n" << std::flush;
        }
    }
    
    void print_summary() {
        auto state = risk_manager_.get_account_state();
        
        std::cout << "\n╔═══════════════════════════════════════════════════════════╗\n"
                  << "║                    TRADING SUMMARY                         ║\n"
                  << "╠═══════════════════════════════════════════════════════════╣\n"
                  << "  Mode: " << (config_.paper_trading ? "PAPER" : "LIVE") << "\n"
                  << "  Predictions Received: " << predictions_received_ << "\n"
                  << "  Trades Executed: " << trades_executed_ << "\n"
                  << "  Final Balance: $" << std::fixed << std::setprecision(2) 
                  << state.balance_usd << "\n"
                  << "  Final Equity: $" << state.equity << "\n"
                  << "  Total P&L: $" << (state.equity - 10000.0) << "\n"
                  << "  Return: " << ((state.equity - 10000.0) / 10000.0 * 100) << "%\n"
                  << "  Max Drawdown: " << state.drawdown_pct << "%\n"
                  << "╚═══════════════════════════════════════════════════════════╝\n";
    }
    
    double get_last_price(const std::string& symbol) {
        std::lock_guard<std::mutex> lock(price_mutex_);
        auto it = last_prices_.find(symbol);
        return it != last_prices_.end() ? it->second : 0.0;
    }
    
    void set_last_price(const std::string& symbol, double price) {
        std::lock_guard<std::mutex> lock(price_mutex_);
        last_prices_[symbol] = price;
    }
    
    Config config_;
    RiskManager risk_manager_;
    std::unique_ptr<IOrderExecutor> executor_;
    std::unique_ptr<StreamClient> client_;
    std::atomic<bool> running_{false};
    
    // Threads
    std::thread prediction_thread_;
    std::thread price_thread_;
    std::thread stats_thread_;
    
    // Price cache
    std::mutex price_mutex_;
    std::map<std::string, double> last_prices_;
    
    // Stats
    std::atomic<uint64_t> predictions_received_{0};
    std::atomic<uint64_t> trades_executed_{0};
};

} // namespace pipeline
