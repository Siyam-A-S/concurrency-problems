#pragma once

/**
 * Coinbase Real-Time Inference Pipeline
 * 
 * Components:
 * - FeatureWindow: Thread-safe 512x7 sliding window buffer
 * - OpenVINOEngine: Model inference (stub without OpenVINO runtime)
 * - MarketDataConsumer: TCP client for stream-processor
 * - CoinbasePipeline: Orchestrates all components
 */

#include "feature_types.h"
#include "network/client.h"

#include <deque>
#include <array>
#include <mutex>
#include <atomic>
#include <thread>
#include <chrono>
#include <cmath>
#include <functional>
#include <fstream>
#include <iostream>
#include <sstream>

namespace pipeline {

// =============================================================================
// FeatureWindow - Thread-Safe Sliding Window Buffer
// =============================================================================

class FeatureWindow {
public:
    static constexpr size_t WINDOW_SIZE = PATCHTST_CONTEXT_LENGTH;
    static constexpr size_t NUM_FEATURES = PATCHTST_NUM_CHANNELS;
    static constexpr double PRICE_NORMALIZATION = 45000.0;  // BTC reference price
    
    using FeatureRow = std::array<float, NUM_FEATURES>;
    
    FeatureWindow() {
        price_history_.reserve(WINDOW_SIZE + 10);
    }
    
    /**
     * Add a new market tick and compute features
     */
    void add_tick(const MarketTick& tick) {
        std::lock_guard<std::mutex> lock(mutex_);
        
        // Store raw price for momentum calculation
        price_history_.push_back(tick.price);
        if (price_history_.size() > WINDOW_SIZE + 10) {
            price_history_.erase(price_history_.begin());
        }
        
        // Compute 7-channel feature vector
        FeatureRow features;
        
        // Channel 0: Normalized log price
        features[0] = static_cast<float>(std::log(tick.price / PRICE_NORMALIZATION));
        
        // Channel 1: Bid-ask spread ratio
        features[1] = static_cast<float>((tick.ask - tick.bid) / tick.price);
        
        // Channel 2: Log volume
        features[2] = static_cast<float>(std::log1p(tick.volume));
        
        // Channel 3: Order book imbalance (simplified - would need real order book)
        double obi = (tick.bid - tick.price) / (tick.ask - tick.bid + 1e-8);
        features[3] = static_cast<float>(std::clamp(obi, -1.0, 1.0));
        
        // Channel 4: Price momentum (5-tick lookback)
        if (price_history_.size() > 5) {
            size_t idx = price_history_.size() - 6;
            double prev_price = price_history_[idx];
            double momentum = (tick.price - prev_price) / prev_price * 100.0;
            features[4] = static_cast<float>(std::clamp(momentum, -10.0, 10.0));
        } else {
            features[4] = 0.0f;
        }
        
        // Channel 5: Volatility estimate (rolling std of returns)
        features[5] = compute_volatility();
        
        // Channel 6: Time cyclical encoding
        auto now = std::chrono::system_clock::now();
        auto time_t = std::chrono::system_clock::to_time_t(now);
        std::tm* tm = std::localtime(&time_t);
        double ms_of_day = (tm->tm_hour * 3600 + tm->tm_min * 60 + tm->tm_sec) * 1000.0;
        double day_ms = 24.0 * 3600.0 * 1000.0;
        features[6] = static_cast<float>(std::sin(ms_of_day / day_ms * 2.0 * M_PI));
        
        // Add to sliding window
        window_.push_back(features);
        if (window_.size() > WINDOW_SIZE) {
            window_.pop_front();
        }
        
        last_price_ = tick.price;
        tick_count_++;
    }
    
    /**
     * Check if window is full (ready for inference)
     */
    bool is_ready() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return window_.size() >= WINDOW_SIZE;
    }
    
    /**
     * Get the current window as a flat vector [512 * 7]
     */
    std::vector<float> get_features() const {
        std::lock_guard<std::mutex> lock(mutex_);
        
        std::vector<float> features;
        features.reserve(WINDOW_SIZE * NUM_FEATURES);
        
        for (const auto& row : window_) {
            for (float f : row) {
                features.push_back(f);
            }
        }
        
        // Pad with zeros if not full
        while (features.size() < WINDOW_SIZE * NUM_FEATURES) {
            features.push_back(0.0f);
        }
        
        return features;
    }
    
    size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return window_.size();
    }
    
    double last_price() const { return last_price_; }
    uint64_t tick_count() const { return tick_count_; }

private:
    float compute_volatility() {
        if (price_history_.size() < 10) return 0.0f;
        
        // Compute returns
        std::vector<double> returns;
        for (size_t i = 1; i < std::min(price_history_.size(), size_t(20)); i++) {
            double ret = (price_history_[i] - price_history_[i-1]) / price_history_[i-1];
            returns.push_back(ret);
        }
        
        // Compute std
        double sum = 0.0, sq_sum = 0.0;
        for (double r : returns) {
            sum += r;
            sq_sum += r * r;
        }
        double mean = sum / returns.size();
        double variance = (sq_sum / returns.size()) - (mean * mean);
        return static_cast<float>(std::sqrt(std::max(0.0, variance)) * 100.0);
    }
    
    mutable std::mutex mutex_;
    std::deque<FeatureRow> window_;
    std::vector<double> price_history_;
    std::atomic<double> last_price_{0.0};
    std::atomic<uint64_t> tick_count_{0};
};

// =============================================================================
// OpenVINOEngine - Model Inference
// =============================================================================

/**
 * OpenVINO inference engine
 * 
 * Note: This is a functional stub that simulates inference.
 * To enable real OpenVINO inference, link against OpenVINO runtime
 * and uncomment the OpenVINO-specific code blocks.
 */
class OpenVINOEngine {
public:
    struct Config {
        std::string model_dir = "models/openvino";
        std::string device = "CPU";
        int num_threads = 2;
        bool use_fp16 = true;
    };
    
    explicit OpenVINOEngine(const Config& config) 
        : config_(config), loaded_(false) {}
    
    /**
     * Load the OpenVINO model
     */
    bool load() {
        std::string xml_path = config_.model_dir + "/openvino_model.xml";
        std::string bin_path = config_.model_dir + "/openvino_model.bin";
        
        // Check if model files exist
        std::ifstream xml_file(xml_path);
        std::ifstream bin_file(bin_path);
        
        if (!xml_file.good() || !bin_file.good()) {
            std::cerr << "[OpenVINO] Model files not found at: " << config_.model_dir << "\n";
            std::cerr << "[OpenVINO] Running in SIMULATION MODE\n";
            loaded_ = false;
            return true;  // Allow pipeline to run in simulation mode
        }
        
        std::cout << "[OpenVINO] Model files found:\n"
                  << "  XML: " << xml_path << "\n"
                  << "  BIN: " << bin_path << "\n";
        
        // =====================================================================
        // OpenVINO Runtime Integration (requires linking -lopenvino)
        // =====================================================================
        #ifdef WITH_OPENVINO
        #include <openvino/openvino.hpp>
        
        ov::Core core;
        
        // Set AMD EPYC optimizations
        ov::AnyMap config = {
            {ov::hint::performance_mode.name(), ov::hint::PerformanceMode::LATENCY},
            {ov::inference_num_threads.name(), config_.num_threads},
            {ov::hint::num_requests.name(), 1},
            {ov::affinity.name(), ov::Affinity::CORE},
        };
        
        auto model = core.read_model(xml_path);
        compiled_model_ = core.compile_model(model, config_.device, config);
        infer_request_ = compiled_model_.create_infer_request();
        
        // Get input/output tensors
        input_tensor_ = infer_request_.get_input_tensor();
        output_tensor_ = infer_request_.get_output_tensor();
        
        loaded_ = true;
        #endif
        
        // Simulation mode
        loaded_ = false;
        std::cout << "[OpenVINO] Running in SIMULATION MODE (no runtime linked)\n";
        return true;
    }
    
    /**
     * Run inference on the feature window
     * 
     * Input: [1, 512, 7] flattened to [3584]
     * Output: [1, 96, 1] - 96 future price predictions
     */
    std::vector<float> infer(const std::vector<float>& features) {
        if (features.size() != PATCHTST_CONTEXT_LENGTH * PATCHTST_NUM_CHANNELS) {
            std::cerr << "[OpenVINO] Invalid input size: " << features.size() 
                      << " (expected " << PATCHTST_CONTEXT_LENGTH * PATCHTST_NUM_CHANNELS << ")\n";
            return {};
        }
        
        inference_count_++;
        
        // =====================================================================
        // Real OpenVINO Inference
        // =====================================================================
        // #ifdef WITH_OPENVINO
        // if (loaded_) {
        //     // Copy input data
        //     float* input_data = input_tensor_.data<float>();
        //     std::memcpy(input_data, features.data(), features.size() * sizeof(float));
        //     
        //     // Run inference
        //     infer_request_.infer();
        //     
        //     // Get output
        //     const float* output_data = output_tensor_.data<float>();
        //     return std::vector<float>(output_data, output_data + PATCHTST_PREDICTION_LENGTH);
        // }
        // #endif
        
        // Simulation mode - return trend-following prediction
        return simulate_inference(features);
    }
    
    bool is_loaded() const { return loaded_; }
    uint64_t inference_count() const { return inference_count_; }
    const Config& config() const { return config_; }

private:
    /**
     * Simulate inference for testing without OpenVINO runtime
     * Uses simple momentum-based prediction
     */
    std::vector<float> simulate_inference(const std::vector<float>& features) {
        std::vector<float> predictions(PATCHTST_PREDICTION_LENGTH);
        
        // Get recent prices from feature channel 0 (normalized log price)
        float last_log_price = features[(PATCHTST_CONTEXT_LENGTH - 1) * PATCHTST_NUM_CHANNELS + 0];
        float prev_log_price = features[(PATCHTST_CONTEXT_LENGTH - 10) * PATCHTST_NUM_CHANNELS + 0];
        
        // Simple momentum extrapolation
        float momentum = (last_log_price - prev_log_price) / 10.0f;
        
        // Add some noise for realism
        for (size_t i = 0; i < PATCHTST_PREDICTION_LENGTH; i++) {
            float noise = (static_cast<float>(rand()) / RAND_MAX - 0.5f) * 0.001f;
            predictions[i] = last_log_price + momentum * (i + 1) * 0.5f + noise;
        }
        
        return predictions;
    }
    
    Config config_;
    bool loaded_;
    std::atomic<uint64_t> inference_count_{0};
    
    // OpenVINO objects (when linked with runtime)
    // #ifdef WITH_OPENVINO
    // ov::CompiledModel compiled_model_;
    // ov::InferRequest infer_request_;
    // ov::Tensor input_tensor_;
    // ov::Tensor output_tensor_;
    // #endif
};

// =============================================================================
// MarketDataConsumer - TCP Client for Stream Processor
// =============================================================================

class MarketDataConsumer {
public:
    struct Config {
        std::string host{"localhost"};
        uint16_t port{9092};
        std::string topic{"coinbase.BTC_USD"};
        std::string consumer_group{"inference-pipeline"};
    };
    
    explicit MarketDataConsumer(const Config& config)
        : config_(config) {}
    
    bool connect() {
        stream::network::ClientConfig client_config;
        client_config.host = config_.host;
        client_config.port = config_.port;
        client_config.connect_timeout = std::chrono::milliseconds(5000);
        client_config.auto_reconnect = true;
        
        client_ = std::make_unique<stream::network::StreamClient>(client_config);
        
        if (!client_->connect()) {
            std::cerr << "[Consumer] Failed to connect to " 
                      << config_.host << ":" << config_.port << "\n";
            return false;
        }
        
        std::cout << "[Consumer] Connected to stream processor at "
                  << config_.host << ":" << config_.port << "\n";
        
        // Create topic if it doesn't exist
        client_->create_topic(config_.topic, 1);
        
        // Join consumer group
        auto join_response = client_->join_group(config_.consumer_group, config_.topic);
        if (join_response) {
            consumer_id_ = join_response->consumer_id;
            std::cout << "[Consumer] Joined group '" << config_.consumer_group 
                      << "' as consumer " << consumer_id_ << "\n";
        }
        
        connected_ = true;
        return true;
    }
    
    void disconnect() {
        if (client_ && connected_) {
            if (!consumer_id_.empty()) {
                client_->leave_group(config_.consumer_group, consumer_id_);
            }
            client_->disconnect();
            connected_ = false;
        }
    }
    
    /**
     * Fetch the next batch of market ticks
     * Returns parsed MarketTick objects from JSON message values
     */
    std::vector<MarketTick> fetch_ticks(uint32_t max_messages = 100) {
        std::vector<MarketTick> ticks;
        
        if (!client_ || !connected_) {
            return ticks;
        }
        
        auto response = client_->fetch(config_.topic, 0, current_offset_, max_messages);
        if (!response || response->messages.empty()) {
            return ticks;
        }
        
        for (const auto& msg : response->messages) {
            MarketTick tick = parse_tick_json(msg->value);
            if (tick.is_valid()) {
                ticks.push_back(tick);
            }
            current_offset_ = msg->offset + 1;
        }
        
        // Commit offset periodically
        if (current_offset_ % 100 == 0 && !consumer_id_.empty()) {
            client_->commit_offset(config_.consumer_group, config_.topic, 0, current_offset_);
        }
        
        return ticks;
    }
    
    bool is_connected() const { return connected_; }
    uint64_t current_offset() const { return current_offset_; }
    const Config& config() const { return config_; }

private:
    /**
     * Parse Coinbase ticker JSON into MarketTick
     * Example: {"price": "97000.50", "best_bid": "97000.00", "best_ask": "97001.00", "last_size": "0.5",...}
     */
    MarketTick parse_tick_json(const std::string& json) {
        MarketTick tick;
        
        // Simple JSON parsing - handles both "key": "value" and "key":"value"
        auto extract_value = [&json](const std::string& key) -> double {
            // Look for "key" in the JSON
            std::string key_pattern = "\"" + key + "\"";
            size_t key_pos = json.find(key_pattern);
            if (key_pos == std::string::npos) return 0.0;
            
            // Move past the key and find the colon
            size_t pos = key_pos + key_pattern.length();
            while (pos < json.size() && (json[pos] == ' ' || json[pos] == ':')) pos++;
            
            // Now we should be at the value (possibly quoted)
            if (pos >= json.size()) return 0.0;
            
            if (json[pos] == '"') {
                // Quoted string value
                pos++;  // Skip opening quote
                size_t end = json.find('"', pos);
                if (end == std::string::npos) return 0.0;
                try {
                    return std::stod(json.substr(pos, end - pos));
                } catch (...) {
                    return 0.0;
                }
            } else {
                // Unquoted numeric value
                size_t end = json.find_first_of(",}", pos);
                if (end == std::string::npos) return 0.0;
                try {
                    return std::stod(json.substr(pos, end - pos));
                } catch (...) {
                    return 0.0;
                }
            }
        };
        
        tick.price = extract_value("price");
        tick.bid = extract_value("best_bid");
        tick.ask = extract_value("best_ask");
        tick.volume = extract_value("last_size");
        
        // Fallback for different JSON formats
        if (tick.bid == 0.0) tick.bid = extract_value("bid");
        if (tick.ask == 0.0) tick.ask = extract_value("ask");
        if (tick.volume == 0.0) tick.volume = extract_value("volume");
        
        // Approximate bid/ask from price if not available
        if (tick.bid == 0.0 && tick.price > 0.0) tick.bid = tick.price * 0.9999;
        if (tick.ask == 0.0 && tick.price > 0.0) tick.ask = tick.price * 1.0001;
        
        tick.timestamp_ms = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()
            ).count()
        );
        
        return tick;
    }
    
    Config config_;
    std::unique_ptr<stream::network::StreamClient> client_;
    std::string consumer_id_;
    uint64_t current_offset_{0};
    bool connected_{false};
};

// =============================================================================
// CoinbasePipeline - Main Orchestrator
// =============================================================================

class CoinbasePipeline {
public:
    struct Config {
        std::string stream_host{"localhost"};
        uint16_t stream_port{9092};
        std::string topic{"coinbase.BTC_USD"};
        std::string model_dir{"models/openvino"};
        int inference_interval_ms{500};
    };
    
    using InferenceCallback = std::function<void(const InferenceResult&)>;
    
    explicit CoinbasePipeline(const Config& config) : config_(config) {}
    
    ~CoinbasePipeline() {
        stop();
    }
    
    void set_inference_callback(InferenceCallback callback) {
        callback_ = std::move(callback);
    }
    
    bool start() {
        // Initialize OpenVINO engine
        OpenVINOEngine::Config engine_config;
        engine_config.model_dir = config_.model_dir;
        engine_config.num_threads = 2;
        
        engine_ = std::make_unique<OpenVINOEngine>(engine_config);
        if (!engine_->load()) {
            std::cerr << "[Pipeline] Failed to load OpenVINO model\n";
            return false;
        }
        
        // Connect to stream processor
        MarketDataConsumer::Config consumer_config;
        consumer_config.host = config_.stream_host;
        consumer_config.port = config_.stream_port;
        consumer_config.topic = config_.topic;
        
        consumer_ = std::make_unique<MarketDataConsumer>(consumer_config);
        if (!consumer_->connect()) {
            std::cerr << "[Pipeline] Failed to connect to stream processor\n";
            return false;
        }
        
        // Start consumer thread
        running_ = true;
        consumer_thread_ = std::thread(&CoinbasePipeline::consumer_loop, this);
        
        // Start inference thread
        inference_thread_ = std::thread(&CoinbasePipeline::inference_loop, this);
        
        std::cout << "[Pipeline] Started successfully\n";
        return true;
    }
    
    void stop() {
        running_ = false;
        
        if (consumer_thread_.joinable()) {
            consumer_thread_.join();
        }
        if (inference_thread_.joinable()) {
            inference_thread_.join();
        }
        
        if (consumer_) {
            consumer_->disconnect();
        }
    }
    
    bool is_running() const { return running_; }
    
    // Statistics
    uint64_t ticks_processed() const { return window_.tick_count(); }
    uint64_t predictions_made() const { return prediction_count_; }
    bool window_ready() const { return window_.is_ready(); }
    size_t window_size() const { return window_.size(); }

private:
    void consumer_loop() {
        std::cout << "[Consumer] Starting consumption loop...\n";
        
        while (running_) {
            auto ticks = consumer_->fetch_ticks(100);
            
            for (const auto& tick : ticks) {
                window_.add_tick(tick);
            }
            
            // Small sleep if no data
            if (ticks.empty()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        }
        
        std::cout << "[Consumer] Loop ended\n";
    }
    
    void inference_loop() {
        std::cout << "[Inference] Starting inference loop (interval: " 
                  << config_.inference_interval_ms << "ms)...\n";
        
        while (running_) {
            // Wait for window to fill
            if (!window_.is_ready()) {
                std::cout << "[Inference] Waiting for window... (" 
                          << window_.size() << "/" << PATCHTST_CONTEXT_LENGTH << ")\r" << std::flush;
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                continue;
            }
            
            // Run inference
            auto start = std::chrono::high_resolution_clock::now();
            
            auto features = window_.get_features();
            auto predictions = engine_->infer(features);
            
            auto end = std::chrono::high_resolution_clock::now();
            double latency_ms = std::chrono::duration<double, std::milli>(end - start).count();
            
            if (!predictions.empty()) {
                // Build result
                InferenceResult result;
                result.prediction_num = ++prediction_count_;
                result.current_price = window_.last_price();
                result.latency_ms = latency_ms;
                result.timestamp_ms = static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::system_clock::now().time_since_epoch()
                    ).count()
                );
                
                // Convert prediction from log-space
                // Output is normalized log price, need to denormalize
                float pred_log_price = predictions[PATCHTST_PREDICTION_LENGTH / 2];  // Mid-horizon
                result.forecast_price = std::exp(pred_log_price) * FeatureWindow::PRICE_NORMALIZATION;
                
                // Calculate change
                if (result.current_price > 0) {
                    result.change_pct = (result.forecast_price - result.current_price) 
                                        / result.current_price * 100.0;
                }
                
                // Determine signal
                if (result.change_pct > 0.1) {
                    result.signal = TradingSignal::BUY;
                } else if (result.change_pct < -0.1) {
                    result.signal = TradingSignal::SELL;
                } else {
                    result.signal = TradingSignal::HOLD;
                }
                
                // Invoke callback
                if (callback_) {
                    callback_(result);
                }
            }
            
            // Wait for next inference interval
            std::this_thread::sleep_for(std::chrono::milliseconds(config_.inference_interval_ms));
        }
        
        std::cout << "[Inference] Loop ended\n";
    }
    
    Config config_;
    FeatureWindow window_;
    std::unique_ptr<OpenVINOEngine> engine_;
    std::unique_ptr<MarketDataConsumer> consumer_;
    
    std::thread consumer_thread_;
    std::thread inference_thread_;
    std::atomic<bool> running_{false};
    std::atomic<uint64_t> prediction_count_{0};
    
    InferenceCallback callback_;
};

} // namespace pipeline
