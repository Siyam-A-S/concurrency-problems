/**
 * Inference Engine - ML Model Serving
 * 
 * High-performance inference engine for PatchTST-style models.
 * Supports both local inference and remote model servers (Triton/BentoML).
 */

#pragma once

#include "feature_types.h"
#include <vector>
#include <memory>
#include <mutex>
#include <thread>
#include <atomic>
#include <queue>
#include <condition_variable>
#include <functional>
#include <chrono>
#include <cmath>
#include <random>
#include <sstream>

namespace pipeline {

// ============================================================================
// Model Interface
// ============================================================================

class IModel {
public:
    virtual ~IModel() = default;
    virtual Prediction predict(const FeatureVector& features) = 0;
    virtual std::string version() const = 0;
    virtual bool is_ready() const = 0;
};

// ============================================================================
// Mock PatchTST Model (for testing/demo)
// ============================================================================

class MockPatchTSTModel : public IModel {
public:
    MockPatchTSTModel() : rng_(std::random_device{}()) {}
    
    Prediction predict(const FeatureVector& features) override {
        auto start = std::chrono::high_resolution_clock::now();
        
        Prediction pred;
        pred.symbol = features.symbol;
        pred.timestamp_ms = now_ms();
        pred.model_version = version();
        
        // Simulate model computation based on features
        // In production, this would be replaced with actual model inference
        
        // Use RSI and OBI as primary signals
        double rsi = features.current_indicators.rsi_14;
        double obi = features.current_indicators.obi;
        double tfi = features.current_indicators.tfi;
        double bb_pct = features.current_indicators.bb_percent;
        
        // Composite signal
        double signal = 0.0;
        
        // RSI signal: oversold (<30) = buy, overbought (>70) = sell
        if (rsi < 30) signal += 0.3;
        else if (rsi > 70) signal -= 0.3;
        else signal += (50 - rsi) / 100.0 * 0.2;
        
        // OBI signal: positive = buy pressure
        signal += obi * 0.3;
        
        // TFI signal: trade flow direction
        signal += tfi * 0.2;
        
        // Bollinger Band signal: mean reversion
        if (bb_pct < 0.2) signal += 0.2;  // Near lower band = buy
        else if (bb_pct > 0.8) signal -= 0.2;  // Near upper band = sell
        
        // Add small noise for realism
        std::normal_distribution<double> noise(0.0, 0.05);
        signal += noise(rng_);
        
        // Clamp signal to [-1, 1]
        signal = clamp(signal, -1.0, 1.0);
        
        // Convert signal to probabilities
        pred.prob_up = 0.5 + signal * 0.4;
        pred.prob_down = 1.0 - pred.prob_up;
        
        // Predicted returns (simplified)
        pred.predicted_return_1s = signal * 0.0001;   // 0.01% max
        pred.predicted_return_5s = signal * 0.0003;
        pred.predicted_return_30s = signal * 0.001;
        
        // Confidence based on signal strength and agreement of indicators
        double indicator_agreement = std::abs(obi - tfi) < 0.3 ? 0.8 : 0.5;
        pred.confidence = std::abs(signal) * indicator_agreement;
        
        // Simulate inference latency (1-5ms)
        std::this_thread::sleep_for(std::chrono::microseconds(
            static_cast<int>(1000 + noise(rng_) * 500)));
        
        auto end = std::chrono::high_resolution_clock::now();
        pred.inference_latency_ms = std::chrono::duration<double, std::milli>(
            end - start).count();
        
        return pred;
    }
    
    std::string version() const override {
        return "MockPatchTST-v1.0";
    }
    
    bool is_ready() const override {
        return true;
    }

private:
    mutable std::mt19937 rng_;
};

// ============================================================================
// ONNX Runtime Model (for production with real models)
// ============================================================================

class ONNXModel : public IModel {
public:
    explicit ONNXModel(const std::string& model_path) 
        : model_path_(model_path), ready_(false) {
        // In production, initialize ONNX Runtime here:
        // - Load model file
        // - Create inference session
        // - Allocate input/output tensors
        
        // Placeholder: pretend we loaded the model
        ready_ = true;
        std::cout << "[MODEL] Loaded ONNX model from " << model_path << "\n";
    }
    
    Prediction predict(const FeatureVector& features) override {
        auto start = std::chrono::high_resolution_clock::now();
        
        Prediction pred;
        pred.symbol = features.symbol;
        pred.timestamp_ms = now_ms();
        pred.model_version = version();
        
        if (!ready_) {
            pred.confidence = 0.0;
            return pred;
        }
        
        // Convert features to tensor
        auto tensor = features.to_tensor();
        
        // In production, run ONNX inference:
        // Ort::Value input_tensor = Ort::Value::CreateTensor<float>(...);
        // auto output_tensors = session_.Run(...);
        
        // Placeholder: use mock values
        pred.prob_up = 0.5;
        pred.prob_down = 0.5;
        pred.predicted_return_1s = 0.0;
        pred.confidence = 0.0;
        
        auto end = std::chrono::high_resolution_clock::now();
        pred.inference_latency_ms = std::chrono::duration<double, std::milli>(
            end - start).count();
        
        return pred;
    }
    
    std::string version() const override {
        return "PatchTST-ONNX-v1.0";
    }
    
    bool is_ready() const override {
        return ready_;
    }

private:
    std::string model_path_;
    std::atomic<bool> ready_;
    // Ort::Session session_;  // ONNX Runtime session
};

// ============================================================================
// Remote Model Client (gRPC to Triton/BentoML)
// ============================================================================

class RemoteModelClient : public IModel {
public:
    struct Config {
        std::string server_host{"localhost"};
        uint16_t server_port{8001};
        std::string model_name{"patchtst"};
        int timeout_ms{100};
    };
    
    explicit RemoteModelClient(const Config& config) : config_(config) {
        // In production, initialize gRPC channel:
        // channel_ = grpc::CreateChannel(config.server_host + ":" + 
        //                                std::to_string(config.server_port),
        //                                grpc::InsecureChannelCredentials());
        // stub_ = InferenceService::NewStub(channel_);
        
        connected_ = true;  // Placeholder
        std::cout << "[MODEL] Connected to model server at " 
                  << config.server_host << ":" << config.server_port << "\n";
    }
    
    Prediction predict(const FeatureVector& features) override {
        auto start = std::chrono::high_resolution_clock::now();
        
        Prediction pred;
        pred.symbol = features.symbol;
        pred.timestamp_ms = now_ms();
        pred.model_version = version();
        
        if (!connected_) {
            pred.confidence = 0.0;
            return pred;
        }
        
        // In production, make gRPC call:
        // InferRequest request;
        // request.set_model_name(config_.model_name);
        // request.mutable_input()->set_data(features.to_tensor());
        // 
        // InferResponse response;
        // grpc::ClientContext context;
        // context.set_deadline(std::chrono::system_clock::now() + 
        //                      std::chrono::milliseconds(config_.timeout_ms));
        // 
        // auto status = stub_->Infer(&context, request, &response);
        
        // Placeholder: use mock inference
        MockPatchTSTModel mock;
        pred = mock.predict(features);
        pred.model_version = version();
        
        auto end = std::chrono::high_resolution_clock::now();
        pred.inference_latency_ms = std::chrono::duration<double, std::milli>(
            end - start).count();
        
        return pred;
    }
    
    std::string version() const override {
        return "RemoteModel-" + config_.model_name + "-v1.0";
    }
    
    bool is_ready() const override {
        return connected_;
    }

private:
    Config config_;
    std::atomic<bool> connected_{false};
    // std::shared_ptr<grpc::Channel> channel_;
    // std::unique_ptr<InferenceService::Stub> stub_;
};

// ============================================================================
// Inference Engine - Manages model serving and batching
// ============================================================================

class InferenceEngine {
public:
    struct Config {
        // Model configuration
        std::string model_type{"mock"};  // "mock", "onnx", "remote"
        std::string model_path;
        std::string remote_host{"localhost"};
        uint16_t remote_port{8001};
        
        // Batching
        bool enable_batching{false};
        size_t max_batch_size{32};
        int batch_timeout_ms{10};
        
        // Queue
        size_t max_queue_size{1000};
        
        // Threading
        size_t num_inference_threads{2};
    };
    
    using PredictionCallback = std::function<void(const Prediction&)>;
    
    explicit InferenceEngine(const Config& config) : config_(config) {
        // Create model
        if (config.model_type == "onnx") {
            model_ = std::make_unique<ONNXModel>(config.model_path);
        } else if (config.model_type == "remote") {
            RemoteModelClient::Config remote_config;
            remote_config.server_host = config.remote_host;
            remote_config.server_port = config.remote_port;
            model_ = std::make_unique<RemoteModelClient>(remote_config);
        } else {
            model_ = std::make_unique<MockPatchTSTModel>();
        }
    }
    
    ~InferenceEngine() {
        stop();
    }
    
    void set_prediction_callback(PredictionCallback cb) {
        prediction_callback_ = std::move(cb);
    }
    
    bool start() {
        if (running_) return true;
        
        if (!model_ || !model_->is_ready()) {
            std::cerr << "[INFERENCE] Model not ready\n";
            return false;
        }
        
        running_ = true;
        
        // Start inference worker threads
        for (size_t i = 0; i < config_.num_inference_threads; ++i) {
            workers_.emplace_back(&InferenceEngine::worker_loop, this);
        }
        
        std::cout << "[INFERENCE] Started with " << config_.num_inference_threads 
                  << " workers, model: " << model_->version() << "\n";
        return true;
    }
    
    void stop() {
        running_ = false;
        cv_.notify_all();
        
        for (auto& w : workers_) {
            if (w.joinable()) w.join();
        }
        workers_.clear();
        
        double avg_latency;
        {
            std::lock_guard<std::mutex> lock(latency_mutex_);
            avg_latency = predictions_made_ > 0 ? total_latency_ms_ / predictions_made_ : 0.0;
        }
        std::cout << "[INFERENCE] Stopped. Processed: " << predictions_made_ 
                  << ", Avg latency: " << avg_latency << "ms\n";
    }
    
    // Submit features for inference (non-blocking)
    bool submit(const FeatureVector& features) {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        
        if (queue_.size() >= config_.max_queue_size) {
            queue_drops_++;
            return false;
        }
        
        queue_.push(features);
        cv_.notify_one();
        return true;
    }
    
    // Synchronous inference
    Prediction predict(const FeatureVector& features) {
        if (!model_ || !model_->is_ready()) {
            return Prediction{};
        }
        
        auto pred = model_->predict(features);
        predictions_made_++;
        {
            std::lock_guard<std::mutex> lock(latency_mutex_);
            total_latency_ms_ += pred.inference_latency_ms;
        }
        return pred;
    }
    
    // Stats
    uint64_t predictions_made() const { return predictions_made_; }
    uint64_t queue_drops() const { return queue_drops_; }
    double avg_latency_ms() const {
        std::lock_guard<std::mutex> lock(latency_mutex_);
        return predictions_made_ > 0 ? total_latency_ms_ / predictions_made_ : 0.0;
    }
    
    size_t queue_size() const {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        return queue_.size();
    }

private:
    void worker_loop() {
        while (running_) {
            FeatureVector features;
            
            {
                std::unique_lock<std::mutex> lock(queue_mutex_);
                cv_.wait_for(lock, std::chrono::milliseconds(10), [this] {
                    return !queue_.empty() || !running_;
                });
                
                if (!running_ && queue_.empty()) break;
                if (queue_.empty()) continue;
                
                features = std::move(queue_.front());
                queue_.pop();
            }
            
            // Run inference
            auto pred = model_->predict(features);
            predictions_made_++;
            {
                std::lock_guard<std::mutex> lock(latency_mutex_);
                total_latency_ms_ += pred.inference_latency_ms;
            }
            
            // Invoke callback
            if (prediction_callback_) {
                prediction_callback_(pred);
            }
        }
    }
    
    Config config_;
    std::unique_ptr<IModel> model_;
    std::atomic<bool> running_{false};
    
    // Worker threads
    std::vector<std::thread> workers_;
    
    // Request queue
    mutable std::mutex queue_mutex_;
    std::condition_variable cv_;
    std::queue<FeatureVector> queue_;
    
    // Callback
    PredictionCallback prediction_callback_;
    
    // Stats
    std::atomic<uint64_t> predictions_made_{0};
    std::atomic<uint64_t> queue_drops_{0};
    mutable std::mutex latency_mutex_;
    double total_latency_ms_{0.0};
};

// ============================================================================
// Prediction Publisher - Sends predictions to stream processor
// ============================================================================

class PredictionPublisher {
public:
    struct Config {
        std::string stream_host{"localhost"};
        uint16_t stream_port{9092};
        std::string topic_prefix{"predictions"};
    };
    
    explicit PredictionPublisher(const Config& config) : config_(config) {}
    
    bool connect() {
        stream::network::ClientConfig client_config;
        client_config.host = config_.stream_host;
        client_config.port = config_.stream_port;
        
        client_ = std::make_unique<stream::network::StreamClient>(client_config);
        if (!client_->connect()) {
            std::cerr << "[PUBLISHER] Failed to connect\n";
            return false;
        }
        
        connected_ = true;
        std::cout << "[PUBLISHER] Connected to stream processor\n";
        return true;
    }
    
    void disconnect() {
        if (client_) {
            client_->disconnect();
        }
        connected_ = false;
    }
    
    bool publish(const Prediction& pred) {
        if (!connected_) return false;
        
        std::string topic = config_.topic_prefix + "." + pred.symbol;
        for (auto& c : topic) {
            if (c == '-') c = '_';
        }
        
        // Ensure topic exists
        client_->create_topic(topic, 4);
        
        // Format: timestamp|prob_up|prob_down|ret_1s|ret_5s|ret_30s|confidence|signal
        std::ostringstream oss;
        oss << pred.timestamp_ms << "|"
            << pred.prob_up << "|"
            << pred.prob_down << "|"
            << pred.predicted_return_1s << "|"
            << pred.predicted_return_5s << "|"
            << pred.predicted_return_30s << "|"
            << pred.confidence << "|"
            << pred.signal_str();
        
        std::string key = pred.symbol + ":" + std::to_string(pred.timestamp_ms);
        auto result = client_->produce(topic, key, oss.str());
        return result.has_value();
    }
    
private:
    Config config_;
    std::unique_ptr<stream::network::StreamClient> client_;
    std::atomic<bool> connected_{false};
};

} // namespace pipeline
