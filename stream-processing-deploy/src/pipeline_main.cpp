/**
 * ML Inference Pipeline - Main Entry Point
 * 
 * Complete pipeline for real-time market data processing:
 * 1. Feature Engineering - Compute technical indicators from raw data
 * 2. Inference - Run ML model predictions
 * 3. Actuation - Execute trades based on predictions
 */

#include "pipeline/feature_engineer.h"
#include "pipeline/inference_engine.h"
#include "pipeline/actuator.h"

#include <iostream>
#include <csignal>
#include <atomic>
#include <thread>
#include <cstdlib>
#include <iomanip>

using namespace pipeline;

// Global shutdown flag
std::atomic<bool> g_shutdown{false};

void signal_handler(int signum) {
    std::cout << "\n[PIPELINE] Received signal " << signum << ", shutting down...\n";
    g_shutdown = true;
}

struct PipelineConfig {
    // Stream processor
    std::string stream_host{"localhost"};
    uint16_t stream_port{9092};
    
    // Symbols
    std::vector<std::string> symbols{"BTC-USD", "ETH-USD"};
    
    // Components
    bool enable_features{true};
    bool enable_inference{true};
    bool enable_trading{true};
    
    // Trading mode
    bool paper_trading{true};
    
    // Model
    std::string model_type{"mock"};
    std::string model_path;
    std::string model_server_host{"localhost"};
    uint16_t model_server_port{8001};
    
    // Risk
    double max_position{0.01};
    double max_order{0.001};
    double max_daily_loss{100.0};
};

PipelineConfig parse_args(int argc, char* argv[]) {
    PipelineConfig config;
    
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        
        if (arg.find("--stream-host=") == 0) {
            config.stream_host = arg.substr(14);
        } else if (arg.find("--stream-port=") == 0) {
            config.stream_port = static_cast<uint16_t>(std::stoi(arg.substr(14)));
        } else if (arg.find("--symbols=") == 0) {
            config.symbols.clear();
            std::string symbols = arg.substr(10);
            size_t pos = 0;
            while ((pos = symbols.find(',')) != std::string::npos) {
                config.symbols.push_back(symbols.substr(0, pos));
                symbols.erase(0, pos + 1);
            }
            if (!symbols.empty()) config.symbols.push_back(symbols);
        } else if (arg == "--no-features") {
            config.enable_features = false;
        } else if (arg == "--no-inference") {
            config.enable_inference = false;
        } else if (arg == "--no-trading") {
            config.enable_trading = false;
        } else if (arg == "--live-trading") {
            config.paper_trading = false;
        } else if (arg.find("--model-type=") == 0) {
            config.model_type = arg.substr(13);
        } else if (arg.find("--model-path=") == 0) {
            config.model_path = arg.substr(13);
        } else if (arg.find("--model-server=") == 0) {
            std::string server = arg.substr(15);
            size_t colon = server.find(':');
            if (colon != std::string::npos) {
                config.model_server_host = server.substr(0, colon);
                config.model_server_port = static_cast<uint16_t>(
                    std::stoi(server.substr(colon + 1)));
            }
        } else if (arg.find("--max-position=") == 0) {
            config.max_position = std::stod(arg.substr(15));
        } else if (arg.find("--max-order=") == 0) {
            config.max_order = std::stod(arg.substr(12));
        } else if (arg.find("--max-daily-loss=") == 0) {
            config.max_daily_loss = std::stod(arg.substr(17));
        } else if (arg == "--help" || arg == "-h") {
            std::cout << "ML Inference Pipeline\n"
                      << "Usage: " << argv[0] << " [options]\n\n"
                      << "Options:\n"
                      << "  --stream-host=HOST      Stream processor host (default: localhost)\n"
                      << "  --stream-port=PORT      Stream processor port (default: 9092)\n"
                      << "  --symbols=SYM1,SYM2     Symbols to process (default: BTC-USD,ETH-USD)\n"
                      << "\nComponent Control:\n"
                      << "  --no-features           Disable feature engineering\n"
                      << "  --no-inference          Disable ML inference\n"
                      << "  --no-trading            Disable trading (analysis only)\n"
                      << "\nTrading:\n"
                      << "  --live-trading          Enable live trading (default: paper)\n"
                      << "  --max-position=SIZE     Max position in BTC (default: 0.01)\n"
                      << "  --max-order=SIZE        Max order size (default: 0.001)\n"
                      << "  --max-daily-loss=USD    Max daily loss (default: 100)\n"
                      << "\nModel:\n"
                      << "  --model-type=TYPE       Model type: mock, onnx, remote (default: mock)\n"
                      << "  --model-path=PATH       Path to ONNX model file\n"
                      << "  --model-server=H:P      Remote model server host:port\n"
                      << "\n";
            std::exit(0);
        }
    }
    
    // Override from environment
    if (const char* env = std::getenv("STREAM_HOST")) config.stream_host = env;
    if (const char* env = std::getenv("STREAM_PORT")) {
        config.stream_port = static_cast<uint16_t>(std::stoi(env));
    }
    if (const char* env = std::getenv("MODEL_TYPE")) config.model_type = env;
    if (const char* env = std::getenv("MODEL_PATH")) config.model_path = env;
    if (const char* env = std::getenv("PAPER_TRADING")) {
        std::string val = env;
        config.paper_trading = (val != "false" && val != "0");
    }
    
    return config;
}

void print_banner() {
    std::cout << R"(
╔═══════════════════════════════════════════════════════════╗
║         ML Inference Pipeline v1.0                        ║
║    Real-Time Feature Engineering & Model Inference        ║
║                                                           ║
║    ┌──────────┐   ┌───────────┐   ┌───────────┐          ║
║    │ Features │ → │ Inference │ → │ Actuation │          ║
║    └──────────┘   └───────────┘   └───────────┘          ║
╚═══════════════════════════════════════════════════════════╝
)" << std::endl;
}

int main(int argc, char* argv[]) {
    try {
        print_banner();
        
        auto config = parse_args(argc, argv);
        
        // Setup signal handlers
        std::signal(SIGINT, signal_handler);
        std::signal(SIGTERM, signal_handler);
        
        std::cout << "[PIPELINE] Configuration:\n"
                  << "  Stream Server:  " << config.stream_host << ":" << config.stream_port << "\n"
                  << "  Symbols:        ";
        for (const auto& s : config.symbols) std::cout << s << " ";
        std::cout << "\n"
                  << "  Features:       " << (config.enable_features ? "enabled" : "disabled") << "\n"
                  << "  Inference:      " << (config.enable_inference ? "enabled" : "disabled") << "\n"
                  << "  Trading:        " << (config.enable_trading ? 
                        (config.paper_trading ? "PAPER" : "LIVE") : "disabled") << "\n"
                  << "  Model:          " << config.model_type << "\n"
                  << "\n" << std::flush;
        
        // Initialize components
        std::unique_ptr<FeatureEngineer> feature_engineer;
        std::unique_ptr<InferenceEngine> inference_engine;
        std::unique_ptr<PredictionPublisher> publisher;
        std::unique_ptr<Actuator> actuator;
        
        // Create inference engine
        if (config.enable_inference) {
            InferenceEngine::Config ie_config;
            ie_config.model_type = config.model_type;
            ie_config.model_path = config.model_path;
            ie_config.remote_host = config.model_server_host;
            ie_config.remote_port = config.model_server_port;
            ie_config.num_inference_threads = 2;
            
            inference_engine = std::make_unique<InferenceEngine>(ie_config);
            
            // Create publisher for predictions
            PredictionPublisher::Config pub_config;
            pub_config.stream_host = config.stream_host;
            pub_config.stream_port = config.stream_port;
            publisher = std::make_unique<PredictionPublisher>(pub_config);
            
            if (!publisher->connect()) {
                std::cerr << "[PIPELINE] Failed to connect publisher\n";
                return 1;
            }
            
            // Set callback to publish predictions
            inference_engine->set_prediction_callback([&publisher](const Prediction& pred) {
                publisher->publish(pred);
                
                // Log high-confidence predictions
                if (pred.confidence > 0.6) {
                    std::cout << "[PREDICTION] " << pred.symbol 
                              << " | " << pred.signal_str()
                              << " | Confidence: " << std::fixed << std::setprecision(1) 
                              << (pred.confidence * 100) << "%"
                              << " | Prob Up: " << (pred.prob_up * 100) << "%"
                              << " | Latency: " << std::setprecision(2) 
                              << pred.inference_latency_ms << "ms\n";
                }
            });
            
            if (!inference_engine->start()) {
                std::cerr << "[PIPELINE] Failed to start inference engine\n";
                return 1;
            }
        }
        
        // Create feature engineer
        if (config.enable_features) {
            FeatureEngineer::Config fe_config;
            fe_config.stream_host = config.stream_host;
            fe_config.stream_port = config.stream_port;
            fe_config.symbols = config.symbols;
            fe_config.feature_emit_interval_ms = 100;  // 10 Hz
            
            feature_engineer = std::make_unique<FeatureEngineer>(fe_config);
            
            // Set callbacks
            feature_engineer->set_indicator_callback([](const std::string& symbol, 
                                                        const TechnicalIndicators& ind) {
                // Log indicators periodically (every ~10 seconds)
                static std::map<std::string, int64_t> last_log;
                int64_t now = now_ms();
                
                if (now - last_log[symbol] > 10000) {
                    std::cout << "[INDICATORS] " << symbol 
                              << " | Price: $" << std::fixed << std::setprecision(2) 
                              << ind.sma_5
                              << " | RSI: " << std::setprecision(1) << ind.rsi_14
                              << " | OBI: " << std::setprecision(3) << ind.obi
                              << " | TFI: " << ind.tfi
                              << " | BB%: " << std::setprecision(2) << ind.bb_percent
                              << "\n";
                    last_log[symbol] = now;
                }
            });
            
            if (inference_engine) {
                feature_engineer->set_feature_callback([&inference_engine](const FeatureVector& features) {
                    inference_engine->submit(features);
                });
            }
            
            if (!feature_engineer->start()) {
                std::cerr << "[PIPELINE] Failed to start feature engineer\n";
                return 1;
            }
        }
        
        // Create actuator
        if (config.enable_trading) {
            Actuator::Config act_config;
            act_config.stream_host = config.stream_host;
            act_config.stream_port = config.stream_port;
            act_config.symbols = config.symbols;
            act_config.paper_trading = config.paper_trading;
            act_config.risk_limits.max_position_size = config.max_position;
            act_config.risk_limits.max_order_size = config.max_order;
            act_config.risk_limits.max_daily_loss = config.max_daily_loss;
            
            actuator = std::make_unique<Actuator>(act_config);
            
            if (!actuator->start()) {
                std::cerr << "[PIPELINE] Failed to start actuator\n";
                return 1;
            }
        }
        
        std::cout << "[PIPELINE] ✓ Pipeline started successfully!\n"
                  << "[PIPELINE] Processing real-time market data...\n\n" << std::flush;
        
        // Main loop - print stats periodically
        uint64_t iteration = 0;
        while (!g_shutdown) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            iteration++;
            
            // Print periodic summary every 60 seconds
            if (iteration % 60 == 0 && inference_engine) {
                std::cout << "\n[PIPELINE STATS] ─────────────────────────────────\n"
                          << "  Predictions: " << inference_engine->predictions_made()
                          << " | Avg Latency: " << std::fixed << std::setprecision(2) 
                          << inference_engine->avg_latency_ms() << "ms"
                          << " | Queue: " << inference_engine->queue_size()
                          << " | Drops: " << inference_engine->queue_drops()
                          << "\n─────────────────────────────────────────────────\n" 
                          << std::flush;
            }
        }
        
        // Shutdown
        std::cout << "\n[PIPELINE] Shutting down...\n";
        
        if (actuator) actuator->stop();
        if (feature_engineer) feature_engineer->stop();
        if (inference_engine) inference_engine->stop();
        if (publisher) publisher->disconnect();
        
        std::cout << "[PIPELINE] Goodbye!\n";
        return 0;
        
    } catch (const std::exception& e) {
        std::cerr << "[PIPELINE] FATAL ERROR: " << e.what() << "\n";
        return 1;
    }
}
