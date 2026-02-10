/**
 * Coinbase Inference Pipeline - Main Entry Point
 * 
 * Connects to stream-processor (Docker), consumes market data,
 * builds 512x7 sliding windows, and runs OpenVINO inference.
 * 
 * Usage:
 *   ./coinbase-inference --host localhost --port 9092 --topic coinbase.BTC_USD
 */

#include "pipeline/coinbase_pipeline.h"

#include <iostream>
#include <iomanip>
#include <csignal>
#include <atomic>
#include <thread>
#include <chrono>

std::atomic<bool> g_shutdown{false};

void signal_handler(int signum) {
    std::cout << "\n[SIGNAL] Received signal " << signum << ", shutting down...\n";
    g_shutdown = true;
}

void print_usage(const char* program) {
    std::cout << "Coinbase Inference Pipeline\n\n"
              << "Usage: " << program << " [options]\n\n"
              << "Options:\n"
              << "  --host HOST         Stream processor host (default: localhost)\n"
              << "  --port PORT         Stream processor port (default: 9092)\n"
              << "  --topic TOPIC       Topic to consume (default: coinbase.BTC_USD)\n"
              << "  --model-dir PATH    OpenVINO model directory\n"
              << "  --interval MS       Inference interval in ms (default: 500)\n"
              << "  --help              Show this help\n";
}

int main(int argc, char* argv[]) {
    // Default configuration
    pipeline::CoinbasePipeline::Config config;
    config.stream_host = "localhost";
    config.stream_port = 9092;
    config.topic = "coinbase.BTC_USD";
    config.model_dir = "models/openvino";
    config.inference_interval_ms = 500;
    
    // Parse arguments
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        
        if (arg == "--help" || arg == "-h") {
            print_usage(argv[0]);
            return 0;
        } else if (arg == "--host" && i + 1 < argc) {
            config.stream_host = argv[++i];
        } else if (arg == "--port" && i + 1 < argc) {
            config.stream_port = static_cast<uint16_t>(std::stoi(argv[++i]));
        } else if (arg == "--topic" && i + 1 < argc) {
            config.topic = argv[++i];
        } else if (arg == "--model-dir" && i + 1 < argc) {
            config.model_dir = argv[++i];
        } else if (arg == "--interval" && i + 1 < argc) {
            config.inference_interval_ms = std::stoi(argv[++i]);
        }
    }
    
    // Setup signal handlers
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);
    
    std::cout << R"(
╔══════════════════════════════════════════════════════════════════════╗
║            Coinbase Real-Time Inference Pipeline (C++)               ║
║                                                                      ║
║   Stream Processor → 512x7 Window → OpenVINO PatchTST → Signals     ║
║                                                                      ║
║   Target: AMD EPYC 7763 (Zen 3, AVX2)                               ║
╚══════════════════════════════════════════════════════════════════════╝
)" << std::endl;
    
    std::cout << "[CONFIG]\n"
              << "  Host:              " << config.stream_host << "\n"
              << "  Port:              " << config.stream_port << "\n"
              << "  Topic:             " << config.topic << "\n"
              << "  Model Directory:   " << config.model_dir << "\n"
              << "  Inference Interval: " << config.inference_interval_ms << " ms\n"
              << "  Window Size:       " << pipeline::PATCHTST_CONTEXT_LENGTH << " x " 
              << pipeline::PATCHTST_NUM_CHANNELS << "\n"
              << std::endl;
    
    // Create and start pipeline
    pipeline::CoinbasePipeline pipeline(config);
    
    // Custom callback for predictions (optional)
    pipeline.set_inference_callback([](const pipeline::InferenceResult& result) {
        // ANSI colors
        const char* color;
        switch (result.signal) {
            case pipeline::TradingSignal::BUY: color = "\033[92m"; break;
            case pipeline::TradingSignal::SELL: color = "\033[91m"; break;
            default: color = "\033[93m"; break;
        }
        const char* reset = "\033[0m";
        
        std::cout << "[Prediction #" << result.prediction_num << "] "
                  << "BTC-USD: $" << std::fixed << std::setprecision(2) 
                  << result.current_price << " → "
                  << "Forecast: $" << result.forecast_price 
                  << " (" << std::showpos << std::setprecision(3) 
                  << result.change_pct << "%) "
                  << std::noshowpos
                  << color << pipeline::signal_to_string(result.signal) << reset
                  << " [" << std::setprecision(1) << result.latency_ms << "ms]\n";
    });
    
    if (!pipeline.start()) {
        std::cerr << "[ERROR] Failed to start pipeline\n";
        return 1;
    }
    
    std::cout << "[RUNNING] Press Ctrl+C to stop...\n\n";
    
    // Main loop - wait for shutdown
    while (!g_shutdown) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    
    // Shutdown
    pipeline.stop();
    
    std::cout << "[DONE] Pipeline stopped.\n";
    return 0;
}
