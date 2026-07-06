#pragma once

#include <string>
#include <vector>
#include <memory>
#include <atomic>
#include <algorithm>
#include <cstdint>
#include <limits>
#include "record.hpp"
#include "utils.hpp"

struct KafkaWriterConfig {
    std::string brokers;
    std::string compression = "";
    bool asyncFlush = false;
    bool trackLatency = false;
};

class LatencyTracker {
public:
    void add(double latencyMs) {
        updateMin(latencyMs);
        updateMax(latencyMs);
        updateSum(latencyMs);
        count_.fetch_add(1, std::memory_order_relaxed);
        if (count_.load(std::memory_order_relaxed) % 10 == 0) {
            size_t idx = sampleIdx_.fetch_add(1, std::memory_order_relaxed);
            if (idx < MAX_SAMPLES) {
                samples_[idx] = latencyMs;
            }
        }
    }

    LatencyStats computeStats() {
        LatencyStats s;
        s.count = count_.load(std::memory_order_relaxed);
        if (s.count == 0) return s;
        s.min = min_.load(std::memory_order_relaxed);
        s.max = max_.load(std::memory_order_relaxed);
        s.sum = sum_.load(std::memory_order_relaxed);
        size_t n = std::min(sampleIdx_.load(std::memory_order_relaxed), (uint64_t)MAX_SAMPLES);
        if (n > 0) {
            std::sort(samples_, samples_ + n);
            s.p50 = percentile(samples_, n, 50);
            s.p95 = percentile(samples_, n, 95);
            s.p99 = percentile(samples_, n, 99);
        }
        return s;
    }

private:
    static constexpr size_t MAX_SAMPLES = 4096;

    static double percentile(double* sorted, size_t n, int p) {
        if (n == 0) return 0;
        size_t idx = (n * p + 99) / 100;
        if (idx >= n) idx = n - 1;
        return sorted[idx];
    }

    void updateMin(double v) {
        double expected = min_.load(std::memory_order_relaxed);
        while (v < expected && !min_.compare_exchange_weak(expected, v, std::memory_order_relaxed)) {}
    }

    void updateMax(double v) {
        double expected = max_.load(std::memory_order_relaxed);
        while (v > expected && !max_.compare_exchange_weak(expected, v, std::memory_order_relaxed)) {}
    }

    void updateSum(double v) {
        double expected = sum_.load(std::memory_order_relaxed);
        while (!sum_.compare_exchange_weak(expected, expected + v, std::memory_order_relaxed)) {}
    }

    std::atomic<double> min_{std::numeric_limits<double>::max()};
    std::atomic<double> max_{0.0};
    std::atomic<double> sum_{0.0};
    std::atomic<uint64_t> count_{0};
    std::atomic<uint64_t> sampleIdx_{0};
    double samples_[MAX_SAMPLES] = {};
};

class KafkaWriter {
public:
    explicit KafkaWriter(const KafkaWriterConfig& config);
    ~KafkaWriter();

    void produceFloat(const std::vector<FloatRecord>& records, size_t batchSize, const std::string& topic);
    void produceDouble(const std::vector<DoubleRecord>& records, size_t batchSize, const std::string& topic);
    void produceInt(const std::vector<IntRecord>& records, size_t batchSize, const std::string& topic);
    void flush();
    std::shared_ptr<LatencyTracker> getLatencyTracker() const { return latencyTracker_; }

private:
    class Impl;
    std::shared_ptr<LatencyTracker> latencyTracker_;
    std::unique_ptr<Impl> impl_;
};
