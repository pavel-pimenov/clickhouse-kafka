#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <atomic>
#include <limits>

struct ResourceUsage {
    double cpuUserSec = 0.0;
    double cpuSysSec = 0.0;
    uint64_t currentRssKb = 0;
    uint64_t peakRssKb = 0;
    uint64_t volCtxSwitches = 0;
    uint64_t involCtxSwitches = 0;
    uint64_t pageFaults = 0;
};

struct LatencyStats {
    uint64_t count = 0;
    double min = std::numeric_limits<double>::max();
    double max = 0.0;
    double sum = 0.0;
    double p50 = 0.0;
    double p95 = 0.0;
    double p99 = 0.0;

    double avg() const { return count > 0 ? sum / count : 0.0; }
};

struct BenchmarkResult {
    std::string name;
    double elapsedSeconds;
    uint64_t numRecords;
    double throughputRecsPerSec;
    double throughputMbPerSec;
    ResourceUsage usage;
    LatencyStats latency;
    double dataSizeMB = 0;
};

struct CsvRow {
    std::string phase;
    double elapsedSec;
    double recsPerSec;
    double mbPerSec;
    double cpuUser;
    double cpuSys;
    uint64_t rssKb;
    uint64_t peakRssKb;
    uint64_t volCsw;
    uint64_t involCsw;
    uint64_t pageFaults;
    double latAvgMs;
    double latP50Ms;
    double latP95Ms;
    double latP99Ms;
    double latMaxMs;
    uint64_t records;
    double dataMB;
};

double nowSec();
ResourceUsage getResourceUsage();
std::string resultsCsv(const std::vector<BenchmarkResult>& results);
std::string resultsJson(const std::vector<BenchmarkResult>& results);
void printResults(const std::vector<BenchmarkResult>& results);
std::string getEnv(const std::string& key, const std::string& defaultVal);
void writeFile(const std::string& path, const std::string& content);
