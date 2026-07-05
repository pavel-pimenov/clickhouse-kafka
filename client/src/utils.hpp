#pragma once

#include <cstdint>
#include <string>
#include <vector>

struct ResourceUsage {
    double cpuUserSec = 0.0;
    double cpuSysSec = 0.0;
    uint64_t currentRssKb = 0;
};

struct BenchmarkResult {
    std::string name;
    double elapsedSeconds;
    uint64_t numRecords;
    double throughputRecsPerSec;
    double throughputMbPerSec;
    ResourceUsage usage;
};

double nowSec();
ResourceUsage getResourceUsage();
void printResults(const std::vector<BenchmarkResult>& results);
std::string getEnv(const std::string& key, const std::string& defaultVal);
