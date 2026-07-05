#include "utils.hpp"

#include <chrono>
#include <sys/resource.h>
#include <unistd.h>
#include <fstream>
#include <cstdlib>
#include <iomanip>
#include <iostream>

double nowSec() {
    using namespace std::chrono;
    return duration_cast<duration<double>>(
        steady_clock::now().time_since_epoch()).count();
}

ResourceUsage getResourceUsage() {
    ResourceUsage usage;
    struct rusage ru;
    if (getrusage(RUSAGE_SELF, &ru) == 0) {
        usage.cpuUserSec = ru.ru_utime.tv_sec + ru.ru_utime.tv_usec / 1e6;
        usage.cpuSysSec  = ru.ru_stime.tv_sec + ru.ru_stime.tv_usec / 1e6;
    }

    std::ifstream status("/proc/self/status");
    std::string line;
    while (std::getline(status, line)) {
        if (line.find("VmPeak:") == 0) {
            std::string val;
            for (char c : line) {
                if (std::isdigit(c)) val += c;
            }
            if (!val.empty()) usage.peakRssKb = std::stoull(val);
            break;
        }
    }
    return usage;
}

void printResults(const BenchmarkResult& direct, const BenchmarkResult& kafka) {
    auto fmt = [](double v) {
        std::ostringstream os;
        if (v >= 1e6) os << std::fixed << std::setprecision(2) << (v / 1e6) << " M";
        else if (v >= 1e3) os << std::fixed << std::setprecision(2) << (v / 1e3) << " K";
        else os << std::fixed << std::setprecision(2) << v;
        return os.str();
    };

    std::cout << "\n============================================\n";
    std::cout << "       BENCHMARK RESULTS COMPARISON\n";
    std::cout << "============================================\n\n";

    auto printRow = [&](const std::string& label,
                        const BenchmarkResult& r1, const BenchmarkResult& r2) {
        auto ratio = [](double a, double b) -> std::string {
            if (b == 0) return "inf";
            std::ostringstream os;
            os << std::fixed << std::setprecision(2) << (a / b) << "x";
            return os.str();
        };

        std::cout << std::left << std::setw(35) << label
                  << std::right << std::setw(18) << r1.name
                  << std::setw(18) << r2.name
                  << std::setw(14) << "ratio" << "\n";
        std::cout << std::string(85, '-') << "\n";
        std::cout << std::left << std::setw(35) << "Records"
                  << std::right << std::setw(18) << fmt(r1.numRecords)
                  << std::setw(18) << fmt(r2.numRecords)
                  << std::setw(14) << ratio(r1.numRecords, r2.numRecords) << "\n";
        std::cout << std::left << std::setw(35) << "Elapsed time (s)"
                  << std::right << std::setw(18) << std::fixed << std::setprecision(2) << r1.elapsedSeconds
                  << std::setw(18) << std::fixed << std::setprecision(2) << r2.elapsedSeconds
                  << std::setw(14) << ratio(r1.elapsedSeconds, r2.elapsedSeconds) << "\n";
        std::cout << std::left << std::setw(35) << "Throughput (records/s)"
                  << std::right << std::setw(18) << fmt(r1.throughputRecsPerSec)
                  << std::setw(18) << fmt(r2.throughputRecsPerSec)
                  << std::setw(14) << ratio(r1.throughputRecsPerSec, r2.throughputRecsPerSec) << "\n";
        std::cout << std::left << std::setw(35) << "Throughput (MB/s)"
                  << std::right << std::setw(18) << std::fixed << std::setprecision(3) << r1.throughputMbPerSec
                  << std::setw(18) << std::fixed << std::setprecision(3) << r2.throughputMbPerSec
                  << std::setw(14) << ratio(r1.throughputMbPerSec, r2.throughputMbPerSec) << "\n";
        std::cout << std::left << std::setw(35) << "CPU user (s)"
                  << std::right << std::setw(18) << std::fixed << std::setprecision(2) << r1.usage.cpuUserSec
                  << std::setw(18) << std::fixed << std::setprecision(2) << r2.usage.cpuUserSec
                  << std::setw(14) << ratio(r1.usage.cpuUserSec, r2.usage.cpuUserSec) << "\n";
        std::cout << std::left << std::setw(35) << "CPU sys (s)"
                  << std::right << std::setw(18) << std::fixed << std::setprecision(2) << r1.usage.cpuSysSec
                  << std::setw(18) << std::fixed << std::setprecision(2) << r2.usage.cpuSysSec
                  << std::setw(14) << ratio(r1.usage.cpuSysSec, r2.usage.cpuSysSec) << "\n";
        std::cout << std::left << std::setw(35) << "Peak RSS (KB)"
                  << std::right << std::setw(18) << r1.usage.peakRssKb
                  << std::setw(18) << r2.usage.peakRssKb
                  << std::setw(14) << ratio(r1.usage.peakRssKb, r2.usage.peakRssKb) << "\n\n";
    };

    printRow("Metric", direct, kafka);

    std::cout << "============================================\n";
    std::cout << "RATIO LEGEND: > 1 means first column wins\n";
    std::cout << "============================================\n\n";
}

std::string getEnv(const std::string& key, const std::string& defaultVal) {
    const char* val = std::getenv(key.c_str());
    return val ? std::string(val) : defaultVal;
}
