#include "utils.hpp"

#include <chrono>
#include <sys/resource.h>
#include <unistd.h>
#include <fstream>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <functional>

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
        if (line.find("VmRSS:") == 0) {
            std::string val;
            for (char c : line) {
                if (std::isdigit(c)) val += c;
            }
            if (!val.empty()) usage.currentRssKb = std::stoull(val);
            break;
        }
    }
    return usage;
}

void printResults(const std::vector<BenchmarkResult>& results) {
    auto fmt = [](double v) {
        std::ostringstream os;
        if (v >= 1e6) os << std::fixed << std::setprecision(2) << (v / 1e6) << " M";
        else if (v >= 1e3) os << std::fixed << std::setprecision(2) << (v / 1e3) << " K";
        else os << std::fixed << std::setprecision(2) << v;
        return os.str();
    };

    auto ratio = [](double a, double b) -> std::string {
        if (b == 0) return "inf";
        std::ostringstream os;
        os << std::fixed << std::setprecision(2) << (a / b) << "x";
        return os.str();
    };

    const int colW = 20;
    const int labelW = 35;
    int totalW = labelW + (colW + 2) * static_cast<int>(results.size());

    std::cout << "\n" << std::string(totalW, '=') << "\n";
    std::cout << "       BENCHMARK RESULTS COMPARISON\n";
    std::cout << std::string(totalW, '=') << "\n\n";

    // header row
    std::cout << std::left << std::setw(labelW) << "Metric";
    for (const auto& r : results)
        std::cout << std::right << std::setw(colW) << r.name;
    // ratio columns
    for (size_t i = 1; i < results.size(); ++i)
        std::cout << std::right << std::setw(colW) << ("vs " + results[i].name);
    std::cout << "\n" << std::string(totalW, '-') << "\n";

    auto printRow = [&](const std::string& label,
                        std::function<std::string(const BenchmarkResult&)> getVal) {
        std::cout << std::left << std::setw(labelW) << label;
        for (const auto& r : results)
            std::cout << std::right << std::setw(colW) << getVal(r);
        for (size_t i = 1; i < results.size(); ++i)
            std::cout << std::right << std::setw(colW) << ratio(
                results[i].throughputRecsPerSec, results[0].throughputRecsPerSec);
        std::cout << "\n";
    };

    printRow("Records", [&](const BenchmarkResult& r) { return fmt(r.numRecords); });
    printRow("Elapsed time (s)", [&](const BenchmarkResult& r) {
        std::ostringstream os; os << std::fixed << std::setprecision(2) << r.elapsedSeconds; return os.str();
    });
    printRow("Throughput (records/s)", [&](const BenchmarkResult& r) { return fmt(r.throughputRecsPerSec); });
    printRow("Throughput (MB/s)", [&](const BenchmarkResult& r) {
        std::ostringstream os; os << std::fixed << std::setprecision(3) << r.throughputMbPerSec; return os.str();
    });
    printRow("CPU user (s)", [&](const BenchmarkResult& r) {
        std::ostringstream os; os << std::fixed << std::setprecision(2) << r.usage.cpuUserSec; return os.str();
    });
    printRow("CPU sys (s)", [&](const BenchmarkResult& r) {
        std::ostringstream os; os << std::fixed << std::setprecision(2) << r.usage.cpuSysSec; return os.str();
    });
    printRow("RSS (KB)", [&](const BenchmarkResult& r) {
        return std::to_string(r.usage.currentRssKb);
    });

    std::cout << "\n" << std::string(totalW, '=') << "\n";
    std::cout << "RATIO columns: vs Kafka/Redpanda = their throughput / native throughput\n";
    std::cout << std::string(totalW, '=') << "\n\n";
}

std::string getEnv(const std::string& key, const std::string& defaultVal) {
    const char* val = std::getenv(key.c_str());
    return val ? std::string(val) : defaultVal;
}
