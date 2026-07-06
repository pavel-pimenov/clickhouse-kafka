#include "utils.hpp"

#include <chrono>
#include <sys/resource.h>
#include <unistd.h>
#include <fstream>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <functional>
#include <sstream>
#include <algorithm>
#include <cmath>
#include <numeric>

double nowSec() {
    using namespace std::chrono;
    return duration_cast<duration<double>>(
        steady_clock::now().time_since_epoch()).count();
}

static uint64_t parseProcStatus(const std::string& key) {
    std::ifstream status("/proc/self/status");
    std::string line;
    while (std::getline(status, line)) {
        if (line.find(key) == 0) {
            std::string val;
            for (char c : line) {
                if (std::isdigit(c)) val += c;
            }
            if (!val.empty()) return std::stoull(val);
        }
    }
    return 0;
}

ResourceUsage getResourceUsage() {
    ResourceUsage usage;
    struct rusage ru;
    if (getrusage(RUSAGE_SELF, &ru) == 0) {
        usage.cpuUserSec = ru.ru_utime.tv_sec + ru.ru_utime.tv_usec / 1e6;
        usage.cpuSysSec  = ru.ru_stime.tv_sec + ru.ru_stime.tv_usec / 1e6;
        usage.volCtxSwitches = ru.ru_nvcsw;
        usage.involCtxSwitches = ru.ru_nivcsw;
        usage.pageFaults = ru.ru_majflt;
    }

    usage.currentRssKb = parseProcStatus("VmRSS:");
    usage.peakRssKb = parseProcStatus("VmPeak:");

    return usage;
}

static std::string fmt(double v) {
    std::ostringstream os;
    if (v >= 1e6) os << std::fixed << std::setprecision(2) << (v / 1e6) << " M";
    else if (v >= 1e3) os << std::fixed << std::setprecision(2) << (v / 1e3) << " K";
    else os << std::fixed << std::setprecision(2) << v;
    return os.str();
}

static std::string ratio(double a, double b) {
    if (b == 0) return "inf";
    std::ostringstream os;
    os << std::fixed << std::setprecision(2) << (a / b) << "x";
    return os.str();
}

std::string resultsCsv(const std::vector<BenchmarkResult>& results) {
    std::ostringstream os;
    os << "phase,elapsed_sec,recs_per_sec,mb_per_sec,"
       << "cpu_user_sec,cpu_sys_sec,rss_kb,peak_rss_kb,"
       << "vol_csw,invol_csw,maj_page_faults,"
       << "lat_avg_ms,lat_p50_ms,lat_p95_ms,lat_p99_ms,lat_max_ms,"
       << "records,data_mb\n";
    for (const auto& r : results) {
        os << r.name << ","
           << r.elapsedSeconds << ","
           << r.throughputRecsPerSec << ","
           << r.throughputMbPerSec << ","
           << r.usage.cpuUserSec << ","
           << r.usage.cpuSysSec << ","
           << r.usage.currentRssKb << ","
           << r.usage.peakRssKb << ","
           << r.usage.volCtxSwitches << ","
           << r.usage.involCtxSwitches << ","
           << r.usage.pageFaults << ","
           << r.latency.avg() << ","
           << r.latency.p50 << ","
           << r.latency.p95 << ","
           << r.latency.p99 << ","
           << r.latency.max << ","
           << r.numRecords << ","
           << r.dataSizeMB << "\n";
    }
    return os.str();
}

std::string resultsJson(const std::vector<BenchmarkResult>& results) {
    std::ostringstream os;
    os << "[\n";
    for (size_t i = 0; i < results.size(); ++i) {
        const auto& r = results[i];
        os << "  {\n"
           << "    \"phase\": \"" << r.name << "\",\n"
           << "    \"elapsed_sec\": " << r.elapsedSeconds << ",\n"
           << "    \"recs_per_sec\": " << r.throughputRecsPerSec << ",\n"
           << "    \"mb_per_sec\": " << r.throughputMbPerSec << ",\n"
           << "    \"cpu_user_sec\": " << r.usage.cpuUserSec << ",\n"
           << "    \"cpu_sys_sec\": " << r.usage.cpuSysSec << ",\n"
           << "    \"rss_kb\": " << r.usage.currentRssKb << ",\n"
           << "    \"peak_rss_kb\": " << r.usage.peakRssKb << ",\n"
           << "    \"vol_csw\": " << r.usage.volCtxSwitches << ",\n"
           << "    \"invol_csw\": " << r.usage.involCtxSwitches << ",\n"
           << "    \"maj_page_faults\": " << r.usage.pageFaults << ",\n"
           << "    \"lat_avg_ms\": " << r.latency.avg() << ",\n"
           << "    \"lat_p50_ms\": " << r.latency.p50 << ",\n"
           << "    \"lat_p95_ms\": " << r.latency.p95 << ",\n"
           << "    \"lat_p99_ms\": " << r.latency.p99 << ",\n"
           << "    \"lat_max_ms\": " << r.latency.max << ",\n"
           << "    \"records\": " << r.numRecords << ",\n"
           << "    \"data_mb\": " << r.dataSizeMB << "\n"
           << "  }";
        if (i < results.size() - 1) os << ",";
        os << "\n";
    }
    os << "]\n";
    return os.str();
}

void printResults(const std::vector<BenchmarkResult>& results) {
    const int colW = 20;
    const int labelW = 35;
    int totalW = labelW + (colW + 2) * static_cast<int>(results.size());

    std::cout << "\n" << std::string(totalW, '=') << "\n";
    std::cout << "       BENCHMARK RESULTS COMPARISON\n";
    std::cout << std::string(totalW, '=') << "\n\n";

    std::cout << std::left << std::setw(labelW) << "Metric";
    for (const auto& r : results)
        std::cout << std::right << std::setw(colW) << r.name;
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

    printRow("Records", [&](const BenchmarkResult& r) { return fmt(static_cast<double>(r.numRecords)); });
    printRow("Elapsed time (s)", [&](const BenchmarkResult& r) {
        std::ostringstream os; os << std::fixed << std::setprecision(2) << r.elapsedSeconds; return os.str();
    });
    printRow("Throughput (rec/s)", [&](const BenchmarkResult& r) { return fmt(r.throughputRecsPerSec); });
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
    printRow("Peak RSS (KB)", [&](const BenchmarkResult& r) {
        return std::to_string(r.usage.peakRssKb);
    });
    printRow("Vol ctx switches", [&](const BenchmarkResult& r) {
        return std::to_string(r.usage.volCtxSwitches);
    });
    printRow("Invol ctx switches", [&](const BenchmarkResult& r) {
        return std::to_string(r.usage.involCtxSwitches);
    });
    printRow("Page faults", [&](const BenchmarkResult& r) {
        return std::to_string(r.usage.pageFaults);
    });

    // Latency rows (only if there are actual measurements)
    bool hasLatency = std::any_of(results.begin(), results.end(),
        [](const BenchmarkResult& r) { return r.latency.count > 0; });
    if (hasLatency) {
        std::cout << "\n" << std::string(totalW, '=') << "\n";
        std::cout << "       DELIVERY LATENCY (ms)\n";
        std::cout << std::string(totalW, '=') << "\n";
        printRow("Lat avg (ms)", [&](const BenchmarkResult& r) {
            std::ostringstream os; os << std::fixed << std::setprecision(3) << r.latency.avg(); return os.str();
        });
        printRow("Lat p50 (ms)", [&](const BenchmarkResult& r) {
            std::ostringstream os; os << std::fixed << std::setprecision(3) << r.latency.p50; return os.str();
        });
        printRow("Lat p95 (ms)", [&](const BenchmarkResult& r) {
            std::ostringstream os; os << std::fixed << std::setprecision(3) << r.latency.p95; return os.str();
        });
        printRow("Lat p99 (ms)", [&](const BenchmarkResult& r) {
            std::ostringstream os; os << std::fixed << std::setprecision(3) << r.latency.p99; return os.str();
        });
        printRow("Lat max (ms)", [&](const BenchmarkResult& r) {
            std::ostringstream os; os << std::fixed << std::setprecision(3) << r.latency.max; return os.str();
        });
    }

    std::cout << "\n" << std::string(totalW, '=') << "\n";
    std::cout << "RATIO columns: vs native = their throughput / native throughput\n";
    std::cout << std::string(totalW, '=') << "\n\n";
}

std::string getEnv(const std::string& key, const std::string& defaultVal) {
    const char* val = std::getenv(key.c_str());
    return val ? std::string(val) : defaultVal;
}

void writeFile(const std::string& path, const std::string& content) {
    std::ofstream out(path);
    if (out.is_open()) {
        out << content;
    } else {
        std::cerr << "WARNING: cannot write " << path << "\n";
    }
}
