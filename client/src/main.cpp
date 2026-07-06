#include "record.hpp"
#include "clickhouse_writer.hpp"
#include "kafka_writer.hpp"
#include "utils.hpp"

#include <iostream>
#include <thread>
#include <chrono>
#include <iomanip>
#include <sstream>
#include <functional>
#include <vector>
#include <string>

static void waitForServices(ClickHouseWriter& ch, int maxRetries) {
    std::cout << "Waiting for ClickHouse...\n";
    for (int i = 0; i < maxRetries; ++i) {
        if (ch.ping()) {
            std::cout << "ClickHouse ready\n";
            return;
        }
        std::this_thread::sleep_for(std::chrono::seconds(2));
    }
    throw std::runtime_error("ClickHouse not ready after " + std::to_string(maxRetries) + " retries");
}

static uint64_t verifyConsumption(ClickHouseWriter& ch, const std::string& table,
                                    const std::string& label, uint64_t expected,
                                    int maxWait = 60) {
    std::cout << "  Verifying " << label << " -> " << table << "...\n";
    for (int i = 0; i < maxWait; ++i) {
        auto count = ch.countTable(table);
        if (count >= expected) {
            std::cout << "    All " << count << " records consumed\n";
            return count;
        }
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    auto count = ch.countTable(table);
    std::cout << "    WARNING: Only " << count << "/" << expected << " after " << maxWait << "s\n";
    return count;
}

static void runPhase(const std::string& phaseLabel,
                     std::function<void()> fn,
                     const std::string& countLabel, uint64_t count,
                     double dataMB,
                     std::vector<BenchmarkResult>& results,
                     LatencyStats latStats = {})
{
    std::cout << "=== " << phaseLabel << " ===\n";
    auto before = getResourceUsage();
    auto t0 = nowSec();
    fn();
    auto t1 = nowSec();
    auto after = getResourceUsage();
    double elapsed = t1 - t0;
    std::cout << "  " << countLabel << ": " << count << "\n"
              << "  Time: " << std::fixed << std::setprecision(2) << elapsed << " s\n"
              << "  Throughput: " << std::fixed << std::setprecision(1)
              << (count / elapsed) << " rec/s, "
              << (dataMB / elapsed) << " MB/s\n\n";

    results.push_back({
        phaseLabel,
        elapsed,
        count,
        count / elapsed,
        dataMB / elapsed,
        {after.cpuUserSec - before.cpuUserSec,
         after.cpuSysSec - before.cpuSysSec,
         after.currentRssKb,
         after.peakRssKb,
         after.volCtxSwitches - before.volCtxSwitches,
         after.involCtxSwitches - before.involCtxSwitches,
         after.pageFaults - before.pageFaults},
        latStats,
        dataMB
    });
}

// Sequential produce with optional async flush
static void produceSeq(KafkaWriter& w, const TestData& data, size_t batchSize,
                        const std::string& suffix, bool asyncFlush)
{
    w.produceFloat(data.floats, batchSize, "signals-float" + suffix);
    w.produceDouble(data.doubles, batchSize, "signals-double" + suffix);
    w.produceInt(data.ints, batchSize, "signals-int" + suffix);
    if (asyncFlush) w.flush();
}

// Parallel produce: one KafkaWriter per type, 3 concurrent threads
static void produceParallel(const KafkaWriterConfig& baseCfg,
                             const TestData& data, size_t batchSize,
                             const std::string& suffix, bool asyncFlush)
{
    KafkaWriterConfig cfg = baseCfg;
    cfg.asyncFlush = asyncFlush;

    auto runFloat = [&]() {
        KafkaWriter w(cfg);
        w.produceFloat(data.floats, batchSize, "signals-float" + suffix);
        if (asyncFlush) w.flush();
    };
    auto runDouble = [&]() {
        KafkaWriter w(cfg);
        w.produceDouble(data.doubles, batchSize, "signals-double" + suffix);
        if (asyncFlush) w.flush();
    };
    auto runInt = [&]() {
        KafkaWriter w(cfg);
        w.produceInt(data.ints, batchSize, "signals-int" + suffix);
        if (asyncFlush) w.flush();
    };
    std::thread t1(runFloat), t2(runDouble), t3(runInt);
    t1.join(); t2.join(); t3.join();
}

static void verifyBroker(ClickHouseWriter& ch, const TestData& data,
                          const std::string& brokerLabel, const std::string& sinkSuffix)
{
    std::cout << "=== Verifying " << brokerLabel << " -> ClickHouse consumption ===\n";
    auto vf = verifyConsumption(ch, "signals_float_sink" + sinkSuffix,
                                 brokerLabel + "/float", data.floats.size());
    auto vd = verifyConsumption(ch, "signals_double_sink" + sinkSuffix,
                                 brokerLabel + "/double", data.doubles.size());
    auto vi = verifyConsumption(ch, "signals_int_sink" + sinkSuffix,
                                 brokerLabel + "/int", data.ints.size());
    std::cout << "  Total consumed from " << brokerLabel << ": " << (vf + vd + vi) << "\n\n";
}

static void truncateAll(ClickHouseWriter& ch) {
    auto t = [&](const std::string& tbl) { ch.truncate(tbl); };
    t("signals_float"); t("signals_double"); t("signals_int");
    for (const auto& suffix : {"_k", "_k_3p", "_k_5p", "_rp", "_rp_3p", "_rp_5p"}) {
        t("signals_float_sink" + std::string(suffix));
        t("signals_double_sink" + std::string(suffix));
        t("signals_int_sink" + std::string(suffix));
    }
}

// Run a block of Kafka/Redpanda phases for a given partition count
static void runBrokerPhases(const std::string& brokerLabel,
                            const std::string& brokerHost,
                            const TestData& data, size_t batchSize,
                            double dataMB, uint64_t totalRec,
                            const std::string& suffix,
                            std::vector<BenchmarkResult>& results,
                            bool enableLatency)
{
    KafkaWriterConfig cfg;
    cfg.brokers = brokerHost;
    cfg.trackLatency = enableLatency;

    // sync+seq (baseline)
    {
        KafkaWriter w(cfg);
        LatencyStats latStats;
        runPhase("PHASE: " + brokerLabel + " sync+seq (" + suffix + ")",
                 [&]() { produceSeq(w, data, batchSize, suffix, false); },
                 "Produced", totalRec, dataMB, results, latStats);
        if (enableLatency && w.getLatencyTracker())
            latStats = w.getLatencyTracker()->computeStats();
        results.back().latency = latStats;
    }

    // async
    {
        KafkaWriterConfig acfg = cfg;
        acfg.asyncFlush = true;
        KafkaWriter w(acfg);
        LatencyStats latStats;
        runPhase("PHASE: " + brokerLabel + " async (" + suffix + ")",
                 [&]() { produceSeq(w, data, batchSize, suffix, true); },
                 "Produced", totalRec, dataMB, results, latStats);
        if (enableLatency && w.getLatencyTracker())
            latStats = w.getLatencyTracker()->computeStats();
        results.back().latency = latStats;
    }

    // parallel (3 producers, sync)
    runPhase("PHASE: " + brokerLabel + " parallel (" + suffix + ")",
             [&]() { produceParallel(cfg, data, batchSize, suffix, false); },
             "Produced", totalRec, dataMB, results);

    // async+parallel (no compression)
    {
        KafkaWriterConfig ocfg = cfg;
        ocfg.asyncFlush = true;
        runPhase("PHASE: " + brokerLabel + " async+parallel (" + suffix + ")",
                 [&]() { produceParallel(ocfg, data, batchSize, suffix, true); },
                 "Produced", totalRec, dataMB, results);
    }

    // async+parallel+compression variants
    struct CompressVar { std::string name; std::string codec; };
    for (const auto& cv : {CompressVar{"zstd", "zstd"}, CompressVar{"lz4", "lz4"},
                           CompressVar{"snappy", "snappy"}, CompressVar{"gzip", "gzip"}}) {
        KafkaWriterConfig ocfg = cfg;
        ocfg.asyncFlush = true;
        ocfg.compression = cv.codec;
        runPhase("PHASE: " + brokerLabel + " async+parallel+" + cv.name + " (" + suffix + ")",
                 [&]() { produceParallel(ocfg, data, batchSize, suffix, true); },
                 "Produced", totalRec, dataMB, results);
    }
}

static std::string suffixForPartitions(int partitions) {
    if (partitions == 1) return "";
    return "-" + std::to_string(partitions) + "p";
}

static std::string sinkSuffixForPartitions(const std::string& brokerPrefix, int partitions) {
    if (partitions == 1) return "_" + brokerPrefix;
    return "_" + brokerPrefix + "_" + std::to_string(partitions) + "p";
}

int main() {
    try {
        auto chHost = getEnv("CLICKHOUSE_HOST", "localhost");
        auto chPort = std::stoi(getEnv("CLICKHOUSE_PORT", "9000"));
        auto kafkaBroker = getEnv("KAFKA_BROKER", "localhost:9092");
        auto redpandaBroker = getEnv("REDPANDA_BROKER", "localhost:9093");
        auto numRecords = std::stoull(getEnv("NUM_RECORDS", "300000"));
        auto batchSize = std::stoull(getEnv("BATCH_SIZE", "10000"));
        auto partitions = std::stoi(getEnv("PARTITIONS", "3"));
        auto iterations = std::stoi(getEnv("ITERATIONS", "1"));
        auto trackLatencyStr = getEnv("TRACK_LATENCY", "1");
        bool trackLatency = trackLatencyStr == "1";

        std::cout << "Configuration:\n"
                  << "  Total records: " << numRecords
                  << " (~" << (numRecords / 3) << " per type)\n"
                  << "  Batch size: " << batchSize << "\n"
                  << "  Partitions: " << partitions << "\n"
                  << "  Iterations: " << iterations << "\n"
                  << "  Track latency: " << (trackLatency ? "yes" : "no") << "\n"
                  << "  ClickHouse: " << chHost << ":" << chPort << "\n"
                  << "  Kafka: " << kafkaBroker << "\n"
                  << "  Redpanda: " << redpandaBroker << "\n\n";

        ClickHouseWriter chWriter(chHost, chPort);
        waitForServices(chWriter, 30);

        truncateAll(chWriter);
        std::cout << "Tables truncated\n";

        std::cout << "Generating " << numRecords << " test records...\n";
        auto data = generateTestData(numRecords);
        auto dataMB = totalDataSizeMB(data);
        uint64_t totalRec = data.totalRecords();
        std::cout << "  float:   " << data.floats.size() << "\n"
                  << "  double:  " << data.doubles.size() << "\n"
                  << "  int:     " << data.ints.size() << "\n"
                  << "Data size: " << std::fixed << std::setprecision(2)
                  << dataMB << " MB\n\n";

        for (int iter = 0; iter < iterations; ++iter) {
            std::vector<BenchmarkResult> results;
            std::string iterLabel = iterations > 1 ? " (iter " + std::to_string(iter + 1) + ")" : "";

            if (iter > 0) {
                // regenerate data for subsequent iterations
                truncateAll(chWriter);
                data = generateTestData(numRecords);
                dataMB = totalDataSizeMB(data);
                totalRec = data.totalRecords();
            }

            // --- Phase 1: ClickHouse native ---
            runPhase("PHASE 1: CH Native Direct Write" + iterLabel,
                     [&]() { chWriter.insert(data, batchSize); },
                     "Inserted", totalRec, dataMB, results);

            // --- Kafka variants ---
            // For partition testing: run with configured partition count
            std::string kSuffix = suffixForPartitions(partitions);
            std::string kSinkSuffix = sinkSuffixForPartitions("k", partitions);

            runBrokerPhases("Kafka " + std::to_string(partitions) + "p",
                           kafkaBroker, data, batchSize, dataMB, totalRec,
                           kSuffix, results, trackLatency);

            verifyBroker(chWriter, data,
                        "Kafka " + std::to_string(partitions) + "p",
                        kSinkSuffix);

            // --- Redpanda variants ---
            std::string rpSuffix = suffixForPartitions(partitions);
            std::string rpSinkSuffix = sinkSuffixForPartitions("rp", partitions);

            runBrokerPhases("Redpanda " + std::to_string(partitions) + "p",
                           redpandaBroker, data, batchSize, dataMB, totalRec,
                           rpSuffix, results, trackLatency);

            verifyBroker(chWriter, data,
                        "Redpanda " + std::to_string(partitions) + "p",
                        rpSinkSuffix);

            // --- Print comparison ---
            printResults(results);

            // --- CSV export ---
            std::string csvPath = "/tmp/benchmark_results" + iterLabel + ".csv";
            std::string jsonPath = "/tmp/benchmark_results" + iterLabel + ".json";
            writeFile(csvPath, resultsCsv(results));
            writeFile(jsonPath, resultsJson(results));
            std::cout << "Results exported to:\n"
                      << "  " << csvPath << "\n"
                      << "  " << jsonPath << "\n";

            // --- Summary ---
            std::cout << "\n=== ClickHouse table sizes ===\n";
            auto printCount = [&](const std::string& name) {
                std::cout << "  " << name << ": " << chWriter.countTable(name) << " rows\n";
            };
            printCount("signals_float");
            printCount("signals_double");
            printCount("signals_int");
            printCount("signals_float_sink" + kSinkSuffix);
            printCount("signals_double_sink" + kSinkSuffix);
            printCount("signals_int_sink" + kSinkSuffix);
            printCount("signals_float_sink" + rpSinkSuffix);
            printCount("signals_double_sink" + rpSinkSuffix);
            printCount("signals_int_sink" + rpSinkSuffix);
        }

    } catch (const std::exception& e) {
        std::cerr << "FATAL: " << e.what() << "\n";
        return 1;
    }
    return 0;
}
