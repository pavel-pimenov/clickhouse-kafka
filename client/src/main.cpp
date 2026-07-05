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
                     std::vector<BenchmarkResult>& results)
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
         after.currentRssKb}
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

int main() {
    try {
        auto chHost = getEnv("CLICKHOUSE_HOST", "localhost");
        auto chPort = std::stoi(getEnv("CLICKHOUSE_PORT", "9000"));
        auto kafkaBroker = getEnv("KAFKA_BROKER", "localhost:9092");
        auto redpandaBroker = getEnv("REDPANDA_BROKER", "localhost:9093");
        auto numRecords = std::stoull(getEnv("NUM_RECORDS", "300000"));
        auto batchSize = std::stoull(getEnv("BATCH_SIZE", "10000"));

        std::cout << "Configuration:\n"
                  << "  Total records: " << numRecords
                  << " (~" << (numRecords / 3) << " per type)\n"
                  << "  Batch size: " << batchSize << "\n"
                  << "  ClickHouse: " << chHost << ":" << chPort << "\n"
                  << "  Kafka: " << kafkaBroker << "\n"
                  << "  Redpanda: " << redpandaBroker << "\n\n";

        ClickHouseWriter chWriter(chHost, chPort);
        waitForServices(chWriter, 30);

        // Truncate all tables
        auto trunc = [&](const std::string& t) { chWriter.truncate(t); };
        trunc("signals_float"); trunc("signals_double"); trunc("signals_int");
        trunc("signals_float_sink_k_3p");
        trunc("signals_double_sink_k_3p");
        trunc("signals_int_sink_k_3p");
        trunc("signals_float_sink_rp_3p");
        trunc("signals_double_sink_rp_3p");
        trunc("signals_int_sink_rp_3p");
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

        std::vector<BenchmarkResult> results;
        const std::string suffix = "-3p";

        // --- Phase 1: ClickHouse native ---
        runPhase("PHASE 1: ClickHouse Native Direct Write",
                 [&]() { chWriter.insert(data, batchSize); },
                 "Inserted", totalRec, dataMB, results);

        // === Kafka 3p variants ===
        KafkaWriterConfig kCfg;
        kCfg.brokers = kafkaBroker;

        // Phase 2: sync + sequential (baseline)
        {
        KafkaWriter w(kCfg);
        runPhase("PHASE 2: Kafka 3p sync+seq",
                 [&]() { produceSeq(w, data, batchSize, suffix, false); },
                 "Produced", totalRec, dataMB, results);
        }

        // Phase 3: async (no flush per batch)
        {
        KafkaWriterConfig acfg = kCfg;
        acfg.asyncFlush = true;
        KafkaWriter w(acfg);
        runPhase("PHASE 3: Kafka 3p async",
                 [&]() { produceSeq(w, data, batchSize, suffix, true); },
                 "Produced", totalRec, dataMB, results);
        }

        // Phase 4: parallel (3 producers, sync)
        runPhase("PHASE 4: Kafka 3p parallel",
                 [&]() { produceParallel(kCfg, data, batchSize, suffix, false); },
                 "Produced", totalRec, dataMB, results);

        // Phase 5: async+parallel (no compression)
        {
        KafkaWriterConfig ocfg = kCfg;
        ocfg.asyncFlush = true;
        runPhase("PHASE 5: Kafka 3p async+parallel",
                 [&]() { produceParallel(ocfg, data, batchSize, suffix, true); },
                 "Produced", totalRec, dataMB, results);
        }

        // Phase 5z: async+parallel+compression+zstd
        {
        KafkaWriterConfig ocfg = kCfg;
        ocfg.asyncFlush = true;
        ocfg.compression = "zstd";
        runPhase("PHASE 5z: Kafka 3p async+parallel+zstd",
                 [&]() { produceParallel(ocfg, data, batchSize, suffix, true); },
                 "Produced", totalRec, dataMB, results);
        }

        // Phase 5b: async+parallel+compression+lz4
        {
        KafkaWriterConfig ocfg = kCfg;
        ocfg.asyncFlush = true;
        ocfg.compression = "lz4";
        runPhase("PHASE 5b: Kafka 3p async+parallel+lz4",
                 [&]() { produceParallel(ocfg, data, batchSize, suffix, true); },
                 "Produced", totalRec, dataMB, results);
        }

        // Phase 5c: async+parallel+snappy
        {
        KafkaWriterConfig ocfg = kCfg;
        ocfg.asyncFlush = true;
        ocfg.compression = "snappy";
        runPhase("PHASE 5c: Kafka 3p async+parallel+snappy",
                 [&]() { produceParallel(ocfg, data, batchSize, suffix, true); },
                 "Produced", totalRec, dataMB, results);
        }

        // Phase 5d: async+parallel+gzip
        {
        KafkaWriterConfig ocfg = kCfg;
        ocfg.asyncFlush = true;
        ocfg.compression = "gzip";
        runPhase("PHASE 5d: Kafka 3p async+parallel+gzip",
                 [&]() { produceParallel(ocfg, data, batchSize, suffix, true); },
                 "Produced", totalRec, dataMB, results);
        }

        // Verify Kafka batch
        verifyBroker(chWriter, data, "Kafka 3p", "_k_3p");

        // === Redpanda 3p variants ===
        KafkaWriterConfig rpCfg;
        rpCfg.brokers = redpandaBroker;

        // Phase 6: sync+seq
        {
        KafkaWriter w(rpCfg);
        runPhase("PHASE 6: Redpanda 3p sync+seq",
                 [&]() { produceSeq(w, data, batchSize, suffix, false); },
                 "Produced", totalRec, dataMB, results);
        }

        // Phase 7: async
        {
        KafkaWriterConfig acfg = rpCfg;
        acfg.asyncFlush = true;
        KafkaWriter w(acfg);
        runPhase("PHASE 7: Redpanda 3p async",
                 [&]() { produceSeq(w, data, batchSize, suffix, true); },
                 "Produced", totalRec, dataMB, results);
        }

        // Phase 8: parallel
        runPhase("PHASE 8: Redpanda 3p parallel",
                 [&]() { produceParallel(rpCfg, data, batchSize, suffix, false); },
                 "Produced", totalRec, dataMB, results);

        // Phase 9: async+parallel (no compression)
        {
        KafkaWriterConfig ocfg = rpCfg;
        ocfg.asyncFlush = true;
        runPhase("PHASE 9: Redpanda 3p async+parallel",
                 [&]() { produceParallel(ocfg, data, batchSize, suffix, true); },
                 "Produced", totalRec, dataMB, results);
        }

        // Phase 9z: async+parallel+zstd
        {
        KafkaWriterConfig ocfg = rpCfg;
        ocfg.asyncFlush = true;
        ocfg.compression = "zstd";
        runPhase("PHASE 9z: Redpanda 3p async+parallel+zstd",
                 [&]() { produceParallel(ocfg, data, batchSize, suffix, true); },
                 "Produced", totalRec, dataMB, results);
        }

        // Phase 9b: async+parallel+lz4
        {
        KafkaWriterConfig ocfg = rpCfg;
        ocfg.asyncFlush = true;
        ocfg.compression = "lz4";
        runPhase("PHASE 9b: Redpanda 3p async+parallel+lz4",
                 [&]() { produceParallel(ocfg, data, batchSize, suffix, true); },
                 "Produced", totalRec, dataMB, results);
        }

        // Phase 9c: async+parallel+snappy
        {
        KafkaWriterConfig ocfg = rpCfg;
        ocfg.asyncFlush = true;
        ocfg.compression = "snappy";
        runPhase("PHASE 9c: Redpanda 3p async+parallel+snappy",
                 [&]() { produceParallel(ocfg, data, batchSize, suffix, true); },
                 "Produced", totalRec, dataMB, results);
        }

        // Phase 9d: async+parallel+gzip
        {
        KafkaWriterConfig ocfg = rpCfg;
        ocfg.asyncFlush = true;
        ocfg.compression = "gzip";
        runPhase("PHASE 9d: Redpanda 3p async+parallel+gzip",
                 [&]() { produceParallel(ocfg, data, batchSize, suffix, true); },
                 "Produced", totalRec, dataMB, results);
        }

        // Verify Redpanda batch
        verifyBroker(chWriter, data, "Redpanda 3p", "_rp_3p");

        // --- Print comparison ---
        printResults(results);

        // --- Summary ---
        std::cout << "\n=== ClickHouse table sizes ===\n";
        auto printCount = [&](const std::string& name) {
            std::cout << "  " << name << ": " << chWriter.countTable(name) << " rows\n";
        };
        printCount("signals_float");
        printCount("signals_double");
        printCount("signals_int");
        printCount("signals_float_sink_k_3p");
        printCount("signals_double_sink_k_3p");
        printCount("signals_int_sink_k_3p");
        printCount("signals_float_sink_rp_3p");
        printCount("signals_double_sink_rp_3p");
        printCount("signals_int_sink_rp_3p");

    } catch (const std::exception& e) {
        std::cerr << "FATAL: " << e.what() << "\n";
        return 1;
    }
    return 0;
}
