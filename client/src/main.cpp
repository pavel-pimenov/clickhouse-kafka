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

static void producePhase(KafkaWriter& writer, const TestData& data,
                         size_t batchSize, const std::string& topicSuffix,
                         const std::string& phaseLabel, uint64_t totalRec,
                         double dataMB, std::vector<BenchmarkResult>& results)
{
    runPhase(phaseLabel,
             [&]() {
                 writer.produceFloat(data.floats, batchSize, "signals-float" + topicSuffix);
                 writer.produceDouble(data.doubles, batchSize, "signals-double" + topicSuffix);
                 writer.produceInt(data.ints, batchSize, "signals-int" + topicSuffix);
             },
             "Produced", totalRec, dataMB, results);
}

static void verifyBroker(ClickHouseWriter& ch, const TestData& data,
                         const std::string& brokerLabel, const std::string& sinkSuffix)
{
    std::cout << "=== Verifying " << brokerLabel << " -> ClickHouse consumption ===\n";
    auto vf = verifyConsumption(ch, "signals_float_sink" + sinkSuffix, brokerLabel + "/float", data.floats.size());
    auto vd = verifyConsumption(ch, "signals_double_sink" + sinkSuffix, brokerLabel + "/double", data.doubles.size());
    auto vi = verifyConsumption(ch, "signals_int_sink" + sinkSuffix, brokerLabel + "/int", data.ints.size());
    std::cout << "  Total consumed from " << brokerLabel << ": " << (vf + vd + vi) << "\n\n";
}

int main() {
    try {
        auto chHost = getEnv("CLICKHOUSE_HOST", "localhost");
        auto chPort = std::stoi(getEnv("CLICKHOUSE_PORT", "9000"));
        auto kafkaBroker = getEnv("KAFKA_BROKER", "localhost:9092");
        auto redpandaBroker = getEnv("REDPANDA_BROKER", "localhost:9093");
        auto numRecords = std::stoull(getEnv("NUM_RECORDS", "300000"));
        auto batchSize = std::stoull(getEnv("BATCH_SIZE", "1000"));

        std::cout << "Configuration:\n"
                  << "  Total records: " << numRecords
                  << " (~" << (numRecords / 3) << " per type)\n"
                  << "  Batch size: " << batchSize << "\n"
                  << "  ClickHouse: " << chHost << ":" << chPort << "\n"
                  << "  Kafka: " << kafkaBroker << "\n"
                  << "  Redpanda: " << redpandaBroker << "\n\n";

        ClickHouseWriter chWriter(chHost, chPort);
        waitForServices(chWriter, 30);

        // Truncate all tables from previous runs
        auto trunc = [&](const std::string& t) { chWriter.truncate(t); };
        trunc("signals_float"); trunc("signals_double"); trunc("signals_int");
        trunc("signals_float_sink_k"); trunc("signals_double_sink_k"); trunc("signals_int_sink_k");
        trunc("signals_float_sink_k_3p"); trunc("signals_double_sink_k_3p"); trunc("signals_int_sink_k_3p");
        trunc("signals_float_sink_k_5p"); trunc("signals_double_sink_k_5p"); trunc("signals_int_sink_k_5p");
        trunc("signals_float_sink_rp"); trunc("signals_double_sink_rp"); trunc("signals_int_sink_rp");
        trunc("signals_float_sink_rp_3p"); trunc("signals_double_sink_rp_3p"); trunc("signals_int_sink_rp_3p");
        trunc("signals_float_sink_rp_5p"); trunc("signals_double_sink_rp_5p"); trunc("signals_int_sink_rp_5p");
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

        // --- Phase 1: ClickHouse native ---
        runPhase("PHASE 1: ClickHouse Native Direct Write",
                 [&]() { chWriter.insert(data, batchSize); },
                 "Inserted", totalRec, dataMB, results);

        // --- Kafka Phases ---
        {
        KafkaWriter kw(kafkaBroker);
        producePhase(kw, data, batchSize, "",        "PHASE 2: Kafka 1p",  totalRec, dataMB, results);
        }
        verifyBroker(chWriter, data, "Kafka 1p", "_k");

        {
        KafkaWriter kw(kafkaBroker);
        producePhase(kw, data, batchSize, "-3p",     "PHASE 3: Kafka 3p",  totalRec, dataMB, results);
        }
        verifyBroker(chWriter, data, "Kafka 3p", "_k_3p");

        {
        KafkaWriter kw(kafkaBroker);
        producePhase(kw, data, batchSize, "-5p",     "PHASE 4: Kafka 5p",  totalRec, dataMB, results);
        }
        verifyBroker(chWriter, data, "Kafka 5p", "_k_5p");

        // --- Redpanda Phases ---
        {
        KafkaWriter rw(redpandaBroker);
        producePhase(rw, data, batchSize, "",        "PHASE 5: Redpanda 1p", totalRec, dataMB, results);
        }
        verifyBroker(chWriter, data, "Redpanda 1p", "_rp");

        {
        KafkaWriter rw(redpandaBroker);
        producePhase(rw, data, batchSize, "-3p",     "PHASE 6: Redpanda 3p", totalRec, dataMB, results);
        }
        verifyBroker(chWriter, data, "Redpanda 3p", "_rp_3p");

        {
        KafkaWriter rw(redpandaBroker);
        producePhase(rw, data, batchSize, "-5p",     "PHASE 7: Redpanda 5p", totalRec, dataMB, results);
        }
        verifyBroker(chWriter, data, "Redpanda 5p", "_rp_5p");

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
        printCount("signals_float_sink_k");
        printCount("signals_double_sink_k");
        printCount("signals_int_sink_k");
        printCount("signals_float_sink_k_3p");
        printCount("signals_double_sink_k_3p");
        printCount("signals_int_sink_k_3p");
        printCount("signals_float_sink_k_5p");
        printCount("signals_double_sink_k_5p");
        printCount("signals_int_sink_k_5p");
        printCount("signals_float_sink_rp");
        printCount("signals_double_sink_rp");
        printCount("signals_int_sink_rp");
        printCount("signals_float_sink_rp_3p");
        printCount("signals_double_sink_rp_3p");
        printCount("signals_int_sink_rp_3p");
        printCount("signals_float_sink_rp_5p");
        printCount("signals_double_sink_rp_5p");
        printCount("signals_int_sink_rp_5p");

    } catch (const std::exception& e) {
        std::cerr << "FATAL: " << e.what() << "\n";
        return 1;
    }
    return 0;
}
