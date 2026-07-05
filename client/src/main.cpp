#include "record.hpp"
#include "clickhouse_writer.hpp"
#include "kafka_writer.hpp"
#include "utils.hpp"

#include <iostream>
#include <thread>
#include <chrono>
#include <iomanip>
#include <sstream>

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
         after.peakRssKb}
    });
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

        // --- Phase 2: Kafka Produce ---
        KafkaWriter kafkaWriter(kafkaBroker);
        runPhase("PHASE 2: Kafka Produce",
                 [&]() {
                     kafkaWriter.produceFloat(data.floats, batchSize, "signals-float");
                     kafkaWriter.produceDouble(data.doubles, batchSize, "signals-double");
                     kafkaWriter.produceInt(data.ints, batchSize, "signals-int");
                 },
                 "Produced", totalRec, dataMB, results);

        std::cout << "=== Verifying Kafka -> ClickHouse consumption ===\n";
        uint64_t kFloat = verifyConsumption(chWriter, "signals_float_sink_k", "Kafka/float", data.floats.size());
        uint64_t kDouble = verifyConsumption(chWriter, "signals_double_sink_k", "Kafka/double", data.doubles.size());
        uint64_t kInt = verifyConsumption(chWriter, "signals_int_sink_k", "Kafka/int", data.ints.size());
        std::cout << "  Total consumed from Kafka: " << (kFloat + kDouble + kInt) << "\n\n";

        // --- Phase 3: Redpanda Produce ---
        KafkaWriter redpandaWriter(redpandaBroker);
        runPhase("PHASE 3: Redpanda Produce",
                 [&]() {
                     redpandaWriter.produceFloat(data.floats, batchSize, "signals-float");
                     redpandaWriter.produceDouble(data.doubles, batchSize, "signals-double");
                     redpandaWriter.produceInt(data.ints, batchSize, "signals-int");
                 },
                 "Produced", totalRec, dataMB, results);

        std::cout << "=== Verifying Redpanda -> ClickHouse consumption ===\n";
        uint64_t rFloat = verifyConsumption(chWriter, "signals_float_sink_rp", "Redpanda/float", data.floats.size());
        uint64_t rDouble = verifyConsumption(chWriter, "signals_double_sink_rp", "Redpanda/double", data.doubles.size());
        uint64_t rInt = verifyConsumption(chWriter, "signals_int_sink_rp", "Redpanda/int", data.ints.size());
        std::cout << "  Total consumed from Redpanda: " << (rFloat + rDouble + rInt) << "\n\n";

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
        printCount("signals_float_sink_rp");
        printCount("signals_double_sink_rp");
        printCount("signals_int_sink_rp");

    } catch (const std::exception& e) {
        std::cerr << "FATAL: " << e.what() << "\n";
        return 1;
    }
    return 0;
}
