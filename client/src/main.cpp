#include "record.hpp"
#include "clickhouse_writer.hpp"
#include "kafka_writer.hpp"
#include "utils.hpp"

#include <iostream>
#include <thread>
#include <chrono>
#include <random>
#include <algorithm>
#include <iomanip>
#include <sstream>

std::vector<Record> generateTestData(uint64_t count, size_t messageSize) {
    std::vector<Record> data;
    data.reserve(count);

    std::mt19937 rng(42);
    std::uniform_real_distribution<double> valDist(0.0, 1000000.0);
    static const char alnum[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz"
        "0123456789 ";
    std::uniform_int_distribution<int> charDist(0, sizeof(alnum) - 2);

    auto now = static_cast<uint64_t>(std::time(nullptr));

    for (uint64_t i = 0; i < count; ++i) {
        std::string msg(messageSize, ' ');
        for (size_t j = 0; j < messageSize; ++j) {
            msg[j] = alnum[charDist(rng)];
        }
        data.push_back(Record{
            i,
            now + i / 1000,
            valDist(rng),
            std::move(msg)
        });
    }
    return data;
}

double totalDataSizeMB(const std::vector<Record>& data) {
    double bytes = 0;
    for (const auto& r : data) {
        bytes += sizeof(r.id) + sizeof(r.timestamp) + sizeof(r.value) + r.message.size();
    }
    return bytes / (1024.0 * 1024.0);
}

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

static void verifyConsumption(ClickHouseWriter& ch, const std::string& table,
                               const std::string& label, uint64_t expected) {
    std::cout << "=== Verifying " << label << " -> ClickHouse consumption ===\n";
    for (int i = 0; i < 60; ++i) {
        auto count = ch.countTable(table);
        if (count >= expected) {
            std::cout << "  All " << count << " records consumed\n\n";
            return;
        }
        std::this_thread::sleep_for(std::chrono::seconds(1));
        if (i == 59) {
            std::cout << "  WARNING: Only " << count << "/" << expected
                      << " consumed after 60s\n\n";
        }
    }
}

int main() {
    try {
        auto chHost = getEnv("CLICKHOUSE_HOST", "localhost");
        auto chPort = std::stoi(getEnv("CLICKHOUSE_PORT", "9000"));
        auto kafkaBroker = getEnv("KAFKA_BROKER", "localhost:9092");
        auto redpandaBroker = getEnv("REDPANDA_BROKER", "localhost:9093");
        auto numRecords = std::stoull(getEnv("NUM_RECORDS", "100000"));
        auto batchSize = std::stoull(getEnv("BATCH_SIZE", "1000"));
        auto msgSize = std::stoull(getEnv("MESSAGE_SIZE", "100"));

        std::cout << "Configuration:\n"
                  << "  Records: " << numRecords << "\n"
                  << "  Batch size: " << batchSize << "\n"
                  << "  Message size: " << msgSize << " B\n"
                  << "  ClickHouse: " << chHost << ":" << chPort << "\n"
                  << "  Kafka: " << kafkaBroker << "\n"
                  << "  Redpanda: " << redpandaBroker << "\n\n";

        ClickHouseWriter chWriter(chHost, chPort);
        waitForServices(chWriter, 30);

        std::cout << "Generating " << numRecords << " test records...\n";
        auto data = generateTestData(numRecords, msgSize);
        auto dataMB = totalDataSizeMB(data);
        std::cout << "Data size: " << std::fixed << std::setprecision(2)
                  << dataMB << " MB\n\n";

        std::vector<BenchmarkResult> results;

        // --- Phase 1: ClickHouse native ---
        std::cout << "=== PHASE 1: ClickHouse Native Direct Write ===\n";
        auto before = getResourceUsage();
        auto t0 = nowSec();
        chWriter.insert(data, batchSize);
        auto t1 = nowSec();
        auto after = getResourceUsage();
        double elapsed = t1 - t0;
        auto chCount = chWriter.countTable("direct");
        std::cout << "  Inserted " << chCount << " records\n"
                  << "  Time: " << std::fixed << std::setprecision(2) << elapsed << " s\n"
                  << "  Throughput: " << std::fixed << std::setprecision(1)
                  << (numRecords / elapsed) << " rec/s, "
                  << (dataMB / elapsed) << " MB/s\n\n";

        results.push_back({
            "ClickHouse Native",
            elapsed,
            chCount,
            numRecords / elapsed,
            dataMB / elapsed,
            {after.cpuUserSec - before.cpuUserSec,
             after.cpuSysSec - before.cpuSysSec,
             after.peakRssKb}
        });

        // --- Phase 2: Kafka Produce ---
        std::cout << "=== PHASE 2: Kafka Produce ===\n";
        KafkaWriter kafkaWriter(kafkaBroker, "benchmark");

        before = getResourceUsage();
        t0 = nowSec();
        kafkaWriter.produce(data, batchSize);
        t1 = nowSec();
        after = getResourceUsage();
        elapsed = t1 - t0;
        std::cout << "  Produced " << numRecords << " records to Kafka\n"
                  << "  Time: " << std::fixed << std::setprecision(2) << elapsed << " s\n"
                  << "  Throughput: " << std::fixed << std::setprecision(1)
                  << (numRecords / elapsed) << " rec/s, "
                  << (dataMB / elapsed) << " MB/s\n\n";

        results.push_back({
            "Kafka Produce",
            elapsed,
            numRecords,
            numRecords / elapsed,
            dataMB / elapsed,
            {after.cpuUserSec - before.cpuUserSec,
             after.cpuSysSec - before.cpuSysSec,
             after.peakRssKb}
        });

        verifyConsumption(chWriter, "kafka_sink", "Kafka", numRecords);

        // --- Phase 3: Redpanda Produce ---
        std::cout << "=== PHASE 3: Redpanda Produce ===\n";
        KafkaWriter redpandaWriter(redpandaBroker, "benchmark");

        before = getResourceUsage();
        t0 = nowSec();
        redpandaWriter.produce(data, batchSize);
        t1 = nowSec();
        after = getResourceUsage();
        elapsed = t1 - t0;
        std::cout << "  Produced " << numRecords << " records to Redpanda\n"
                  << "  Time: " << std::fixed << std::setprecision(2) << elapsed << " s\n"
                  << "  Throughput: " << std::fixed << std::setprecision(1)
                  << (numRecords / elapsed) << " rec/s, "
                  << (dataMB / elapsed) << " MB/s\n\n";

        results.push_back({
            "Redpanda Produce",
            elapsed,
            numRecords,
            numRecords / elapsed,
            dataMB / elapsed,
            {after.cpuUserSec - before.cpuUserSec,
             after.cpuSysSec - before.cpuSysSec,
             after.peakRssKb}
        });

        verifyConsumption(chWriter, "redpanda_sink", "Redpanda", numRecords);

        // --- Print comparison ---
        printResults(results);

        // --- Disk info ---
        std::cout << "\n=== ClickHouse table sizes ===\n";
        auto chDirectBytes = chWriter.countTable("direct");
        auto chKafkaBytes = chWriter.countTable("kafka_sink");
        auto chRedpandaBytes = chWriter.countTable("redpanda_sink");
        std::cout << "  benchmark.direct:        " << chDirectBytes << " rows\n";
        std::cout << "  benchmark.kafka_sink:    " << chKafkaBytes << " rows\n";
        std::cout << "  benchmark.redpanda_sink: " << chRedpandaBytes << " rows\n";

    } catch (const std::exception& e) {
        std::cerr << "FATAL: " << e.what() << "\n";
        return 1;
    }
    return 0;
}
