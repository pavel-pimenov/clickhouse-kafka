#pragma once

#include <string>
#include <vector>
#include <memory>
#include "record.hpp"

struct KafkaWriterConfig {
    std::string brokers;
    std::string compression = "";
    bool asyncFlush = false;
};

class KafkaWriter {
public:
    explicit KafkaWriter(const KafkaWriterConfig& config);
    ~KafkaWriter();

    void produceFloat(const std::vector<FloatRecord>& records, size_t batchSize, const std::string& topic);
    void produceDouble(const std::vector<DoubleRecord>& records, size_t batchSize, const std::string& topic);
    void produceInt(const std::vector<IntRecord>& records, size_t batchSize, const std::string& topic);
    void flush();

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};
