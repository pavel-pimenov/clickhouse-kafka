#pragma once

#include <string>
#include <vector>
#include <memory>
#include "record.hpp"

class KafkaWriter {
public:
    KafkaWriter(const std::string& brokers);
    ~KafkaWriter();

    void produceFloat(const std::vector<FloatRecord>& records, size_t batchSize, const std::string& topic);
    void produceDouble(const std::vector<DoubleRecord>& records, size_t batchSize, const std::string& topic);
    void produceInt(const std::vector<IntRecord>& records, size_t batchSize, const std::string& topic);

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};
