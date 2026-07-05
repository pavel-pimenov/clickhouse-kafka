#pragma once

#include <string>
#include <vector>
#include <memory>
#include "record.hpp"

class KafkaWriter {
public:
    KafkaWriter(const std::string& brokers, const std::string& topic);
    ~KafkaWriter();

    void produce(const std::vector<Record>& records, size_t batchSize);

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};
