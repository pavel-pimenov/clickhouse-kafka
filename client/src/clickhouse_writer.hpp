#pragma once

#include <string>
#include <vector>
#include <memory>
#include "record.hpp"

class ClickHouseWriter {
public:
    ClickHouseWriter(const std::string& host, int port);
    ~ClickHouseWriter();

    void insert(const TestData& data, size_t batchSize);
    bool ping();
    void truncate(const std::string& table);
    uint64_t countTable(const std::string& table);

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};
