#pragma once

#include <string>
#include <vector>
#include <memory>
#include "record.hpp"

class ClickHouseWriter {
public:
    ClickHouseWriter(const std::string& host, int port);
    ~ClickHouseWriter();

    void insert(const std::vector<Record>& records, size_t batchSize);
    bool ping();
    uint64_t countTable(const std::string& table);

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};
