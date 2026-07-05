#pragma once

#include <cstdint>
#include <string>
#include <vector>

struct Record {
    uint64_t id;
    uint64_t timestamp;
    double value;
    std::string message;
};

std::vector<Record> generateTestData(uint64_t count, size_t messageSize);
double totalDataSizeMB(const std::vector<Record>& data);
