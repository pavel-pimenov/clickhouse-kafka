#pragma once

#include <cstdint>
#include <string>
#include <vector>

struct FloatRecord {
    uint64_t signalId;
    uint64_t time;
    float value;
};

struct DoubleRecord {
    uint64_t signalId;
    uint64_t time;
    double value;
};

struct IntRecord {
    uint64_t signalId;
    uint64_t time;
    int64_t value;
};

struct TestData {
    std::vector<FloatRecord> floats;
    std::vector<DoubleRecord> doubles;
    std::vector<IntRecord> ints;

    uint64_t totalRecords() const {
        return floats.size() + doubles.size() + ints.size();
    }
};

TestData generateTestData(uint64_t totalCount);
double totalDataSizeMB(const TestData& data);

