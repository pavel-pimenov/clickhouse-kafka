#include "record.hpp"

#include <random>
#include <chrono>
#include <ctime>
#include <cstdint>
#include <limits>

TestData generateTestData(uint64_t totalCount) {
    TestData data;

    uint64_t cnt = totalCount / 3;
    if (cnt == 0) cnt = 1;

    data.floats.reserve(cnt);
    data.doubles.reserve(cnt);
    data.ints.reserve(cnt);

    std::mt19937 rng(42);
    std::uniform_real_distribution<float> floatDist(-1e6f, 1e6f);
    std::uniform_real_distribution<double> doubleDist(-1e12, 1e12);
    std::uniform_int_distribution<int64_t> intDist(
        std::numeric_limits<int64_t>::min() / 2,
        std::numeric_limits<int64_t>::max() / 2);

    auto now = static_cast<uint64_t>(std::time(nullptr));
    uint64_t idx = 0;

    auto fill = [&](auto& vec, auto& dist, auto toVal) {
        for (uint64_t i = 0; i < cnt; ++i) {
            vec.push_back({
                idx % 1000,
                now + idx / 1000,
                static_cast<decltype(vec[0].value)>(toVal(dist, rng))
            });
            ++idx;
        }
    };

    fill(data.floats, floatDist, [](auto& d, auto& g) { return d(g); });
    fill(data.doubles, doubleDist, [](auto& d, auto& g) { return d(g); });
    fill(data.ints, intDist, [](auto& d, auto& g) { return d(g); });

    return data;
}

double totalDataSizeMB(const TestData& data) {
    double bytes = 0;
    bytes += data.floats.size() * (sizeof(uint64_t) + sizeof(uint64_t) + sizeof(float));
    bytes += data.doubles.size() * (sizeof(uint64_t) + sizeof(uint64_t) + sizeof(double));
    bytes += data.ints.size() * (sizeof(uint64_t) + sizeof(uint64_t) + sizeof(int64_t));
    return bytes / (1024.0 * 1024.0);
}
