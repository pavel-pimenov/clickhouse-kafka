#include "clickhouse_writer.hpp"
#include <clickhouse/client.h>
#include <clickhouse/columns/string.h>
#include <stdexcept>
#include <iostream>

class ClickHouseWriter::Impl {
public:
    Impl(const std::string& host, int port)
        : client_(clickhouse::ClientOptions()
              .SetHost(host)
              .SetPort(port)
              .SetUser("default")
              .SetPassword("benchmark")
              .SetDefaultDatabase("benchmark"))
    {}

    void insert(const TestData& data, size_t batchSize) {
        insertFloat(data.floats, batchSize);
        insertDouble(data.doubles, batchSize);
        insertInt(data.ints, batchSize);
    }

    bool ping() {
        try {
            clickhouse::Query q("SELECT 1");
            client_.Execute(q);
            return true;
        } catch (const std::exception&) {
            return false;
        }
    }

    void truncateTable(const std::string& table) {
        clickhouse::Query q("TRUNCATE TABLE IF EXISTS benchmark." + table);
        client_.Execute(q);
    }

    uint64_t countTable(const std::string& table) {
        uint64_t count = 0;
        auto query = "SELECT count() FROM benchmark." + table;
        client_.Select(query, clickhouse::SelectCallback(
            [&count](const clickhouse::Block& block) {
                if (block.GetRowCount() > 0) {
                    count = block[0]->As<clickhouse::ColumnUInt64>()->At(0);
                }
            })
        );
        return count;
    }

private:
    clickhouse::Client client_;

    template<typename Rec, typename Col>
    void insertBatch(const std::vector<Rec>& records, size_t i, size_t end,
                     const std::string& table)
    {
        clickhouse::Block block;
        auto colId = std::make_shared<clickhouse::ColumnUInt64>();
        auto colTs = std::make_shared<clickhouse::ColumnDateTime>();
        auto colVal = std::make_shared<Col>();

        for (size_t j = i; j < end; ++j) {
            colId->Append(records[j].signalId);
            colTs->Append(static_cast<time_t>(records[j].time));
        }

        block.AppendColumn("signal_id", colId);
        block.AppendColumn("time", colTs);

        // Fill value in a second pass so block owns the column pointer
        for (size_t j = i; j < end; ++j)
            appendValue(colVal.get(), records[j]);
        block.AppendColumn("value", colVal);

        client_.Insert("benchmark." + table, block);
    }

    void appendValue(clickhouse::ColumnFloat32* col, const FloatRecord& r) {
        col->Append(r.value);
    }
    void appendValue(clickhouse::ColumnFloat64* col, const DoubleRecord& r) {
        col->Append(r.value);
    }
    void appendValue(clickhouse::ColumnInt64* col, const IntRecord& r) {
        col->Append(r.value);
    }

    void insertFloat(const std::vector<FloatRecord>& records, size_t batchSize) {
        for (size_t i = 0; i < records.size(); i += batchSize) {
            auto end = std::min(i + batchSize, records.size());
            insertBatch<FloatRecord, clickhouse::ColumnFloat32>(records, i, end, "signals_float");
        }
    }

    void insertDouble(const std::vector<DoubleRecord>& records, size_t batchSize) {
        for (size_t i = 0; i < records.size(); i += batchSize) {
            auto end = std::min(i + batchSize, records.size());
            insertBatch<DoubleRecord, clickhouse::ColumnFloat64>(records, i, end, "signals_double");
        }
    }

    void insertInt(const std::vector<IntRecord>& records, size_t batchSize) {
        for (size_t i = 0; i < records.size(); i += batchSize) {
            auto end = std::min(i + batchSize, records.size());
            insertBatch<IntRecord, clickhouse::ColumnInt64>(records, i, end, "signals_int");
        }
    }
};

ClickHouseWriter::ClickHouseWriter(const std::string& host, int port)
    : impl_(std::make_unique<Impl>(host, port)) {}

ClickHouseWriter::~ClickHouseWriter() = default;

void ClickHouseWriter::insert(const TestData& data, size_t batchSize) {
    impl_->insert(data, batchSize);
}

bool ClickHouseWriter::ping() {
    return impl_->ping();
}

void ClickHouseWriter::truncate(const std::string& table) {
    impl_->truncateTable(table);
}

uint64_t ClickHouseWriter::countTable(const std::string& table) {
    return impl_->countTable(table);
}
