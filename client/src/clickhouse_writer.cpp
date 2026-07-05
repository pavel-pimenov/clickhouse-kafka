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
        insertTyped(data.floats,   batchSize, "signals_float",   &Impl::appendFloat);
        insertTyped(data.doubles,  batchSize, "signals_double",  &Impl::appendDouble);
        insertTyped(data.ints,     batchSize, "signals_int",     &Impl::appendInt);
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
    void insertTyped(const std::vector<Rec>& records, size_t batchSize,
                     const std::string& table,
                     void (Impl::*append)(clickhouse::Block&, const Rec&))
    {
        size_t i = 0;
        while (i < records.size()) {
            size_t end = std::min(i + batchSize, records.size());

            clickhouse::Block block;
            block.AppendColumn("signal_id", std::make_shared<clickhouse::ColumnUInt64>());
            block.AppendColumn("time", std::make_shared<clickhouse::ColumnDateTime>());
            block.AppendColumn("value", std::make_shared<Col>());

            for (size_t j = i; j < end; ++j) {
                block[0]->As<clickhouse::ColumnUInt64>()->Append(records[j].signalId);
                block[1]->As<clickhouse::ColumnDateTime>()->Append(static_cast<time_t>(records[j].time));
            }

            // Fill value column using the type-specific appender
            auto* valueCol = block[2].get();
            for (size_t j = i; j < end; ++j) {
                appendValue(valueCol, records[j]);
            }

            client_.Insert("benchmark." + table, block);
            i = end;
        }
    }

    void appendValue(clickhouse::Column* col, const FloatRecord& r) {
        col->As<clickhouse::ColumnFloat32>()->Append(r.value);
    }

    void appendValue(clickhouse::Column* col, const DoubleRecord& r) {
        col->As<clickhouse::ColumnFloat64>()->Append(r.value);
    }

    void appendValue(clickhouse::Column* col, const IntRecord& r) {
        col->As<clickhouse::ColumnInt64>()->Append(r.value);
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

uint64_t ClickHouseWriter::countTable(const std::string& table) {
    return impl_->countTable(table);
}
