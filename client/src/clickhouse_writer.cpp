#include "clickhouse_writer.hpp"
#include <clickhouse/client.h>
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

    void insert(const std::vector<Record>& records, size_t batchSize) {
        size_t i = 0;
        while (i < records.size()) {
            size_t end = std::min(i + batchSize, records.size());

            clickhouse::Block block;
            auto colId = std::make_shared<clickhouse::ColumnUInt64>();
            auto colTs = std::make_shared<clickhouse::ColumnDateTime>();
            auto colVal = std::make_shared<clickhouse::ColumnFloat64>();
            auto colMsg = std::make_shared<clickhouse::ColumnString>();

            for (size_t j = i; j < end; ++j) {
                const auto& r = records[j];
                colId->Append(r.id);
                colTs->Append(static_cast<time_t>(r.timestamp));
                colVal->Append(r.value);
                colMsg->Append(r.message);
            }

            block.AppendColumn("id", colId);
            block.AppendColumn("timestamp", colTs);
            block.AppendColumn("value", colVal);
            block.AppendColumn("message", colMsg);

            client_.Insert("benchmark.direct", block);
            i = end;
        }
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
};

ClickHouseWriter::ClickHouseWriter(const std::string& host, int port)
    : impl_(std::make_unique<Impl>(host, port)) {}

ClickHouseWriter::~ClickHouseWriter() = default;

void ClickHouseWriter::insert(const std::vector<Record>& records, size_t batchSize) {
    impl_->insert(records, batchSize);
}

bool ClickHouseWriter::ping() {
    return impl_->ping();
}

uint64_t ClickHouseWriter::countTable(const std::string& table) {
    return impl_->countTable(table);
}
