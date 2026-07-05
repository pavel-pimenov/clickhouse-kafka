#include "kafka_writer.hpp"
#include <librdkafka/rdkafkacpp.h>
#include <sstream>
#include <stdexcept>
#include <iostream>

static std::string recordToJson(const Record& r) {
    std::ostringstream os;
    os << "{\"id\":" << r.id
       << ",\"timestamp\":" << r.timestamp
       << ",\"value\":" << r.value
       << ",\"message\":\"" << r.message << "\"}";
    return os.str();
}

class KafkaWriter::Impl {
public:
    Impl(const std::string& brokers, const std::string& topic)
        : topic_(topic)
    {
        std::string errstr;

        RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
        if (!conf)
            throw std::runtime_error("failed to create kafka config");

        auto conf_cleanup = [](RdKafka::Conf* c) { delete c; };

        if (conf->set("bootstrap.servers", brokers, errstr) != RdKafka::Conf::CONF_OK) {
            throw std::runtime_error("kafka conf error: " + errstr);
        }
        if (conf->set("message.timeout.ms", "30000", errstr) != RdKafka::Conf::CONF_OK) {
            throw std::runtime_error("kafka conf error: " + errstr);
        }
        if (conf->set("queue.buffering.max.messages", "100000", errstr) != RdKafka::Conf::CONF_OK) {
            throw std::runtime_error("kafka conf error: " + errstr);
        }
        if (conf->set("batch.num.messages", "10000", errstr) != RdKafka::Conf::CONF_OK) {
            throw std::runtime_error("kafka conf error: " + errstr);
        }

        producer_.reset(RdKafka::Producer::create(conf, errstr));
        delete conf;

        if (!producer_) {
            throw std::runtime_error("failed to create kafka producer: " + errstr);
        }
    }

    ~Impl() {
        if (producer_) {
            producer_->flush(10000);
        }
    }

    void produce(const std::vector<Record>& records, size_t batchSize) {
        size_t i = 0;
        while (i < records.size()) {
            size_t end = std::min(i + batchSize, records.size());

            std::string batch;
            for (size_t j = i; j < end; ++j) {
                if (j > i) batch += '\n';
                batch += recordToJson(records[j]);
            }

            RdKafka::ErrorCode err = producer_->produce(
                topic_,
                RdKafka::Topic::PARTITION_UA,
                RdKafka::Producer::RK_MSG_COPY,
                const_cast<char*>(batch.data()),
                batch.size(),
                nullptr, 0,
                0, nullptr, nullptr);

            if (err != RdKafka::ERR_NO_ERROR) {
                producer_->poll(100);
                err = producer_->produce(
                    topic_,
                    RdKafka::Topic::PARTITION_UA,
                    RdKafka::Producer::RK_MSG_COPY,
                    const_cast<char*>(batch.data()),
                    batch.size(),
                    nullptr, 0,
                    0, nullptr, nullptr);
                if (err != RdKafka::ERR_NO_ERROR) {
                    throw std::runtime_error("kafka produce error: " +
                        RdKafka::err2str(err));
                }
            }

            producer_->flush(5000);
            i = end;
        }
    }

private:
    std::string topic_;
    std::unique_ptr<RdKafka::Producer> producer_;
};

KafkaWriter::KafkaWriter(const std::string& brokers, const std::string& topic)
    : impl_(std::make_unique<Impl>(brokers, topic)) {}

KafkaWriter::~KafkaWriter() = default;

void KafkaWriter::produce(const std::vector<Record>& records, size_t batchSize) {
    impl_->produce(records, batchSize);
}
