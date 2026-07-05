#include "kafka_writer.hpp"
#include <librdkafka/rdkafkacpp.h>
#include <sstream>
#include <stdexcept>
#include <iostream>
#include <iomanip>

static std::string floatToJson(const FloatRecord& r) {
    std::ostringstream os;
    os << "{\"signal_id\":" << r.signalId
       << ",\"time\":" << r.time
       << ",\"value\":" << std::setprecision(8) << r.value << "}";
    return os.str();
}

static std::string doubleToJson(const DoubleRecord& r) {
    std::ostringstream os;
    os << "{\"signal_id\":" << r.signalId
       << ",\"time\":" << r.time
       << ",\"value\":" << std::setprecision(16) << r.value << "}";
    return os.str();
}

static std::string intToJson(const IntRecord& r) {
    std::ostringstream os;
    os << "{\"signal_id\":" << r.signalId
       << ",\"time\":" << r.time
       << ",\"value\":" << r.value << "}";
    return os.str();
}

class KafkaWriter::Impl {
public:
    Impl(const std::string& brokers)
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

    template<typename Rec, typename ToJson>
    void produce(const std::vector<Rec>& records, size_t batchSize,
                 const std::string& topic, ToJson toJson) {
        size_t i = 0;
        while (i < records.size()) {
            size_t end = std::min(i + batchSize, records.size());

            std::string batch;
            for (size_t j = i; j < end; ++j) {
                if (j > i) batch += '\n';
                batch += toJson(records[j]);
            }

            RdKafka::ErrorCode err = producer_->produce(
                topic,
                RdKafka::Topic::PARTITION_UA,
                RdKafka::Producer::RK_MSG_COPY,
                const_cast<char*>(batch.data()),
                batch.size(),
                nullptr, 0,
                0, nullptr, nullptr);

            if (err != RdKafka::ERR_NO_ERROR) {
                producer_->poll(100);
                err = producer_->produce(
                    topic,
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
    std::unique_ptr<RdKafka::Producer> producer_;
};

KafkaWriter::KafkaWriter(const std::string& brokers)
    : impl_(std::make_unique<Impl>(brokers)) {}

KafkaWriter::~KafkaWriter() = default;

void KafkaWriter::produceFloat(const std::vector<FloatRecord>& records, size_t batchSize, const std::string& topic) {
    impl_->produce(records, batchSize, topic, floatToJson);
}

void KafkaWriter::produceDouble(const std::vector<DoubleRecord>& records, size_t batchSize, const std::string& topic) {
    impl_->produce(records, batchSize, topic, doubleToJson);
}

void KafkaWriter::produceInt(const std::vector<IntRecord>& records, size_t batchSize, const std::string& topic) {
    impl_->produce(records, batchSize, topic, intToJson);
}
