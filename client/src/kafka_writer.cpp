#include "kafka_writer.hpp"
#include <librdkafka/rdkafkacpp.h>
#include <sstream>
#include <stdexcept>
#include <iostream>
#include <iomanip>
#include <cstring>
#include <chrono>

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

class DeliveryCb : public RdKafka::DeliveryReportCb {
public:
    explicit DeliveryCb(std::shared_ptr<LatencyTracker> tracker)
        : tracker_(std::move(tracker)) {}

    void dr_cb(RdKafka::Message& message) override {
        if (message.err() != RdKafka::ERR_NO_ERROR) return;
        if (!message.msg_opaque()) return;
        auto* ts = static_cast<uint64_t*>(message.msg_opaque());
        auto now = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
        double latMs = static_cast<double>(now - *ts) / 1000.0;
        delete ts;
        if (tracker_) {
            try {
                tracker_->add(latMs);
            } catch (...) {}
        }
    }

private:
    std::shared_ptr<LatencyTracker> tracker_;
};

class KafkaWriter::Impl {
public:
    Impl(const KafkaWriterConfig& config, std::shared_ptr<LatencyTracker> tracker)
        : asyncFlush_(config.asyncFlush)
        , deliveryCb_(tracker)
        , trackLatency_(tracker != nullptr)
    {
        std::string errstr;

        conf_.reset(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
        if (!conf_)
            throw std::runtime_error("failed to create kafka config");

        if (conf_->set("bootstrap.servers", config.brokers, errstr) != RdKafka::Conf::CONF_OK)
            throw std::runtime_error("kafka conf error: " + errstr);
        if (conf_->set("message.timeout.ms", "30000", errstr) != RdKafka::Conf::CONF_OK)
            throw std::runtime_error("kafka conf error: " + errstr);
        if (conf_->set("queue.buffering.max.messages", "100000", errstr) != RdKafka::Conf::CONF_OK)
            throw std::runtime_error("kafka conf error: " + errstr);
        if (conf_->set("batch.num.messages", "10000", errstr) != RdKafka::Conf::CONF_OK)
            throw std::runtime_error("kafka conf error: " + errstr);

        if (trackLatency_) {
            if (conf_->set("dr_cb", &deliveryCb_, errstr) != RdKafka::Conf::CONF_OK)
                throw std::runtime_error("kafka conf error: " + errstr);
        }

        if (!config.compression.empty()) {
            if (conf_->set("compression.codec", config.compression, errstr) != RdKafka::Conf::CONF_OK)
                throw std::runtime_error("kafka conf error: " + errstr);
        }

        producer_.reset(RdKafka::Producer::create(conf_.get(), errstr));
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

            std::ostringstream os;
            for (size_t j = i; j < end; ++j) {
                if (j > i) os << '\n';
                os << toJson(records[j]);
            }
            std::string batch = os.str();

            char* buf = static_cast<char*>(std::malloc(batch.size()));
            if (!buf) throw std::bad_alloc();
            std::memcpy(buf, batch.data(), batch.size());

            void* opaque = nullptr;
            if (trackLatency_) {
                auto* ts = new uint64_t(
                    std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::steady_clock::now().time_since_epoch()).count());
                opaque = ts;
            }

            RdKafka::ErrorCode err = producer_->produce(
                topic,
                RdKafka::Topic::PARTITION_UA,
                RdKafka::Producer::RK_MSG_FREE,
                buf, batch.size(),
                nullptr, 0,
                0, nullptr, opaque);

            if (err != RdKafka::ERR_NO_ERROR) {
                if (opaque) delete static_cast<uint64_t*>(opaque);
                std::free(buf);
                producer_->poll(100);
                char* buf2 = static_cast<char*>(std::malloc(batch.size()));
                if (!buf2) throw std::bad_alloc();
                std::memcpy(buf2, batch.data(), batch.size());
                err = producer_->produce(
                    topic,
                    RdKafka::Topic::PARTITION_UA,
                    RdKafka::Producer::RK_MSG_FREE,
                    buf2, batch.size(),
                    nullptr, 0,
                    0, nullptr, nullptr);
                if (err != RdKafka::ERR_NO_ERROR) {
                    std::free(buf2);
                    throw std::runtime_error("kafka produce error: " +
                        RdKafka::err2str(err));
                }
            }

            producer_->poll(0);

            if (!asyncFlush_) {
                producer_->flush(5000);
            }

            i = end;
        }
    }

    void flush() {
        if (producer_) {
            producer_->flush(10000);
        }
    }

private:
    std::unique_ptr<RdKafka::Conf> conf_;
    DeliveryCb deliveryCb_;
    std::unique_ptr<RdKafka::Producer> producer_;
    bool asyncFlush_;
    bool trackLatency_ = false;
};

KafkaWriter::KafkaWriter(const KafkaWriterConfig& config)
    : latencyTracker_(config.trackLatency ? std::make_shared<LatencyTracker>() : nullptr)
    , impl_(std::make_unique<Impl>(config, latencyTracker_))
{}

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

void KafkaWriter::flush() {
    impl_->flush();
}
