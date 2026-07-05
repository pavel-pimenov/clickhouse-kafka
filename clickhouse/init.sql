CREATE DATABASE IF NOT EXISTS benchmark;

-- Target table for direct ClickHouse native writes
CREATE TABLE IF NOT EXISTS benchmark.direct (
    id UInt64,
    timestamp DateTime,
    value Float64,
    message String
) ENGINE = MergeTree()
ORDER BY (timestamp, id);

-- Target table for Kafka-sourced writes
CREATE TABLE IF NOT EXISTS benchmark.kafka_sink (
    id UInt64,
    timestamp DateTime,
    value Float64,
    message String
) ENGINE = MergeTree()
ORDER BY (timestamp, id);

-- Kafka engine table (consumes from Confluent Kafka)
CREATE TABLE IF NOT EXISTS benchmark.kafka_queue (
    id UInt64,
    timestamp DateTime,
    value Float64,
    message String
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'benchmark',
    kafka_group_name = 'benchmark-group',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_thread_per_consumer = 1;

-- Materialised view: Kafka -> kafka_sink
CREATE MATERIALIZED VIEW IF NOT EXISTS benchmark.kafka_mv TO benchmark.kafka_sink AS
SELECT * FROM benchmark.kafka_queue;

-- Target table for Redpanda-sourced writes
CREATE TABLE IF NOT EXISTS benchmark.redpanda_sink (
    id UInt64,
    timestamp DateTime,
    value Float64,
    message String
) ENGINE = MergeTree()
ORDER BY (timestamp, id);

-- Kafka engine table (consumes from Redpanda)
CREATE TABLE IF NOT EXISTS benchmark.redpanda_queue (
    id UInt64,
    timestamp DateTime,
    value Float64,
    message String
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'redpanda:9093',
    kafka_topic_list = 'benchmark',
    kafka_group_name = 'benchmark-group-rp',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_thread_per_consumer = 1;

-- Materialised view: Redpanda -> redpanda_sink
CREATE MATERIALIZED VIEW IF NOT EXISTS benchmark.redpanda_mv TO benchmark.redpanda_sink AS
SELECT * FROM benchmark.redpanda_queue;
