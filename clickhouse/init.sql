CREATE DATABASE IF NOT EXISTS benchmark;

-- Direct-insert target tables (one per value type)
CREATE TABLE IF NOT EXISTS benchmark.signals_float (
    signal_id UInt64,
    time DateTime,
    value Float32
) ENGINE = MergeTree()
ORDER BY (signal_id, time);

CREATE TABLE IF NOT EXISTS benchmark.signals_double (
    signal_id UInt64,
    time DateTime,
    value Float64
) ENGINE = MergeTree()
ORDER BY (signal_id, time);

CREATE TABLE IF NOT EXISTS benchmark.signals_int (
    signal_id UInt64,
    time DateTime,
    value Int64
) ENGINE = MergeTree()
ORDER BY (signal_id, time);

-- ── Kafka (Confluent) engine tables ──────────────────────

CREATE TABLE IF NOT EXISTS benchmark.signals_float_queue_k (
    signal_id UInt64,
    time DateTime,
    value Float32
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'signals-float',
    kafka_group_name = 'benchmark-float',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS benchmark.signals_double_queue_k (
    signal_id UInt64,
    time DateTime,
    value Float64
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'signals-double',
    kafka_group_name = 'benchmark-double',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS benchmark.signals_int_queue_k (
    signal_id UInt64,
    time DateTime,
    value Int64
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'signals-int',
    kafka_group_name = 'benchmark-int',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

-- Kafka sink tables
CREATE TABLE IF NOT EXISTS benchmark.signals_float_sink_k (
    signal_id UInt64,
    time DateTime,
    value Float32
) ENGINE = MergeTree()
ORDER BY (signal_id, time);

CREATE TABLE IF NOT EXISTS benchmark.signals_double_sink_k (
    signal_id UInt64,
    time DateTime,
    value Float64
) ENGINE = MergeTree()
ORDER BY (signal_id, time);

CREATE TABLE IF NOT EXISTS benchmark.signals_int_sink_k (
    signal_id UInt64,
    time DateTime,
    value Int64
) ENGINE = MergeTree()
ORDER BY (signal_id, time);

-- Kafka materialised views
CREATE MATERIALIZED VIEW IF NOT EXISTS benchmark.signals_float_mv_k
    TO benchmark.signals_float_sink_k AS
SELECT * FROM benchmark.signals_float_queue_k;

CREATE MATERIALIZED VIEW IF NOT EXISTS benchmark.signals_double_mv_k
    TO benchmark.signals_double_sink_k AS
SELECT * FROM benchmark.signals_double_queue_k;

CREATE MATERIALIZED VIEW IF NOT EXISTS benchmark.signals_int_mv_k
    TO benchmark.signals_int_sink_k AS
SELECT * FROM benchmark.signals_int_queue_k;

-- ── Redpanda engine tables ───────────────────────────────

CREATE TABLE IF NOT EXISTS benchmark.signals_float_queue_rp (
    signal_id UInt64,
    time DateTime,
    value Float32
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'redpanda:9093',
    kafka_topic_list = 'signals-float',
    kafka_group_name = 'benchmark-float-rp',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS benchmark.signals_double_queue_rp (
    signal_id UInt64,
    time DateTime,
    value Float64
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'redpanda:9093',
    kafka_topic_list = 'signals-double',
    kafka_group_name = 'benchmark-double-rp',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS benchmark.signals_int_queue_rp (
    signal_id UInt64,
    time DateTime,
    value Int64
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'redpanda:9093',
    kafka_topic_list = 'signals-int',
    kafka_group_name = 'benchmark-int-rp',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

-- Redpanda sink tables
CREATE TABLE IF NOT EXISTS benchmark.signals_float_sink_rp (
    signal_id UInt64,
    time DateTime,
    value Float32
) ENGINE = MergeTree()
ORDER BY (signal_id, time);

CREATE TABLE IF NOT EXISTS benchmark.signals_double_sink_rp (
    signal_id UInt64,
    time DateTime,
    value Float64
) ENGINE = MergeTree()
ORDER BY (signal_id, time);

CREATE TABLE IF NOT EXISTS benchmark.signals_int_sink_rp (
    signal_id UInt64,
    time DateTime,
    value Int64
) ENGINE = MergeTree()
ORDER BY (signal_id, time);

-- Redpanda materialised views
CREATE MATERIALIZED VIEW IF NOT EXISTS benchmark.signals_float_mv_rp
    TO benchmark.signals_float_sink_rp AS
SELECT * FROM benchmark.signals_float_queue_rp;

CREATE MATERIALIZED VIEW IF NOT EXISTS benchmark.signals_double_mv_rp
    TO benchmark.signals_double_sink_rp AS
SELECT * FROM benchmark.signals_double_queue_rp;

CREATE MATERIALIZED VIEW IF NOT EXISTS benchmark.signals_int_mv_rp
    TO benchmark.signals_int_sink_rp AS
SELECT * FROM benchmark.signals_int_queue_rp;
