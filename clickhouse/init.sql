CREATE DATABASE IF NOT EXISTS benchmark;

-- ── Direct-insert target tables (one per value type) ──────

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

-- ── Kafka (Confluent) — 1 partition (default) ─────────────

CREATE TABLE IF NOT EXISTS benchmark.signals_float_queue_k (
    signal_id UInt64, time DateTime, value Float32
) ENGINE = Kafka()
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'signals-float',
         kafka_group_name = 'bf-k-1p',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS benchmark.signals_double_queue_k (
    signal_id UInt64, time DateTime, value Float64
) ENGINE = Kafka()
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'signals-double',
         kafka_group_name = 'bd-k-1p',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS benchmark.signals_int_queue_k (
    signal_id UInt64, time DateTime, value Int64
) ENGINE = Kafka()
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'signals-int',
         kafka_group_name = 'bi-k-1p',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS benchmark.signals_float_sink_k (
    signal_id UInt64, time DateTime, value Float32
) ENGINE = MergeTree() ORDER BY (signal_id, time);

CREATE TABLE IF NOT EXISTS benchmark.signals_double_sink_k (
    signal_id UInt64, time DateTime, value Float64
) ENGINE = MergeTree() ORDER BY (signal_id, time);

CREATE TABLE IF NOT EXISTS benchmark.signals_int_sink_k (
    signal_id UInt64, time DateTime, value Int64
) ENGINE = MergeTree() ORDER BY (signal_id, time);

CREATE MATERIALIZED VIEW IF NOT EXISTS benchmark.signals_float_mv_k
    TO benchmark.signals_float_sink_k AS
SELECT * FROM benchmark.signals_float_queue_k;

CREATE MATERIALIZED VIEW IF NOT EXISTS benchmark.signals_double_mv_k
    TO benchmark.signals_double_sink_k AS
SELECT * FROM benchmark.signals_double_queue_k;

CREATE MATERIALIZED VIEW IF NOT EXISTS benchmark.signals_int_mv_k
    TO benchmark.signals_int_sink_k AS
SELECT * FROM benchmark.signals_int_queue_k;

-- ── Kafka — 3 partitions ──────────────────────────────────

CREATE TABLE IF NOT EXISTS benchmark.signals_float_queue_k_3p (
    signal_id UInt64, time DateTime, value Float32
) ENGINE = Kafka()
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'signals-float-3p',
         kafka_group_name = 'bf-k-3p',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 3;

CREATE TABLE IF NOT EXISTS benchmark.signals_double_queue_k_3p (
    signal_id UInt64, time DateTime, value Float64
) ENGINE = Kafka()
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'signals-double-3p',
         kafka_group_name = 'bd-k-3p',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 3;

CREATE TABLE IF NOT EXISTS benchmark.signals_int_queue_k_3p (
    signal_id UInt64, time DateTime, value Int64
) ENGINE = Kafka()
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'signals-int-3p',
         kafka_group_name = 'bi-k-3p',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 3;

CREATE TABLE IF NOT EXISTS benchmark.signals_float_sink_k_3p (
    signal_id UInt64, time DateTime, value Float32
) ENGINE = MergeTree() ORDER BY (signal_id, time);

CREATE TABLE IF NOT EXISTS benchmark.signals_double_sink_k_3p (
    signal_id UInt64, time DateTime, value Float64
) ENGINE = MergeTree() ORDER BY (signal_id, time);

CREATE TABLE IF NOT EXISTS benchmark.signals_int_sink_k_3p (
    signal_id UInt64, time DateTime, value Int64
) ENGINE = MergeTree() ORDER BY (signal_id, time);

CREATE MATERIALIZED VIEW IF NOT EXISTS benchmark.signals_float_mv_k_3p
    TO benchmark.signals_float_sink_k_3p AS
SELECT * FROM benchmark.signals_float_queue_k_3p;

CREATE MATERIALIZED VIEW IF NOT EXISTS benchmark.signals_double_mv_k_3p
    TO benchmark.signals_double_sink_k_3p AS
SELECT * FROM benchmark.signals_double_queue_k_3p;

CREATE MATERIALIZED VIEW IF NOT EXISTS benchmark.signals_int_mv_k_3p
    TO benchmark.signals_int_sink_k_3p AS
SELECT * FROM benchmark.signals_int_queue_k_3p;

-- ── Kafka — 5 partitions ──────────────────────────────────

CREATE TABLE IF NOT EXISTS benchmark.signals_float_queue_k_5p (
    signal_id UInt64, time DateTime, value Float32
) ENGINE = Kafka()
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'signals-float-5p',
         kafka_group_name = 'bf-k-5p',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 5;

CREATE TABLE IF NOT EXISTS benchmark.signals_double_queue_k_5p (
    signal_id UInt64, time DateTime, value Float64
) ENGINE = Kafka()
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'signals-double-5p',
         kafka_group_name = 'bd-k-5p',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 5;

CREATE TABLE IF NOT EXISTS benchmark.signals_int_queue_k_5p (
    signal_id UInt64, time DateTime, value Int64
) ENGINE = Kafka()
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'signals-int-5p',
         kafka_group_name = 'bi-k-5p',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 5;

CREATE TABLE IF NOT EXISTS benchmark.signals_float_sink_k_5p (
    signal_id UInt64, time DateTime, value Float32
) ENGINE = MergeTree() ORDER BY (signal_id, time);

CREATE TABLE IF NOT EXISTS benchmark.signals_double_sink_k_5p (
    signal_id UInt64, time DateTime, value Float64
) ENGINE = MergeTree() ORDER BY (signal_id, time);

CREATE TABLE IF NOT EXISTS benchmark.signals_int_sink_k_5p (
    signal_id UInt64, time DateTime, value Int64
) ENGINE = MergeTree() ORDER BY (signal_id, time);

CREATE MATERIALIZED VIEW IF NOT EXISTS benchmark.signals_float_mv_k_5p
    TO benchmark.signals_float_sink_k_5p AS
SELECT * FROM benchmark.signals_float_queue_k_5p;

CREATE MATERIALIZED VIEW IF NOT EXISTS benchmark.signals_double_mv_k_5p
    TO benchmark.signals_double_sink_k_5p AS
SELECT * FROM benchmark.signals_double_queue_k_5p;

CREATE MATERIALIZED VIEW IF NOT EXISTS benchmark.signals_int_mv_k_5p
    TO benchmark.signals_int_sink_k_5p AS
SELECT * FROM benchmark.signals_int_queue_k_5p;

-- ── Redpanda — 1 partition (default) ──────────────────────

CREATE TABLE IF NOT EXISTS benchmark.signals_float_queue_rp (
    signal_id UInt64, time DateTime, value Float32
) ENGINE = Kafka()
SETTINGS kafka_broker_list = 'redpanda:9093',
         kafka_topic_list = 'signals-float',
         kafka_group_name = 'bf-rp-1p',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS benchmark.signals_double_queue_rp (
    signal_id UInt64, time DateTime, value Float64
) ENGINE = Kafka()
SETTINGS kafka_broker_list = 'redpanda:9093',
         kafka_topic_list = 'signals-double',
         kafka_group_name = 'bd-rp-1p',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS benchmark.signals_int_queue_rp (
    signal_id UInt64, time DateTime, value Int64
) ENGINE = Kafka()
SETTINGS kafka_broker_list = 'redpanda:9093',
         kafka_topic_list = 'signals-int',
         kafka_group_name = 'bi-rp-1p',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS benchmark.signals_float_sink_rp (
    signal_id UInt64, time DateTime, value Float32
) ENGINE = MergeTree() ORDER BY (signal_id, time);

CREATE TABLE IF NOT EXISTS benchmark.signals_double_sink_rp (
    signal_id UInt64, time DateTime, value Float64
) ENGINE = MergeTree() ORDER BY (signal_id, time);

CREATE TABLE IF NOT EXISTS benchmark.signals_int_sink_rp (
    signal_id UInt64, time DateTime, value Int64
) ENGINE = MergeTree() ORDER BY (signal_id, time);

CREATE MATERIALIZED VIEW IF NOT EXISTS benchmark.signals_float_mv_rp
    TO benchmark.signals_float_sink_rp AS
SELECT * FROM benchmark.signals_float_queue_rp;

CREATE MATERIALIZED VIEW IF NOT EXISTS benchmark.signals_double_mv_rp
    TO benchmark.signals_double_sink_rp AS
SELECT * FROM benchmark.signals_double_queue_rp;

CREATE MATERIALIZED VIEW IF NOT EXISTS benchmark.signals_int_mv_rp
    TO benchmark.signals_int_sink_rp AS
SELECT * FROM benchmark.signals_int_queue_rp;

-- ── Redpanda — 3 partitions ───────────────────────────────

CREATE TABLE IF NOT EXISTS benchmark.signals_float_queue_rp_3p (
    signal_id UInt64, time DateTime, value Float32
) ENGINE = Kafka()
SETTINGS kafka_broker_list = 'redpanda:9093',
         kafka_topic_list = 'signals-float-3p',
         kafka_group_name = 'bf-rp-3p',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 3;

CREATE TABLE IF NOT EXISTS benchmark.signals_double_queue_rp_3p (
    signal_id UInt64, time DateTime, value Float64
) ENGINE = Kafka()
SETTINGS kafka_broker_list = 'redpanda:9093',
         kafka_topic_list = 'signals-double-3p',
         kafka_group_name = 'bd-rp-3p',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 3;

CREATE TABLE IF NOT EXISTS benchmark.signals_int_queue_rp_3p (
    signal_id UInt64, time DateTime, value Int64
) ENGINE = Kafka()
SETTINGS kafka_broker_list = 'redpanda:9093',
         kafka_topic_list = 'signals-int-3p',
         kafka_group_name = 'bi-rp-3p',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 3;

CREATE TABLE IF NOT EXISTS benchmark.signals_float_sink_rp_3p (
    signal_id UInt64, time DateTime, value Float32
) ENGINE = MergeTree() ORDER BY (signal_id, time);

CREATE TABLE IF NOT EXISTS benchmark.signals_double_sink_rp_3p (
    signal_id UInt64, time DateTime, value Float64
) ENGINE = MergeTree() ORDER BY (signal_id, time);

CREATE TABLE IF NOT EXISTS benchmark.signals_int_sink_rp_3p (
    signal_id UInt64, time DateTime, value Int64
) ENGINE = MergeTree() ORDER BY (signal_id, time);

CREATE MATERIALIZED VIEW IF NOT EXISTS benchmark.signals_float_mv_rp_3p
    TO benchmark.signals_float_sink_rp_3p AS
SELECT * FROM benchmark.signals_float_queue_rp_3p;

CREATE MATERIALIZED VIEW IF NOT EXISTS benchmark.signals_double_mv_rp_3p
    TO benchmark.signals_double_sink_rp_3p AS
SELECT * FROM benchmark.signals_double_queue_rp_3p;

CREATE MATERIALIZED VIEW IF NOT EXISTS benchmark.signals_int_mv_rp_3p
    TO benchmark.signals_int_sink_rp_3p AS
SELECT * FROM benchmark.signals_int_queue_rp_3p;

-- ── Redpanda — 5 partitions ───────────────────────────────

CREATE TABLE IF NOT EXISTS benchmark.signals_float_queue_rp_5p (
    signal_id UInt64, time DateTime, value Float32
) ENGINE = Kafka()
SETTINGS kafka_broker_list = 'redpanda:9093',
         kafka_topic_list = 'signals-float-5p',
         kafka_group_name = 'bf-rp-5p',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 5;

CREATE TABLE IF NOT EXISTS benchmark.signals_double_queue_rp_5p (
    signal_id UInt64, time DateTime, value Float64
) ENGINE = Kafka()
SETTINGS kafka_broker_list = 'redpanda:9093',
         kafka_topic_list = 'signals-double-5p',
         kafka_group_name = 'bd-rp-5p',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 5;

CREATE TABLE IF NOT EXISTS benchmark.signals_int_queue_rp_5p (
    signal_id UInt64, time DateTime, value Int64
) ENGINE = Kafka()
SETTINGS kafka_broker_list = 'redpanda:9093',
         kafka_topic_list = 'signals-int-5p',
         kafka_group_name = 'bi-rp-5p',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 5;

CREATE TABLE IF NOT EXISTS benchmark.signals_float_sink_rp_5p (
    signal_id UInt64, time DateTime, value Float32
) ENGINE = MergeTree() ORDER BY (signal_id, time);

CREATE TABLE IF NOT EXISTS benchmark.signals_double_sink_rp_5p (
    signal_id UInt64, time DateTime, value Float64
) ENGINE = MergeTree() ORDER BY (signal_id, time);

CREATE TABLE IF NOT EXISTS benchmark.signals_int_sink_rp_5p (
    signal_id UInt64, time DateTime, value Int64
) ENGINE = MergeTree() ORDER BY (signal_id, time);

CREATE MATERIALIZED VIEW IF NOT EXISTS benchmark.signals_float_mv_rp_5p
    TO benchmark.signals_float_sink_rp_5p AS
SELECT * FROM benchmark.signals_float_queue_rp_5p;

CREATE MATERIALIZED VIEW IF NOT EXISTS benchmark.signals_double_mv_rp_5p
    TO benchmark.signals_double_sink_rp_5p AS
SELECT * FROM benchmark.signals_double_queue_rp_5p;

CREATE MATERIALIZED VIEW IF NOT EXISTS benchmark.signals_int_mv_rp_5p
    TO benchmark.signals_int_sink_rp_5p AS
SELECT * FROM benchmark.signals_int_queue_rp_5p;
