# ClickHouse Native vs Kafka Produce — Benchmark Results

## V2: Time-Series Benchmark (3-Way: CH Native vs Kafka vs Redpanda)

### Test Environment

#### Hardware
| Component | Detail |
|-----------|--------|
| CPU | Intel Core i5-3337U @ 1.80 GHz (max 2.70 GHz), 2 cores / 4 threads |
| Caches | L1d 64 KiB, L1i 64 KiB, L2 512 KiB, L3 3 MiB |
| Memory | 5.7 GiB total |
| Storage | SSD |
| Network | loopback (Docker compose) |
| ISA flags | SSE4.1/4.2, AVX (no AVX2), AES-NI |

#### Software
| Component | Version |
|-----------|---------|
| Host OS | EndeavourOS, Linux 7.1.2-arch3-1 x86_64 |
| Docker Engine | docker compose plugin v5.3.0 |
| ClickHouse | clickhouse-server:24.3 |
| Kafka | confluentinc/cp-kafka:7.6.3 (KRaft mode, no ZooKeeper) |
| Redpanda | docker.redpanda.com/redpandadata/redpanda:v24.2.7 |
| Client OS | Ubuntu 26.04 (Plucky Puffin) |
| Compiler | GCC 15.2.0 |
| CMake | 4.2 |
| Ninja | 1.12.1 |
| ccache | 4.10.2 |
| clickhouse-cpp | FetchContent (latest) |
| librdkafka | 2.13.0 (Debian) |

#### Configuration
| Parameter | Value |
|-----------|-------|
| Total records | 100 000 (~33 333 per type) |
| Batch size | 1 000 |
| Total data | ~2.16 MB (small typed records) |

### Schema — Time-Series Design

**Direct tables** (ClickHouse native insert targets):
- `signals_float` — `(signal_id UInt64, time DateTime, value Float32)`
- `signals_double` — `(signal_id UInt64, time DateTime, value Float64)`
- `signals_int` — `(signal_id UInt64, time DateTime, value Int64)`

**Kafka engine tables** (per broker, per type):
- Kafka: `signals_float_queue_k`, `signals_double_queue_k`, `signals_int_queue_k`
- Redpanda: `signals_float_queue_rp`, `signals_double_queue_rp`, `signals_int_queue_rp`

**Sink tables + Materialized Views** (per broker):
- `signals_float_sink_k` ← MV ← `signals_float_queue_k`
- `signals_double_sink_k` ← MV ← `signals_double_queue_k`
- `signals_int_sink_k` ← MV ← `signals_int_queue_k`
- Same for Redpanda (`_sink_rp`)

**Topics**: `signals-float`, `signals-double`, `signals-int`

### Kafka Produce Settings
- `acks=1` (leader only)
- Synchronous `flush()` per batch for fair latency comparison

### Workload
Three sequential phases:
1. **Phase 1** — Insert all records via ClickHouse native protocol (port 9000) to the 3 direct tables
2. **Phase 2** — Produce all records to Kafka (`kafka:9092`, 3 topics), then verify consumption via Kafka engine + MV
3. **Phase 3** — Produce all records to Redpanda (`redpanda:9093`, same 3 topics), then verify consumption via Redpanda Kafka engine + MV

### Results

| Metric | ClickHouse Native | Kafka Produce | Redpanda Produce | Ratio (Kafka / CH) | Ratio (RP / CH) |
|--------|------------------:|--------------:|-----------------:|:------------------:|:----------------:|
| Records | 100 000 | 100 000 | 100 000 | 1.00x | 1.00x |
| Elapsed time (s) | 0.42 | 2.56 | 2.56 | **0.16x** | **0.16x** |
| Throughput (rec/s) | 238 235 | 39 128 | 39 130 | **0.16x** | **0.16x** |
| Throughput (MB/s) | 5.150 | 0.846 | 0.846 | **0.16x** | **0.16x** |
| CPU user (s) | 0.02 | 0.25 | 0.31 | **0.09x** | **0.07x** |
| CPU sys (s) | 0.01 | 0.05 | 0.04 | **0.16x** | **0.23x** |
| Peak RSS (KB) | 34 012 | 257 592 | 413 228 | **0.13x** | **0.08x** |

> **Ratio legend**: < 1.00x means broker is slower than ClickHouse native.

### Verification
All records produced to Kafka and Redpanda were successfully consumed by the respective ClickHouse Kafka engine tables:
- `signals_float_sink_k`: 33 333 rows
- `signals_double_sink_k`: 33 333 rows
- `signals_int_sink_k`: 33 333 rows
- `signals_float_sink_rp`: 33 333 rows
- `signals_double_sink_rp`: 33 333 rows
- `signals_int_sink_rp`: 33 333 rows

### Key Takeaways

1. **ClickHouse native is ~6× faster** than both Kafka and Redpanda produce on this workload.
2. **Kafka and Redpanda are nearly identical** in throughput (~39 130 rec/s).
3. **Redpanda uses ~1.6× more peak RSS** than Kafka (413 MB vs 258 MB) — likely due to librdkafka's internal per-producer buffers.
4. **Smaller typed records** (2.16 MB vs 11.8 MB in v1) make the CH native advantage even more pronounced vs flat 100 B messages.
5. Both brokers consumed successfully via Kafka engine + MV — the time-series schema works end-to-end.

---

## V1: Flat Record Benchmark (2-Way: CH Native vs Kafka Produce)

### Configuration
| Parameter | Value |
|-----------|-------|
| Total records | 100 000 |
| Batch size | 1 000 |
| Message payload | 100 B |
| Total data | ~11.8 MB |

### Tables
- `benchmark.direct` — ClickHouse native insert target (MergeTree)
- `benchmark.kafka_sink` — Kafka → ClickHouse consumption target (MergeTree)
- `benchmark.kafka_queue` — Kafka engine table (topic `benchmark`)
- `benchmark.kafka_mv` — Materialized View: `kafka_queue → kafka_sink`

### Results

| Metric | ClickHouse Native | Kafka Produce | Ratio (CH / Kafka) |
|--------|------------------:|--------------:|:-------------------:|
| Records | 100 000 | 100 000 | 1.00x |
| Elapsed time (s) | 1.71 | 3.31 | **0.52x** |
| Throughput (rec/s) | 58 394 | 30 174 | **1.94x** |
| Throughput (MB/s) | 6.905 | 3.568 | **1.94x** |
| CPU user (s) | 0.03 | 0.29 | **0.09x** |
| CPU sys (s) | 0.03 | 0.04 | **0.79x** |
| Peak RSS (KB) | 48 204 | 271 756 | **0.18x** |

> **Ratio legend**: > 1.00 means ClickHouse native wins.

### Verification
All 100 000 records produced to Kafka were successfully consumed by the ClickHouse Kafka engine into `benchmark.kafka_sink`.

### Key Takeaways

1. **Throughput**: ClickHouse native is ~1.9× faster than Kafka produce → MV consumption.
2. **Memory**: ClickHouse native uses ~5× less peak RSS (48 MB vs 272 MB) — librdkafka's internal batching / compression buffers are the likely cause.
3. **CPU**: Kafka produce spends ~9× more user CPU time (primarily in librdkafka's protocol / serialization).
4. **Note**: These numbers reflect a single-threaded client on a low-power mobile CPU (Ivy Bridge). Kafka's advantage (buffering, compression, batching) typically grows with larger payloads, higher record counts, and more partitions.
