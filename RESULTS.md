# ClickHouse Native vs Kafka Produce — Benchmark Results

## V4: Optimization Comparison (async / parallel / zstd — Kafka 3p vs Redpanda 3p)

### Changes Since V3
- **Async mode**: removed per-batch `flush()`, call once at end — librdkafka pipelines batches
- **Parallel mode**: 3 independent producers in 3 threads (one per signal type)
- **RK_MSG_FREE**: librdkafka takes ownership of buffers, saves a copy
- **zstd compression**: `compression.codec=zstd` in librdkafka config

### Results

| Metric | CH Native | Kafka sync+seq | Kafka async | Kafka parallel | Kafka async+par+zstd | Redpanda sync+seq | Redpanda async | Redpanda parallel | Redpanda async+par+zstd |
|---|---|---|---|---|---|---|---|---|---|
| **rec/s** | 145 353 | 71 067 | **242 521** | 141 495 | 181 826 | 70 803 | **343 727** | 131 446 | **407 871** |
| **время (с)** | 0.69 | 1.41 | **0.41** | 0.71 | 0.55 | 1.41 | **0.29** | 0.76 | **0.25** |
| RSS (KB) | 14 996 | 19 900 | 20 940 | 22 428 | 33 236 | 33 264 | 33 720 | 34 152 | 41 184 |

### Ускорение относительно sync+seq baseline

| Оптимизация | Kafka 3p | Ускорение | Redpanda 3p | Ускорение |
|---|---|---|---|---|
| sync+seq (baseline) | 71 067 rec/s | 1.0× | 70 803 rec/s | 1.0× |
| async only | 242 521 | **+3.4×** | 343 727 | **+4.9×** |
| parallel only | 141 495 | **+2.0×** | 131 446 | **+1.9×** |
| async+parallel+zstd | 181 826 | **+2.6×** | **407 871** | **+5.8×** |

### Key Takeaways

1. **Async — главный выигрыш**. Убрать `flush()` из цикла — это +240–380%. librdkafka сам конвейерит produce + poll.
2. **Parallel (3 потока) — +90–100%**. Каждый поток со своим продюсером. Ограничен CPU (2 ядра).
3. **zstd compression** — для Kafka async+parallel+zstd медленнее async (182K vs 242K). Для Redpanda — наоборот быстрее (408K vs 344K). Возможно, zstd даёт меньше данных на wire = быстрее отправка на Redpanda, но на Kafka узкое место не в сети.
4. **Redpanda async+parallel+zstd: 408K rec/s — быстрее CH Native (145K)**. Впервые брокерский путь обогнал прямую вставку в ClickHouse. Хотя сравнение не совсем честное — produce в Kafka vs insert в CH с индексацией.
5. **RSS растёт** с параллельными продюсерами (20K → 33–41K), что ожидаемо.

---

## V3: Partition Count Comparison (1p / 3p / 5p — Kafka vs Redpanda)

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
| Total data | ~2.16 MB |

### Schema — Partition Variants

For each broker, three topic/table sets exist with 1, 3, and 5 partitions:

| Partitions | Kafka topics | Redpanda topics | Sink suffix (Kafka) | Sink suffix (Redpanda) |
|-----------|-------------|----------------|---------------------|----------------------|
| 1 (default) | `signals-{type}` | `signals-{type}` | `_k` | `_rp` |
| 3 | `signals-{type}-3p` | `signals-{type}-3p` | `_k_3p` | `_rp_3p` |
| 5 | `signals-{type}-5p` | `signals-{type}-5p` | `_k_5p` | `_rp_5p` |

Each set has 3 Kafka engine tables + 3 sink tables + 3 MV (21 tables total per broker, 3 direct tables reused). ClickHouse `kafka_num_consumers` matches partition count.

### Kafka Produce Settings
- `acks=1` (leader only)
- Synchronous `flush()` per batch for fair latency comparison
- Tables truncated before each run (no accumulation)

### Workload
Seven sequential phases:
1. **CH Native** — insert to direct tables
2. **Kafka 1p** — produce to 1-partition topics, verify sinks
3. **Kafka 3p** — produce to 3-partition topics, verify sinks
4. **Kafka 5p** — produce to 5-partition topics, verify sinks
5. **Redpanda 1p** — produce to 1-partition topics, verify sinks
6. **Redpanda 3p** — produce to 3-partition topics, verify sinks
7. **Redpanda 5p** — produce to 5-partition topics, verify sinks

Each `KafkaWriter` is scoped and destructed between phases (fair RSS measurement).

### Results

| Metric | CH Native | Kafka 1p | Kafka 3p | Kafka 5p | Redpanda 1p | Redpanda 3p | Redpanda 5p |
|--------|----------:|---------:|---------:|---------:|------------:|------------:|------------:|
| Records | 100 000 | 100 000 | 100 000 | 100 000 | 100 000 | 100 000 | 100 000 |
| Elapsed (s) | 0.37 | 2.48 | 1.90 | 2.31 | 2.21 | **1.35** | **1.37** |
| Throughput (rec/s) | 271 571 | 40 272 | 52 613 | 43 379 | 45 214 | **74 244** | **73 192** |
| Throughput (MB/s) | 5.870 | 0.871 | 1.137 | 0.938 | 0.977 | **1.605** | **1.582** |
| CPU user (s) | 0.02 | 0.23 | 0.22 | 0.22 | 0.22 | 0.23 | 0.22 |
| CPU sys (s) | 0.01 | 0.03 | 0.03 | 0.03 | 0.03 | 0.03 | 0.03 |
| RSS (KB) | 15 096 | 19 868 | 19 892 | 19 796 | 19 800 | 19 804 | 19 820 |

### Partition scaling (throughput rel. to 1p)

| Broker | 1p → 3p | 1p → 5p |
|--------|:-------:|:-------:|
| Kafka | **+31%** | **+8%** |
| Redpanda | **+64%** | **+62%** |

### Key Takeaways

1. **CH native is 3.7–6.7× faster** than any broker path on this workload.
2. **3 partitions is the sweet spot** for Kafka (52.6K rec/s). 5 partitions regresses (–18% from 3p) — likely broker-side partition overhead on a single-core container.
3. **Redpanda scales better with partitions** — 3p gives +64% over 1p (74.2K), and 5p holds the gain without regression.
4. **Redpanda 3p is 41% faster than Kafka 3p** (74.2K vs 52.6K). Redpanda's C++ core handles multi-partition writes more efficiently on this hardware.
5. **RSS is stable** across all phases (~20 KB per producer, identical between brokers).
6. All 21 sink tables received exactly 33 333 rows — end-to-end correctness confirmed.

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
