# Time-Series Benchmark Design

## Rationale

Replace flat `(id, timestamp, value, message)` tables with realistic
time-series storage: signals/sensors with typed values in separate tables.

## Schema

### Direct‑insert tables (MergeTree)

```sql
CREATE TABLE benchmark.signals_float (
    signal_id UInt64,
    time      DateTime,
    value     Float32
) ENGINE = MergeTree()
ORDER BY (signal_id, time);

CREATE TABLE benchmark.signals_double (
    signal_id UInt64,
    time      DateTime,
    value     Float64
) ENGINE = MergeTree()
ORDER BY (signal_id, time);

CREATE TABLE benchmark.signals_int (
    signal_id UInt64,
    time      DateTime,
    value     Int64
) ENGINE = MergeTree()
ORDER BY (signal_id, time);
```

### Kafka‑engine tables (one per type × one per broker)

Six Kafka engine tables + six MVs:

| Queue table | Broker | Topic | Sink table |
|-------------|--------|-------|------------|
| `signals_float_queue_k` | kafka:9092 | `signals-float` | `signals_float_sink_k` |
| `signals_double_queue_k` | kafka:9092 | `signals-double` | `signals_double_sink_k` |
| `signals_int_queue_k` | kafka:9092 | `signals-int` | `signals_int_sink_k` |
| `signals_float_queue_rp` | redpanda:9093 | `signals-float` | `signals_float_sink_rp` |
| `signals_double_queue_rp` | redpanda:9093 | `signals-double` | `signals_double_sink_rp` |
| `signals_int_queue_rp` | redpanda:9093 | `signals-int` | `signals_int_sink_rp` |

### Record structure

```cpp
enum class ValueType { Float, Double, Int };

struct Record {
    uint64_t signalId;
    time_t   time;
    ValueType type;
    union { float f; double d; int64_t i; } value;
};
```

One data vector generated in round‑robin: ⅓ float, ⅓ double, ⅓ int.
Total N = 300 000 (100 000 per type).

### Data generation

- `signalId` ∈ [0, 999] (1 000 signals, round‑robin)
- `time` = wall clock + signalId / 1000 (simulates "each signal fires
  every second")
- Float value: `std::uniform_real_distribution<float>(-1e6, 1e6)`
- Double value: `std::uniform_real_distribution<double>(-1e12, 1e12)`
- Int value: `std::uniform_int_distribution<int64_t>(INT64_MIN/2, INT64_MAX/2)`

## Benchmark phases

1. **ClickHouse native** — 3 `INSERT` calls (one per type) via native protocol,
   batched. Measure total time across all types.
2. **Kafka Produce** — librdkafka produces to 3 topics in succession,
   synced per batch (`flush()`). Measure total time.
3. **Redpanda Produce** — same as #2 but to Redpanda broker.
4. **Verification** — poll each broker’s sink table until all rows arrive
   (or 60 s timeout).

## Metric collection

Same as before:
- wall‑clock elapsed
- throughput (rec/s + MB/s)
- CPU user/sys (getrusage)
- peak RSS (/proc/self/status VmPeak)

## Expected improvements over v1

- Realistic IoT/IIoT workload (many signals, typed measurements)
- Multi‑topic produce measures actual production overhead
- Separate tables per type allow per‑type storage tuning (codec,
  compression, TTL)
- Kafka/Redpanda comparison now uses 3 topics each → more realistic
  multi‑partition load

## Risks / Open questions

- 3 inserts per phase triple the round‑trip overhead. For very high
  batch sizes this is negligible, but at small batches the native
  path may look worse than it should.
- Redpanda healthcheck uses `localhost:9644` — if `curl` is missing
  from the image, we need an alternative (`wget` or `rpk`).
