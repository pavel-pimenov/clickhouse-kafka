#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "============================================"
echo " CH Native vs Kafka vs Redpanda — Benchmark"
echo "============================================"
echo ""

export COMPOSE_PROJECT_NAME=chkafka

cleanup() {
    echo ""
    echo "Stopping services..."
    docker compose -f "$PROJECT_DIR/docker-compose.yml" down -v 2>/dev/null || true
}
trap cleanup EXIT

echo "Starting ClickHouse, Kafka and Redpanda..."
docker compose -f "$PROJECT_DIR/docker-compose.yml" up -d --wait clickhouse kafka redpanda

echo ""
echo "Creating Redpanda topics..."
for topic_partition in signals-float-3p:3 signals-double-3p:3 signals-int-3p:3 \
                       signals-float-5p:5 signals-double-5p:5 signals-int-5p:5; do
    topic="${topic_partition%:*}"
    partitions="${topic_partition#*:}"
    docker compose -f "$PROJECT_DIR/docker-compose.yml" exec -T redpanda \
        rpk topic create "$topic" --partitions "$partitions" 2>&1 || true
done

# Overridable env vars
PARTITIONS="${PARTITIONS:-3}"
BATCH_SIZE="${BATCH_SIZE:-1000}"
NUM_RECORDS="${NUM_RECORDS:-100000}"
ITERATIONS="${ITERATIONS:-1}"
TRACK_LATENCY="${TRACK_LATENCY:-1}"

echo ""
echo "Building client image..."
docker compose -f "$PROJECT_DIR/docker-compose.yml" build client

echo ""
echo "Running benchmark client (partitions=$PARTITIONS, batch=$BATCH_SIZE, records=$NUM_RECORDS, iters=$ITERATIONS)..."
docker compose -f "$PROJECT_DIR/docker-compose.yml" run --rm \
  -e PARTITIONS="$PARTITIONS" \
  -e BATCH_SIZE="$BATCH_SIZE" \
  -e NUM_RECORDS="$NUM_RECORDS" \
  -e ITERATIONS="$ITERATIONS" \
  -e TRACK_LATENCY="$TRACK_LATENCY" \
  client

echo ""
echo "Benchmark finished."
