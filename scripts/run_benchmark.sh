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

echo ""
echo "Building client image..."
docker compose -f "$PROJECT_DIR/docker-compose.yml" build client

echo ""
echo "Running benchmark client..."
docker compose -f "$PROJECT_DIR/docker-compose.yml" run --rm client

echo ""
echo "Benchmark finished."
