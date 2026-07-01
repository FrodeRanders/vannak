#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Vannak integration test runner.
#
# Starts infrastructure and runs integration tests. By default, runs the
# PostgreSQL/Ipto ingest→index→outbox→writer flow. With --cluster, also
# starts a 3-node vannak Raft cluster. With --kafka, also starts a
# Kafka-compatible Redpanda broker and runs the Kafka→Sitas smoke test.
# With --full, starts the full-stack demo (PostgreSQL + Kafka + 3-node
# Vannak supervisor cluster) and runs end-to-end health and query checks.
#
# Usage:
#   ./scripts/run-integration-test.sh               # PostgreSQL + Ipto writer
#   ./scripts/run-integration-test.sh --cluster     # PostgreSQL + Raft cluster
#   ./scripts/run-integration-test.sh --kafka       # also run Kafka smoke test
#   ./scripts/run-integration-test.sh --kafka-only  # Kafka smoke test only
#   ./scripts/run-integration-test.sh --full        # full-stack demo compose
#   ./scripts/run-integration-test.sh --no-build
#   ./scripts/run-integration-test.sh --down
# ---------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PG_COMPOSE="$PROJECT_DIR/docker-compose.test.yml"
CLUSTER_COMPOSE="$PROJECT_DIR/docker-compose.cluster.yml"
KAFKA_COMPOSE="$PROJECT_DIR/docker-compose.kafka.yml"
FULL_COMPOSE="$PROJECT_DIR/docker-compose.full.yml"
export VANNAK_PG_PORT="${VANNAK_PG_PORT:-5432}"
export VANNAK_KAFKA_PORT="${VANNAK_KAFKA_PORT:-19092}"

NO_BUILD=false
DOWN_ONLY=false
RUN_CLUSTER=false
RUN_KAFKA=false
KAFKA_ONLY=false
RUN_FULL=false

for arg in "$@"; do
    case "$arg" in
        --no-build) NO_BUILD=true ;;
        --down) DOWN_ONLY=true ;;
        --cluster) RUN_CLUSTER=true ;;
        --kafka) RUN_KAFKA=true ;;
        --kafka-only) RUN_KAFKA=true; KAFKA_ONLY=true ;;
        --full) RUN_FULL=true ;;
        *) echo "Unknown argument: $arg"; exit 1 ;;
    esac
done

# ---------------------------------------------------------------------------
# Tear down
# ---------------------------------------------------------------------------
compose_down() {
    echo "==> Tearing down..."
    docker compose -f "$PG_COMPOSE" down -v 2>/dev/null || true
    if [ "$RUN_CLUSTER" = true ]; then
        docker compose -f "$CLUSTER_COMPOSE" down -v --remove-orphans 2>/dev/null || true
    fi
    if [ "$RUN_KAFKA" = true ]; then
        docker compose -f "$KAFKA_COMPOSE" down -v --remove-orphans 2>/dev/null || true
    fi
    if [ "$RUN_FULL" = true ]; then
        docker compose -f "$FULL_COMPOSE" down -v --remove-orphans 2>/dev/null || true
    fi
}

if [ "$DOWN_ONLY" = true ]; then
    compose_down
    exit 0
fi

trap compose_down EXIT

# ---------------------------------------------------------------------------
# PostgreSQL + Ipto writer integration test
# ---------------------------------------------------------------------------
if [ "$KAFKA_ONLY" = false ]; then
echo "==> Starting PostgreSQL (port $VANNAK_PG_PORT)..."
docker compose -f "$PG_COMPOSE" down -v 2>/dev/null || true

IPTO_SQL_DIR="$PROJECT_DIR/../ipto/shared/db/postgresql"
if [ ! -f "$IPTO_SQL_DIR/schema.sql" ]; then
    echo "ERROR: ipto schema not found at $IPTO_SQL_DIR/schema.sql"
    echo "  (ipto repo must be checked out at ../ipto)"
    ls -la "$PROJECT_DIR/../ipto" 2>/dev/null || echo "  ../ipto does not exist"
    exit 1
fi
export IPTO_SQL_DIR

if ! docker compose -f "$PG_COMPOSE" up -d --wait postgres; then
    echo "ERROR: PostgreSQL container failed to start. Logs:"
    docker compose -f "$PG_COMPOSE" logs postgres 2>/dev/null || true
    exit 1
fi

if [ "$NO_BUILD" = false ]; then
    echo "==> Building Vannak with ipto-writer feature..."
    cargo test --features ipto-writer --test vannak_integration --no-run 2>&1 | tail -1
fi

echo "==> Running Ipto writer integration test..."
export VANNAK_PG_INTEGRATION=1
cargo test --features ipto-writer --test vannak_integration -- --test-threads=1 --nocapture 2>&1

echo ""
echo "==> Ipto writer test complete."
fi

# ---------------------------------------------------------------------------
# Load test with Ipto writer (smoke test)
# ---------------------------------------------------------------------------
if [ "$KAFKA_ONLY" = false ] && [ "${SKIP_LOAD:-false}" != true ]; then
    if [ "$NO_BUILD" = false ]; then
        echo "==> Building vannak-load with ipto-writer feature..."
        cargo build --features ipto-writer --bin vannak-load --release 2>&1 | tail -1
    fi
    echo "==> Running load test (1000 pipelines, with Ipto)..."
    cargo run --features ipto-writer --bin vannak-load --release -- \
        --pipelines 1000 --with-ipto 2>&1
    echo ""
    echo "==> Load test complete."
fi

# ---------------------------------------------------------------------------
# Kafka -> Sitas integration test
# ---------------------------------------------------------------------------
if [ "$RUN_KAFKA" = true ]; then
    echo ""
    echo "==> Starting Redpanda Kafka broker (port $VANNAK_KAFKA_PORT)..."
    docker compose -f "$KAFKA_COMPOSE" down -v 2>/dev/null || true
    if ! docker compose -f "$KAFKA_COMPOSE" up -d --wait redpanda; then
        echo "ERROR: Redpanda container failed to start. Logs:"
        docker compose -f "$KAFKA_COMPOSE" logs redpanda 2>/dev/null || true
        exit 1
    fi

    if [ "$NO_BUILD" = false ]; then
        echo "==> Building Kafka integration test..."
        cargo test --features kafka-client --test kafka_integration --no-run 2>&1 | tail -1
    fi

    echo "==> Running Kafka -> Sitas integration test..."
    export VANNAK_KAFKA_INTEGRATION=1
    export VANNAK_KAFKA_BROKERS="localhost:$VANNAK_KAFKA_PORT"
    cargo test --features kafka-client --test kafka_integration -- --test-threads=1 --nocapture 2>&1
    echo ""
    echo "==> Kafka smoke test complete."
fi

# ---------------------------------------------------------------------------
# Raft cluster integration test
# ---------------------------------------------------------------------------
if [ "$KAFKA_ONLY" = false ] && [ "$RUN_CLUSTER" = true ]; then
    echo ""
    echo "==> Building vannak cluster Docker image..."
    docker compose -f "$CLUSTER_COMPOSE" build --no-cache vannak-1 2>&1 | tail -3

    echo "==> Starting 3-node Vannak cluster (Raft port 10081, health port 9090)..."
    docker compose -f "$CLUSTER_COMPOSE" up -d 2>/dev/null || true

    echo "==> Waiting for leader election and health (30s)..."
    sleep 30

    echo "==> Checking cluster status..."
    ALL_OK=true
    for node in vannak-1 vannak-2 vannak-3; do
        echo -n "  $node raft: "
        if docker compose -f "$CLUSTER_COMPOSE" exec "$node" \
            vannak probe 127.0.0.1 10081 2>/dev/null; then
            echo ""
        else
            echo "(unreachable)"
            ALL_OK=false
        fi
        echo -n "  $node health: "
        health_port=$(docker compose -f "$CLUSTER_COMPOSE" port "$node" 9090 2>/dev/null | cut -d: -f2)
        if [ -n "$health_port" ] && curl -sf "http://127.0.0.1:${health_port}/health" > /dev/null 2>&1; then
            echo "healthy"
        else
            echo "(unreachable)"
            ALL_OK=false
        fi
    done

    RUNNING=$(docker compose -f "$CLUSTER_COMPOSE" ps --status running -q 2>/dev/null | wc -l | tr -d ' ')
    echo "  Cluster containers running: $RUNNING"

    if [ "$ALL_OK" = true ]; then
        echo "  Cluster healthy — all nodes responding on both Raft and health endpoints."
    else
        echo "  Cluster degraded — dumping node logs:"
        for node in vannak-1 vannak-2 vannak-3; do
            echo "  --- $node ---"
            docker compose -f "$CLUSTER_COMPOSE" logs "$node" 2>/dev/null | tail -5
        done
    fi

    echo "==> Cluster test complete."
fi

# ---------------------------------------------------------------------------
# Full-stack demo (PostgreSQL + Kafka + 3-node Vannak supervisor)
# ---------------------------------------------------------------------------
if [ "$RUN_FULL" = true ]; then
    echo ""
    echo "==> Building full-stack demo image..."
    docker compose -f "$FULL_COMPOSE" build --no-cache vannak-1 2>&1 | tail -3

    echo "==> Starting full-stack demo (PG + Kafka + 3x Vannak)..."
    docker compose -f "$FULL_COMPOSE" up -d --wait 2>&1

    echo "==> Waiting for Vannak health endpoints (60s)..."
    for i in $(seq 1 30); do
        if curl -sf "http://127.0.0.1:19090/health" > /dev/null 2>&1; then
            echo "  vannak-1 healthy after ${i}s"
            break
        fi
        sleep 2
    done

    echo "==> Health snapshot (node-1):"
    curl -sf http://127.0.0.1:19090/health 2>/dev/null | python3 -m json.tool 2>/dev/null || echo "(unreachable)"

    echo ""
    echo "==> Publishing test event to Kafka..."
    echo '{"processInstanceId":"test-1","processId":"test-pipeline","activityId":"step-1","status":"STARTED","eventType":"ACTIVITY_ENTERED","timestamp":"2026-07-01T10:00:00Z"}' | \
        docker exec -i vannak-full-kafka rpk topic produce process-events -f '%v' 2>/dev/null
    echo "  Published."

    sleep 3

    echo "==> Querying process instance..."
    curl -sf -X POST http://127.0.0.1:19090/query \
        -H 'Content-Type: application/json' \
        -d '{"type":"ProcessInstance","process_instance_id":"test-1"}' \
        2>/dev/null | python3 -m json.tool 2>/dev/null || echo "(unreachable)"

    echo "==> Full-stack demo complete."
fi

echo ""
echo "==> All integration tests complete."
