#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Vannak integration test runner.
#
# Starts infrastructure and runs integration tests. By default, runs the
# PostgreSQL/Ipto ingest→index→outbox→writer flow. With --cluster, also
# starts a 3-node vannak-node Raft cluster with DNS auto-discovery.
#
# Usage:
#   ./scripts/run-integration-test.sh               # PostgreSQL + Ipto writer
#   ./scripts/run-integration-test.sh --cluster     # PostgreSQL + Raft cluster
#   ./scripts/run-integration-test.sh --no-build
#   ./scripts/run-integration-test.sh --down
# ---------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PG_COMPOSE="$PROJECT_DIR/docker-compose.test.yml"
CLUSTER_COMPOSE="$PROJECT_DIR/docker-compose.cluster.yml"
export VANNAK_PG_PORT="${VANNAK_PG_PORT:-5432}"

NO_BUILD=false
DOWN_ONLY=false
RUN_CLUSTER=false

for arg in "$@"; do
    case "$arg" in
        --no-build) NO_BUILD=true ;;
        --down) DOWN_ONLY=true ;;
        --cluster) RUN_CLUSTER=true ;;
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
}

if [ "$DOWN_ONLY" = true ]; then
    compose_down
    exit 0
fi

trap compose_down EXIT

# ---------------------------------------------------------------------------
# PostgreSQL + Ipto writer integration test
# ---------------------------------------------------------------------------
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

# ---------------------------------------------------------------------------
# Load test with Ipto writer (smoke test)
# ---------------------------------------------------------------------------
if [ "$SKIP_LOAD" != true ]; then
    if [ "$NO_BUILD" = false ]; then
        echo "==> Building vannak-load with ipto-writer feature..."
        cargo build --features ipto-writer --bin vannak-load --release 2>&1 | tail -1
    fi
    echo "==> Running load test (100 pipelines, with Ipto)..."
    cargo run --features ipto-writer --bin vannak-load --release -- \
        --pipelines 100 --report-interval 50 --with-ipto 2>&1
    echo ""
    echo "==> Load test complete."
fi

# ---------------------------------------------------------------------------
# Raft cluster integration test
# ---------------------------------------------------------------------------
if [ "$RUN_CLUSTER" = true ]; then
    echo ""
    echo "==> Building vannak-node Docker image..."
    docker compose -f "$CLUSTER_COMPOSE" build --no-cache vannak-1 2>&1 | tail -3

    echo "==> Starting 3-node Vannak Raft cluster (port 10081)..."
    docker compose -f "$CLUSTER_COMPOSE" up -d 2>/dev/null || true

    echo "==> Waiting for leader election (15s)..."
    sleep 15

    echo "==> Checking cluster status..."
    ALL_OK=true
    for node in vannak-1 vannak-2 vannak-3; do
        echo -n "  $node: "
        if docker compose -f "$CLUSTER_COMPOSE" exec "$node" \
            /app/vannak-node probe 127.0.0.1 10081 2>/dev/null; then
            echo ""
        else
            echo "(unreachable)"
            ALL_OK=false
        fi
    done

    RUNNING=$(docker compose -f "$CLUSTER_COMPOSE" ps --status running -q 2>/dev/null | wc -l | tr -d ' ')
    echo "  Cluster containers running: $RUNNING"

    if [ "$ALL_OK" = true ]; then
        echo "  Cluster healthy — all nodes responding."
    else
        echo "  Cluster degraded — dumping node logs:"
        for node in vannak-1 vannak-2 vannak-3; do
            echo "  --- $node ---"
            docker compose -f "$CLUSTER_COMPOSE" logs "$node" 2>/dev/null | tail -5
        done
    fi

    echo "==> Cluster test complete."
fi

echo ""
echo "==> All integration tests complete."
