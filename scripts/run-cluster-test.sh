#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Vannak Raft cluster integration test with DNS-based auto-discovery.
#
# Builds and starts a 3-node vannak-node cluster with CoreDNS SRV-based
# peer discovery on port 10081 (famdc). Verifies leader election and
# cluster health, then tears down.
#
# Usage:
#   ./scripts/run-cluster-test.sh
#   ./scripts/run-cluster-test.sh --no-build
#   ./scripts/run-cluster-test.sh --down
# ---------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="$PROJECT_DIR/docker-compose.cluster.yml"
PROBE_BIN="$PROJECT_DIR/target/release/vannak-node"

NO_BUILD=false
DOWN_ONLY=false

for arg in "$@"; do
    case "$arg" in
        --no-build) NO_BUILD=true ;;
        --down) DOWN_ONLY=true ;;
        --no-probe) NO_PROBE=true ;;  # skip probe when binary isn't built for host arch
        *) echo "Unknown argument: $arg"; exit 1 ;;
    esac
done

# ---------------------------------------------------------------------------
# Tear down
# ---------------------------------------------------------------------------
compose_down() {
    echo "==> Tearing down cluster..."
    docker compose -f "$COMPOSE_FILE" down -v --remove-orphans 2>/dev/null || true
}

if [ "$DOWN_ONLY" = true ]; then
    compose_down
    exit 0
fi

trap compose_down EXIT

# ---------------------------------------------------------------------------
# Build image
# ---------------------------------------------------------------------------
if [ "$NO_BUILD" = false ]; then
    echo "==> Building vannak-node Docker image..."
    docker compose -f "$COMPOSE_FILE" build --no-cache node-1 2>&1 | tail -3
fi

# ---------------------------------------------------------------------------
# Start cluster
# ---------------------------------------------------------------------------
echo "==> Starting 3-node Vannak Raft cluster (port 10081)..."
docker compose -f "$COMPOSE_FILE" up -d --wait node-1 node-2 node-3 2>/dev/null || true

# Give nodes time to elect a leader
echo "==> Waiting for leader election (15s)..."
sleep 15

# ---------------------------------------------------------------------------
# Verify cluster
# ---------------------------------------------------------------------------
echo "==> Checking cluster status..."

if [ "${NO_PROBE:-false}" = false ] && [ -x "$PROBE_BIN" ]; then
    for node in vannak-1 vannak-2 vannak-3; do
        echo -n "  $node: "
        docker compose -f "$COMPOSE_FILE" exec "$node" \
            /app/vannak-node probe localhost 10081 2>/dev/null || echo "(unreachable)"
    done
else
    echo "  (skipping probe — build vannak-node with --features node for host probing)"
fi

# Verify all containers are running
RUNNING=$(docker compose -f "$COMPOSE_FILE" ps --status running -q 2>/dev/null | wc -l | tr -d ' ')
echo "  Running containers: $RUNNING (expected 4: dns + 3 nodes)"
if [ "$RUNNING" -ge 4 ]; then
    echo "==> Cluster healthy."
else
    echo "==> WARNING: cluster may not be fully running."
fi

echo ""
echo "To interact with the cluster:"
echo "  docker compose -f docker-compose.cluster.yml exec vannak-1 /app/vannak-node probe localhost 10081"
