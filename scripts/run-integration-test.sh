#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Vannak integration test runner.
#
# Starts a PostgreSQL container with the Ipto schema, runs the Vannak
# integration test, and tears down. The test exercises the full
# ingest → index → outbox → Ipto writer flow.
#
# Usage:
#   ./scripts/run-integration-test.sh          # default port 5432
#   VANNAK_PG_PORT=15432 ./scripts/run-integration-test.sh
#   ./scripts/run-integration-test.sh --no-build
#   ./scripts/run-integration-test.sh --down   # tear down only
# ---------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
export VANNAK_PG_PORT="${VANNAK_PG_PORT:-5432}"

NO_BUILD=false
DOWN_ONLY=false

for arg in "$@"; do
    case "$arg" in
        --no-build) NO_BUILD=true ;;
        --down) DOWN_ONLY=true ;;
        *) echo "Unknown argument: $arg"; exit 1 ;;
    esac
done

# ---------------------------------------------------------------------------
# Tear down
# ---------------------------------------------------------------------------
compose_down() {
    echo "==> Tearing down PostgreSQL container..."
    docker compose -f "$PROJECT_DIR/docker-compose.test.yml" down -v 2>/dev/null || true
}

if [ "$DOWN_ONLY" = true ]; then
    compose_down
    exit 0
fi

# Clean up on exit
trap compose_down EXIT

# ---------------------------------------------------------------------------
# Start PostgreSQL (fresh each time)
# ---------------------------------------------------------------------------
echo "==> Starting fresh PostgreSQL (port $VANNAK_PG_PORT)..."
docker compose -f "$PROJECT_DIR/docker-compose.test.yml" down -v 2>/dev/null || true
docker compose -f "$PROJECT_DIR/docker-compose.test.yml" up -d --wait postgres

# ---------------------------------------------------------------------------
# Build (if needed)
# ---------------------------------------------------------------------------
if [ "$NO_BUILD" = false ]; then
    echo "==> Building Vannak with ipto-writer feature..."
    cargo test --features ipto-writer --test vannak_integration --no-run 2>&1 | tail -1
fi

# ---------------------------------------------------------------------------
# Run integration test
# ---------------------------------------------------------------------------
echo "==> Running Vannak integration test..."
export VANNAK_PG_INTEGRATION=1

cargo test --features ipto-writer --test vannak_integration -- --test-threads=1 --nocapture 2>&1

echo ""
echo "==> Integration test complete."