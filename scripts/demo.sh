#!/usr/bin/env bash
# Vannak full-stack demo orchestration script.
#
# Starts PostgreSQL + Redpanda + 3-node Vannak cluster, waits for readiness,
# produces test events, queries results, and optionally tears down.
#
# Usage:
#   ./scripts/demo.sh              Start everything and run the demo
#   ./scripts/demo.sh --down       Tear down and clean up
#   ./scripts/demo.sh --build      Force rebuild of Vannak image
#   ./scripts/demo.sh --status     Show container status + health snapshot
#
# Prerequisites:
#   - Docker and docker compose
#   - curl
#   - jq (optional, for pretty-printing)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="$PROJECT_DIR/docker-compose.full.yml"
HEALTH_PORT=19090
HEALTH_URL="http://127.0.0.1:${HEALTH_PORT}/health"
QUERY_URL="http://127.0.0.1:${HEALTH_PORT}/query"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log()  { echo -e "${GREEN}[demo]${NC} $*"; }
warn() { echo -e "${YELLOW}[demo]${NC} $*"; }
err()  { echo -e "${RED}[demo]${NC} $*"; }

# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

cmd_down() {
    log "Tearing down full-stack demo..."
    docker compose -f "$COMPOSE_FILE" down -v 2>/dev/null || true
    log "Done."
}

cmd_status() {
    log "Container status:"
    docker compose -f "$COMPOSE_FILE" ps --format 'table {{.Name}}\t{{.Status}}\t{{.Ports}}' 2>/dev/null || {
        warn "No containers running."
        return 0
    }
    echo
    log "Vannak node-1 health snapshot:"
    curl -sf "$HEALTH_URL" 2>/dev/null | python3 -m json.tool 2>/dev/null || \
        warn "(health endpoint not reachable at $HEALTH_URL)"
}

cmd_build() {
    log "Building Vannak image..."
    docker compose -f "$COMPOSE_FILE" build --no-cache vannak-1
}

cmd_up() {
    local build_flag=""
    if [[ "${1:-}" == "--build" ]]; then
        build_flag="--build"
    fi

    log "Starting full-stack demo (PostgreSQL + Redpanda + 3x Vannak)..."
    docker compose -f "$COMPOSE_FILE" up -d $build_flag

    log "Waiting for Vannak nodes to become healthy..."
    local max_wait=120
    local waited=0
    while [[ $waited -lt $max_wait ]]; do
        if curl -sf "$HEALTH_URL" > /dev/null 2>&1; then
            log "Vannak node-1 healthy after ${waited}s"
            break
        fi
        sleep 2
        waited=$((waited + 2))
        if [[ $((waited % 10)) -eq 0 ]]; then
            log "Still waiting (${waited}s/${max_wait}s)..."
        fi
    done

    if [[ $waited -ge $max_wait ]]; then
        err "Timeout waiting for health endpoint (${max_wait}s)."
        err "Container logs:"
        docker compose -f "$COMPOSE_FILE" logs --tail 20 vannak-1 2>/dev/null
        err "Try: docker compose -f $COMPOSE_FILE logs vannak-1"
        exit 1
    fi

    log "All nodes healthy. Running demo..."
}

cmd_demo() {
    cmd_up "$@"

    echo
    log "=== Step 1: Health snapshot (node-1) ==="
    curl -sf "$HEALTH_URL" | python3 -m json.tool 2>/dev/null || cat
    echo

    log "=== Step 2: Query cluster peers ==="
    for port in 19091 19093; do
        echo -n "  node on port $port: "
        if curl -sf "http://127.0.0.1:${port}/health" > /dev/null 2>&1; then
            echo "healthy"
        else
            echo "unreachable"
        fi
    done
    echo

    log "=== Step 3: Ingest process events (POST /ingest) ==="
    local events=(
        '{"process_instance_id":"order-1001","pipeline_id":"order-pipeline","activity_id":"validate","kind":"ActivityEntered","timestamp":"2026-07-01T10:00:00Z"}'
        '{"process_instance_id":"order-1001","pipeline_id":"order-pipeline","activity_id":"validate","kind":"ActivityCompleted","timestamp":"2026-07-01T10:00:01Z"}'
        '{"process_instance_id":"order-1001","pipeline_id":"order-pipeline","activity_id":"enrich","kind":"ActivityEntered","timestamp":"2026-07-01T10:00:02Z"}'
        '{"process_instance_id":"order-1001","pipeline_id":"order-pipeline","activity_id":"enrich","kind":"ActivityCompleted","timestamp":"2026-07-01T10:00:03Z"}'
        '{"process_instance_id":"order-1001","pipeline_id":"order-pipeline","kind":"ProcessCompleted","timestamp":"2026-07-01T10:00:04Z"}'
        '{"process_instance_id":"order-2001","pipeline_id":"order-pipeline","activity_id":"validate","kind":"ActivityEntered","timestamp":"2026-07-01T10:00:05Z"}'
        '{"process_instance_id":"order-2001","pipeline_id":"order-pipeline","kind":"ProcessFailed","timestamp":"2026-07-01T10:00:07Z"}'
    )

    for ev in "${events[@]}"; do
        local result=$(curl -sf -X POST "http://127.0.0.1:${HEALTH_PORT}/ingest" \
            -H 'Content-Type: application/json' \
            -d "$ev" 2>/dev/null)
        echo "  ingest $(echo "$ev" | python3 -c "import json,sys; d=json.load(sys.stdin); print(f'{d[\"process_instance_id\"]}/{d[\"kind\"]}')" 2>/dev/null || echo 'event'): $result"
    done
    log "Published ${#events[@]} events."

    log "=== Step 4: Query ingested data ==="
    sleep 1
    echo "  Process instance order-1001:"
    curl -sf -X POST "http://127.0.0.1:${HEALTH_PORT}/query" \
        -H 'Content-Type: application/json' \
        -d '{"type":"ProcessInstance","process_instance_id":"order-1001"}' \
        2>/dev/null | python3 -m json.tool 2>/dev/null || echo "(not found)"

    echo "  Pipeline query (order-pipeline):"
    curl -sf -X POST "http://127.0.0.1:${HEALTH_PORT}/query" \
        -H 'Content-Type: application/json' \
        -d '{"type":"Pipeline","pipeline_id":"order-pipeline","limit":10}' \
        2>/dev/null | python3 -m json.tool 2>/dev/null || echo "(empty)"

    echo "  Failed instances:"
    curl -sf -X POST "http://127.0.0.1:${HEALTH_PORT}/query" \
        -H 'Content-Type: application/json' \
        -d '{"type":"ProcessStatus","status":"Failed","limit":10}' \
        2>/dev/null | python3 -m json.tool 2>/dev/null || echo "(empty)"
    echo

    log "=== Step 5: Final health snapshot ==="
    curl -sf "$HEALTH_URL" | python3 -c "
import json,sys
d = json.load(sys.stdin)
print(f'  uptime:             {d[\"uptime_secs\"]}s')
print(f'  hot events:         {d[\"hot_index\"][\"event_count\"]}')
print(f'  process instances:  {d[\"hot_index\"][\"process_instance_count\"]}')
print(f'  outbox pending:     {d[\"outbox\"][\"pending\"]}')
print(f'  outbox acked:       {d[\"outbox\"][\"acknowledged\"]}')
print(f'  placement epoch:    {d[\"placement_epoch\"]}')
" 2>/dev/null
    echo

    log "Demo complete."
    log "  Kafka producer: docker exec -i vannak-full-kafka rpk topic produce process-events -f '%v'"
    log "  Vannak health:  curl http://127.0.0.1:19090/health | jq"
    log "  Tear down:      $0 --down"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

case "${1:-}" in
    --down)
        cmd_down
        ;;
    --status)
        cmd_status
        ;;
    --build)
        cmd_build
        ;;
    --help|-h)
        echo "Usage: $0 [--down|--status|--build|--help]"
        echo
        echo "  (no args)   Start the full-stack demo (up + produce events + query)"
        echo "  --down      Tear down and remove volumes"
        echo "  --status    Show container status and health snapshot"
        echo "  --build     Force rebuild of Vannak Docker image"
        echo "  --help      This message"
        ;;
    *)
        cmd_demo "$@"
        ;;
esac
