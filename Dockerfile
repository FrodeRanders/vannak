# Vannak: multi-stage Docker build for the supervisor binary.
#
# Build:
#   docker build -t vannak:local .
#
# Run (standalone, no Kafka, single-node Raft):
#   docker run --rm vannak:local \
#     --raft 0.0.0.0 10081 vannak-1 /data \
#     --health 0.0.0.0:9090 \
#     --outbox /data/outbox.seg outbox-1 vannak-1

ARG RUST_VERSION=1.95

# ---------------------------------------------------------------------------
# Build stage
# ---------------------------------------------------------------------------
FROM rust:${RUST_VERSION}-slim-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config libssl-dev protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy Cargo workspace manifests and source — the build context is the parent
# directory (gautelis repo root), so paths are relative to that.
COPY vannak/Cargo.toml vannak/Cargo.lock* vannak/
COPY vannak/src/ vannak/src/
COPY vannak/tests/ vannak/tests/

# Copy sibling crate sources for path dependencies.
# The graft workspace root is needed for edition/dependency inheritance.
# We use the real Cargo.toml and then strip members we don't copy.
COPY raft/graft-rust/Cargo.toml raft/graft-rust/
COPY raft/graft-rust/graft-core raft/graft-rust/graft-core/
COPY raft/graft-rust/graft-runtime raft/graft-rust/graft-runtime/
COPY raft/graft-rust/graft-storage raft/graft-rust/graft-storage/
COPY raft/graft-rust/graft-transport raft/graft-rust/graft-transport/
COPY raft/graft-rust/graft-proto raft/graft-rust/graft-proto/
COPY raft/graft-rust/graft-telemetry raft/graft-rust/graft-telemetry/
RUN sed -i '/"graft-app-kv"/d; /"graft-tests"/d' raft/graft-rust/Cargo.toml
COPY sitas/Cargo.toml sitas/
COPY sitas/src/ sitas/src/
COPY ipto/implementations/rust ipto/implementations/rust/

RUN cargo build --manifest-path vannak/Cargo.toml --release \
        --features node,daemon --bin vannak \
    && mv vannak/target/release/vannak /usr/local/bin/vannak \
    && cargo clean --manifest-path vannak/Cargo.toml

# ---------------------------------------------------------------------------
# Runtime stage
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl dnsutils \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/bin/vannak /usr/local/bin/vannak

# Health check via the daemon health endpoint (default port 9090).
HEALTHCHECK --interval=5s --timeout=3s --retries=12 \
    CMD curl -sf http://127.0.0.1:9090/health || exit 1

ENTRYPOINT ["/usr/local/bin/vannak"]
