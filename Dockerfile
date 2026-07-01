# Multi-stage build for vannak-node.
# Build context is the parent of vannak/ (containing vannak/ and raft/).
FROM rust:1-bookworm AS builder
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY vannak/Cargo.toml vannak/Cargo.lock ./
COPY vannak/src/ src/
COPY vannak/tests/ tests/

COPY ipto/implementations/rust/ ../ipto/implementations/rust/
COPY ipto/shared/ ../ipto/shared/

COPY sitas/ ../sitas/

COPY raft/graft-rust/graft-proto/ ../raft/graft-rust/graft-proto/
COPY raft/graft-rust/graft-core/ ../raft/graft-rust/graft-core/
COPY raft/graft-rust/graft-storage/ ../raft/graft-rust/graft-storage/
COPY raft/graft-rust/graft-transport/ ../raft/graft-rust/graft-transport/
COPY raft/graft-rust/graft-runtime/ ../raft/graft-rust/graft-runtime/
COPY raft/graft-rust/graft-telemetry/ ../raft/graft-rust/graft-telemetry/
COPY raft/graft-rust/Cargo.toml ../raft/graft-rust/
COPY raft/raft-wire/ ../raft/raft-wire/
RUN cargo build --release --features node --bin vannak-node

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends dnsutils && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/vannak-node /app/vannak-node
EXPOSE 10081
ENTRYPOINT ["/app/vannak-node"]
