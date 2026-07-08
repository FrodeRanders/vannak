<!--
Copyright (C) 2026 Frode Randers
All rights reserved

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Vannak

Vannak is an experimental operational knowledge plane for data pipelines and
metadata systems.

It is meant to connect four related capabilities:

- **Durga** process monitoring: what pipeline process or activity is running,
  completed, failed, escalated, or cancelled;
- **Ipto** metadata management: what datasets, schemas, attributes, lineage,
  classifications, contracts, and metadata objects mean;
- **Sitas** shard-local runtime structure: hot in-memory indexes, explicit
  message passing, owned snapshots, and later CPU/NUMA-aware placement;
- **Raft** cluster control: agreed ownership, placement maps, checkpoints, and
  sealed-segment manifests.

The goal is not another generic log collector. Vannak is intended to answer
operational questions that need process state, data provenance, metadata
meaning, and cluster ownership at the same time.

Examples:

- Which pipelines are unhealthy right now?
- Which datasets or metadata objects are affected by a failed process activity?
- What passive and active metadata has been captured for this specific data
  individual?
- Which Ipto repository instance owns the durable metadata for this data item?
- What changed before an incident started?
- Which process instance last produced or modified a dataset?
- Which cluster node owns the hot state, checkpoints, and sealed event segments
  for a pipeline or metadata placement shard?

## Current Status

This repository currently contains the dependency-free Rust core types,
reducers, a single-node service orchestration boundary, and optional adapters
behind feature flags. The standalone Kafka runner can feed Sitas workers; the
node binary does not yet supervise that runner.

Implemented so far:

- Durga-compatible process event mirror and conversion into Vannak events;
- process-state reducer for current instance view and activity durations;
- typed metadata references;
- single-node hot index for process events, status/time-range queries, and
  metadata-impact lookup;
- dependency-free shard-local runtime model with deterministic process-instance
  routing, bounded per-shard ingest queues, owned shard/queue snapshots, and
  fanout for pipeline, status, time-range, and metadata-impact queries;
- feature-gated Sitas runtime adapter (`sitas-runtime`) using
  `ShardedExecutor`, `ShardLocal<HotIndex>`, `ShardMailboxSet<PipelineEvent>`,
  direct shard submission, mailbox backpressure, explicit shard drains,
  continuous shard-local mailbox workers, fanout queries, and owned
  executor/mailbox/hot-index snapshots;
- feature-gated Kafka-shaped ingest boundary that carries topic, partition, and
  offset identity, appends process events to the journal when configured, feeds
  Sitas mailboxes, retains pending records across backpressure retries, and
  returns the next committable offset;
- feature-gated `rdkafka` consumer and `vannak-kafka-ingest` runner
  (`kafka-client`) that subscribes to qualified Durga process-event topics
  (`process-events-{processId}`), decodes either Vannak binary process-event
  payloads or Durga JSON monitor events, reads with `read_committed` isolation
  so aborted Durga worker transactions stay invisible, feeds Sitas workers,
  pauses partitions on Sitas mailbox backpressure, resumes them after
  admission, records Kafka assignment/revocation callbacks, and commits offsets
  after durable/Sitas admission;
- opt-in Kafka smoke test (`kafka_integration`) and Redpanda Compose file for
  verifying the Kafka-to-Sitas path against a live broker;
- data-individual metadata/provenance event model;
- local data-individual provenance index for recent metadata lookup by data
  individual, process instance, and activity;
- passive and active metadata maps;
- domain-level `DataIndividualShardId` for Ipto placement, with a deterministic
  hash-derivation helper (`from_data_individual`);
- consistent-hash ring placement (`IptoPlacementRing`) with configurable virtual
  nodes per Ipto instance, plus optional range overrides for explicit placement;
- Raft-replicable `IptoPlacementMap` combining the ring with optional operator
  overrides;
- placement map history with epoch-based query fallback in `ClusterControlState`;
- field-to-Ipto-attribute mapping;
- idempotent Ipto write payload construction;
- in-memory metadata outbox state for pending, acknowledged, failed, and retry
  behavior;
- append-only segment storage with checksummed records;
- append-open segment recovery that validates existing records before appending
  new records after a restart;
- deterministic Ipto write-payload codec;
- segment-backed metadata outbox enqueue and replay;
- segment-backed metadata outbox recovery that rebuilds pending entries after a
  checkpoint and continues appending to the recovered segment;
- minimal `IptoWriter` boundary and bounded drain helper for delivering pending
  outbox payloads into a concrete metadata writer;
- target-aware outbox delivery helpers for writer-lease-controlled drains;
- `VannakService`, an in-process single-node service boundary that ingests
  process events, resolves metadata placement, maps metadata events, durably
  enqueues Ipto write payloads, drains a target only when the caller holds the
  writer lease, and exposes owned service snapshots;
- owned metadata outbox snapshots for pending, failed, acknowledged, and segment
  state;
- segment-offset tracking for acknowledged durable outbox entries, allowing
  checkpoint data to be proposed through the cluster control plane;
- checkpoint-aware outbox segment replay that skips acknowledged offsets and
  returns an owned replay summary;
- optional `ipto-writer` feature flag with an `IptoWriter` implementation
  (`IptoRepoWriter`) backed by `ipto_rust::RepoService` (PostgreSQL);
- optional `daemon` feature flag with structured JSON parsing for the health,
  ingest, and query service;
- deterministic correlation-id (UUID v7) generation from `IdempotencyKey` for
  idempotent Ipto writes;
- built-in PROV-O SDL schema for provenance attribute and template registration;
- optional `raft` feature flag with `ClusterStateMachine`: a `graft_core::StateMachine`
  adapter wrapping `ClusterControlState` behind `std::sync::RwLock`, with JSON
  command/snapshot encoding and 7 read-query types;
- `CheckpointManifest` for Raft-replicated shard recovery checkpoints,
  recording segment consumption progress per checkpoint epoch.

## Two Event Planes

Vannak separates two related streams.

### Process Events

Process events are Durga monitor lifecycle events. They describe what the
pipeline process is doing.

The Rust compatibility model mirrors Durga's canonical
`org.gautelis.durga.ProcessEvent` shape:

```text
process_instance_id
process_id
activity_id
token_id
correlation_id
payload
status
error
event_type
process_version
business_key
timestamp
```

Vannak adds ingestion context such as source identity, tenant, environment,
metadata references, and causal parent.

### Data-Individual Metadata Events

Data-individual metadata events describe what happened to one flowing data item.
They are provenance facts, not merely monitoring telemetry.

They capture passive metadata such as:

- created or received timestamps;
- source system, topic, partition, offset, file, or object locator;
- format, content type, encoding, schema, and version;
- size, checksum, tenant, environment, and classification hints.

They also capture active metadata such as:

- transformation plugin and version;
- masking operations;
- validation results;
- enrichment sources;
- added, removed, changed, masked, or normalized fields;
- before and after checksums.

Durga lifecycle events can say that an activity ran. Data-individual metadata
events say what happened to a specific data item during or around that activity.

## Placement Model

Vannak deliberately separates runtime placement from metadata ownership.

```text
Sitas shard id
    where work is currently processed

DataIndividualShardId
    which Ipto repository instance owns durable metadata for the data item
```

A Sitas shard may ingest, buffer, index, or retry a metadata event, but final
Ipto destination is selected by `DataIndividualShardId` through an explicit
Ipto placement resolver.

This matters because multiple Ipto PostgreSQL instances may exist with the same
schema/content model, while ownership is determined by data-individual domain
placement rather than executor placement.

## Durability Model

Process monitoring can be high-volume and recent-state oriented. Data-individual
metadata capture has a stronger requirement: it must not be silently lost.

The intended flow is:

```text
metadata event
    -> validate
    -> resolve DataIndividualShardId to IptoInstanceId
    -> map metadata fields to Ipto attributes/templates
    -> persist to durable outbox or write idempotently to Ipto
    -> acknowledge capture
    -> retry until acknowledged
```

The current code includes both an in-memory outbox model and a segment-backed
outbox wrapper. The durable wrapper appends and syncs the encoded Ipto write
payload before making it visible as pending in memory, and replay can rebuild
pending delivery state from the segment. Process events also have a typed
`ProcessEventJournal` that appends and syncs encoded `PipelineEvent` records,
replays them into owned events, and can reopen validated segments for append.
`VannakService` wires this into an opt-in durable process-ingest path and a
single-node metadata capture path, and can drain a specific Ipto target only
when the caller holds the target's writer lease. The `vannak` supervisor
discovers existing segment files at startup, recovers existing metadata outbox
and process-event journal segments for append, initializes its local control
state from the same placement map used by the service, and can start a
PostgreSQL-backed background Ipto writer with `--with-ipto` when built with
`ipto-writer`. The supervisor handles Ctrl-C/SIGTERM by setting the shared
daemon, Raft, Kafka, and writer stop token. Background writer drains publish
acknowledged metadata outbox checkpoints into the local control state.

## Supervisor Daemon

Build the combined Raft/daemon binary with:

```bash
cargo build --features node,daemon --bin vannak
```

To enable the concrete Ipto writer in that process, include `ipto-writer` and
pass `--with-ipto`:

```bash
cargo build --features node,daemon,ipto-writer --bin vannak
target/debug/vannak --outbox ~/.local/state/vannak/metadata-outbox.seg default-outbox vannak \
  --journal ~/.local/state/vannak/process-events.seg \
  --with-ipto --ipto-tenant 1
```

If the configured outbox or journal file already exists, the supervisor
validates and reopens it for append. The HTTP daemon parses `/ingest` and
`/query` bodies as JSON and rejects unknown event kinds or process statuses
instead of silently defaulting them. When the background writer is enabled,
`/health` reports live writer cycles and totals while the loop is running.
`GET /` and `GET /health` are the health routes; other paths return `404`.
Health bind failures are returned as startup errors. If `--outbox` is omitted,
the default path is under the Raft data directory when Raft is configured,
otherwise under `$HOME/.local/state/vannak`, and directory creation failures
abort startup. The HTTP server rejects POST bodies larger than
`--max-request-body-bytes` (default 1 MiB). Numeric limits such as
`--writer-interval` and `--max-request-body-bytes` must be greater than zero,
and `--placement` must contain an epoch followed by complete target/vnode
pairs. `--with-ipto` starts in degraded mode if the writer cannot initialize;
add `--require-ipto` to fail startup instead.

## Crate Layout

```text
src/
  cluster.rs        Raft/control-plane boundary: consistent-hash ring placement,
                    writer leases, checkpoints, placement history with fallback
  data.rs           data-individual metadata and provenance types
  durga.rs          Durga monitor compatibility types
  index.rs          dependency-free hot index
  ingest.rs         Vannak process event envelope and process-event journal
  ipto.rs           Ipto placement, mapping, write payload, and outbox model
  kafka_ingest.rs   Kafka-shaped Sitas ingest boundary (feature:
                    sitas-runtime)
  kafka_client.rs   rdkafka-backed process-event consumer (feature:
                    kafka-client)
  metadata.rs       typed metadata references
  observability.rs  owned snapshot types
  process.rs        process reducer and current-state view
  query.rs          query request/result types
  runtime.rs        dependency-free shard-local runtime, bounded ingest,
                    and Sitas boundary
  sitas_runtime.rs  Sitas-backed executor/mailbox adapter (feature:
                    sitas-runtime)
  service.rs        single-node ingest/capture/outbox orchestration boundary
  storage.rs        append-only segment writer/reader and manifests
```

More detailed design notes are in [ARCHITECTURE.md](ARCHITECTURE.md).

Agent and development instructions are in [AGENTS.md](AGENTS.md).

## Build and Validate

Requires stable Rust with edition 2024 support.

```bash
cargo fmt --check
cargo test
cargo clippy --all-targets -- -D warnings
```

Current test coverage exercises:

- Durga process-event conversion;
- process state reduction;
- metadata impact indexing;
- duplicate event idempotency;
- consistent-hash ring placement with virtual nodes;
- ring determinism and reconfiguration redistribution;
- placement map overrides taking priority over the ring;
- placement history and epoch-based query fallback;
- metadata mapping into Ipto write payloads;
- metadata outbox duplicate detection and acknowledgment;
- segment-backed metadata outbox replay;
- Ipto writer success/failure handling;
- bounded outbox draining;
- metadata outbox snapshot counts.

## Design Direction

Near-term growth order:

1. Keep the Durga-compatible process event model aligned with Durga monitor.
2. Stabilize data-individual identity and provenance metadata events.
3. Add persistent append-only event/outbox segments.
4. Add an Ipto writer adapter with idempotent writes.
5. Expand the feature-gated Sitas adapter into the service/node runtime path
   and add background mailbox workers where needed.
6. Add Raft-backed placement maps, checkpoints, and segment manifests.
7. Add recovery and ownership-transfer semantics.

## Non-Goals

Vannak is not initially:

- a replacement workflow engine for Durga;
- a replacement metadata catalog for Ipto;
- a distributed SQL engine;
- a generic full-text log search product;
- a consensus-replicated event firehose;
- a storage system for arbitrary data payloads;
- a browser UI before the ingest/query/control APIs are coherent.

Vannak's intended role is narrower: a process-aware, metadata-aware operational
index and durable provenance delivery layer for data-pipeline systems.
