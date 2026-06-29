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
- **IpTo** metadata management: what datasets, schemas, attributes, lineage,
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
- Which IpTo repository instance owns the durable metadata for this data item?
- What changed before an incident started?
- Which process instance last produced or modified a dataset?
- Which cluster node owns the hot state, checkpoints, and sealed event segments
  for a pipeline or metadata placement shard?

## Current Status

This repository currently contains the dependency-free Rust core types and
reducers. It is not yet wired to Durga Kafka topics, IpTo PostgreSQL backends,
Sitas executors, or Raft.

Implemented so far:

- Durga-compatible process event mirror and conversion into Vannak events;
- process-state reducer for current instance view and activity durations;
- typed metadata references;
- single-node hot index for process events and metadata-impact lookup;
- data-individual metadata/provenance event model;
- passive and active metadata maps;
- domain-level `DataIndividualShardId` for IpTo placement;
- IpTo placement resolver;
- field-to-IpTo-attribute mapping;
- idempotent IpTo write payload construction;
- in-memory metadata outbox state for pending, acknowledged, failed, and retry
  behavior.

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
    which IpTo repository instance owns durable metadata for the data item
```

A Sitas shard may ingest, buffer, index, or retry a metadata event, but final
IpTo destination is selected by `DataIndividualShardId` through an explicit
IpTo placement resolver.

This matters because multiple IpTo PostgreSQL instances may exist with the same
schema/content model, while ownership is determined by data-individual domain
placement rather than executor placement.

## Durability Model

Process monitoring can be high-volume and recent-state oriented. Data-individual
metadata capture has a stronger requirement: it must not be silently lost.

The intended flow is:

```text
metadata event
    -> validate
    -> resolve DataIndividualShardId to IpToInstanceId
    -> map metadata fields to IpTo attributes/templates
    -> persist to durable outbox or write idempotently to IpTo
    -> acknowledge capture
    -> retry until acknowledged
```

The current code includes an in-memory outbox model. A later storage layer should
persist outbox entries to append-only segments before acknowledging capture when
loss is unacceptable.

## Crate Layout

```text
src/
  cluster.rs        Raft/control-plane boundary placeholders
  data.rs           data-individual metadata and provenance types
  durga.rs          Durga monitor compatibility types
  index.rs          dependency-free hot index
  ingest.rs         Vannak process event envelope
  ipto.rs           IpTo placement, mapping, write payload, and outbox model
  metadata.rs       typed metadata references
  observability.rs  owned snapshot types
  process.rs        process reducer and current-state view
  query.rs          query request/result types
  runtime.rs        Sitas integration boundary
  storage.rs        append-only segment boundary placeholders
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
- IpTo placement by `DataIndividualShardId`;
- metadata mapping into IpTo write payloads;
- metadata outbox duplicate detection and acknowledgment.

## Design Direction

Near-term growth order:

1. Keep the Durga-compatible process event model aligned with Durga monitor.
2. Stabilize data-individual identity and provenance metadata events.
3. Add persistent append-only event/outbox segments.
4. Add an IpTo writer adapter with idempotent writes.
5. Add Sitas-backed shard-local execution and query fanout.
6. Add Raft-backed placement maps, checkpoints, and segment manifests.
7. Add recovery and ownership-transfer semantics.

## Non-Goals

Vannak is not initially:

- a replacement workflow engine for Durga;
- a replacement metadata catalog for IpTo;
- a distributed SQL engine;
- a generic full-text log search product;
- a consensus-replicated event firehose;
- a storage system for arbitrary data payloads;
- a browser UI before the ingest/query/control APIs are coherent.

Vannak's intended role is narrower: a process-aware, metadata-aware operational
index and durable provenance delivery layer for data-pipeline systems.
