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

# AGENTS.md

## Project Purpose

Vannak explores a clustered operational knowledge plane for data pipelines and
metadata systems.

The project combines four existing lines of work:

- Sitas: a Rust-native shard-per-core runtime and service model;
- Durga: data-pipeline/process management with BPMN-like process state and
  monitoring;
- Ipto: metadata management and semantic object modeling;
- a Rust Raft implementation: replicated agreement for cluster control state.

The goal is not to build another generic log collector. Vannak should connect
runtime process events, metadata meaning, lineage, and cluster coordination so
operators can answer questions such as:

- Which pipelines are currently unhealthy?
- Which datasets or metadata objects are affected by this failure?
- What changed before this incident started?
- Which process instance last produced or modified this dataset?
- Which classified or governed data moved through which process step?
- What passive and active metadata has been captured for this specific data
  individual?
- Which Ipto repository instance owns the durable metadata for this data
  individual?
- What is the current cluster view of ownership, checkpoints, and sealed event
  segments?

## Design Intent

Vannak should grow as a real system, not only as a demo.

Preserve these design choices unless the current task explicitly revises the
architecture:

1. Keep high-volume event ingest out of the consensus hot path.
2. Use Raft for cluster control state, ownership, manifests, checkpoints, and
   metadata-version decisions.
3. Use Sitas for shard-local hot indexes, bounded ingest, explicit
   cross-shard transfer, and observable runtime behavior.
4. Use Durga process events as semantically meaningful state transitions, not
   as unstructured logs.
5. Use Ipto metadata to connect runtime events to datasets, schemas, lineage,
   ownership, classifications, contracts, and versions.
6. Treat data-individual metadata capture as a durable provenance plane, not
   merely as monitoring data.
7. Route writes to Ipto by domain metadata placement, such as
   `DataIndividualShardId`; do not confuse that with Sitas executor shard ids.
8. Do not acknowledge durable data-individual metadata capture until the event
   is safely persisted to an outbox/segment or written idempotently to Ipto.
9. Keep ordinary application state shard-local. Do not normalize service state
   behind `Arc<Mutex<_>>` as the main programming model.
10. Make cross-shard movement explicit through typed messages, typed submitter
   calls, or typed runtime handles.
11. Values crossing shard boundaries must be owned values with explicit
   `Send + 'static` boundaries where required.
12. Observability APIs should return owned snapshots.
13. Unsafe code and OS-specific code should remain isolated behind small safe
    APIs.

## Initial System Shape

The first useful system should be a single-node Vannak service with interfaces
that leave room for clustering:

- ingest structured Durga process events;
- ingest data-individual metadata events carrying passive and active metadata;
- enrich or correlate events with Ipto metadata references;
- maintain shard-local current pipeline/process-instance state;
- maintain recent event and metadata-impact indexes;
- maintain a durable outbox for metadata writes that must reach Ipto;
- route metadata writes to Ipto instances by data-individual placement;
- query by pipeline, process instance, dataset, metadata object, status, and
  time range;
- expose owned service and runtime snapshots;
- write append-only local event segments;
- reserve Raft integration for cluster state and sealed-segment manifests.

Do not start by replicating every event through Raft. That would make consensus
the throughput bottleneck before the system has proven its data model.

## Suggested Repository Layout

Use this layout unless a concrete implementation need suggests otherwise:

```text
src/
  ingest/          typed external event intake and validation
  process/         Durga process-event model and state reducers
  data/            data-individual identity, metadata events, provenance facts
  metadata/        Ipto adapter and metadata reference model
  ipto/            Ipto placement, mapping, writer, and durable outbox
  index/           shard-local indexes and query primitives
  query/           scatter/gather query APIs and result types
  storage/         append-only segment writer/reader and manifests
  cluster/         Raft-backed ownership, membership, checkpoints
  observability/   owned snapshots, status, metrics, diagnostics
  runtime/         Sitas integration boundaries
docs/
  ARCHITECTURE.md  detailed architecture and design notes
```

## Coding Rules

- Prefer stable Rust and edition 2024.
- Keep Sitas integration explicit. Do not hide Sitas shard ownership behind
  generic actor-style mailboxes.
- Prefer typed event enums and typed service APIs over unstructured maps.
- Treat Durga events as process facts with causality, activity identity, and
  lifecycle meaning.
- Treat Ipto metadata references as first-class identifiers, not opaque strings
  sprinkled through payloads.
- Treat data individuals as first-class identities. Metadata about a flowing
  data item should be attached to a stable `DataIndividualId`.
- Distinguish passive metadata (received time, source, format, checksum) from
  active metadata (masking, transformation, validation, enrichment, field
  changes).
- Make Ipto placement explicit and deterministic. A Sitas shard may process a
  metadata event, but `DataIndividualShardId` decides which Ipto instance owns
  the durable metadata.
- Persist metadata events to an outbox or segment before acknowledging capture
  when loss would be unacceptable.
- Keep Raft-facing state compact and intentional.
- Do not put raw event firehose traffic through Raft until the architecture
  explicitly requires it.
- Prefer append-only storage formats for raw event durability.
- Keep query results owned; do not expose borrowed references into shard-local
  state.
- Avoid adding dependencies until the system boundary that needs them is clear.

## Testing Responsibilities

When implementation begins, tests should cover:

- process-event validation;
- data-individual metadata-event validation;
- state reduction from ordered Durga events;
- out-of-order or duplicate event handling policy;
- shard routing and cross-shard query fanout;
- Ipto placement from data-individual shard id;
- durable metadata outbox replay and idempotent Ipto writes;
- passive and active metadata mapping into Ipto attributes/templates;
- metadata correlation and missing-metadata behavior;
- segment sealing and manifest creation;
- restart/recovery from local segments and Raft checkpoints;
- cluster ownership changes;
- bounded ingest and backpressure behavior;
- snapshot correctness without exposing live state.

## Documentation Responsibilities

Update `ARCHITECTURE.md` when changing:

- event model;
- process-state reducer semantics;
- data-individual identity and metadata-event semantics;
- metadata correlation model;
- Ipto placement and mapping rules;
- shard placement/routing;
- query model;
- storage segment format;
- Raft-replicated state;
- checkpoint/recovery semantics;
- observability fields;
- clustering assumptions.

Keep this `AGENTS.md` operational. Put detailed design reasoning in
`ARCHITECTURE.md`.

## Current Non-Goals

Do not implement these unless explicitly requested:

- distributed SQL;
- generic log-search product scope;
- distributed object storage;
- full workflow engine replacement for Durga;
- full metadata-authoring replacement for Ipto;
- consensus replication of every raw event;
- treating durable data-individual provenance as best-effort monitoring;
- coupling Ipto placement to Sitas executor shard ids;
- global load balancing before ownership and checkpoint semantics are clear;
- exactly-once external side effects;
- browser UI before the core ingest/query/control APIs are coherent.

## Design Direction

Grow Vannak in this order:

1. Define Durga-compatible process events and metadata-reference model.
2. Define data-individual identity, passive metadata, active metadata, and Ipto
   placement.
3. Build a single-node shard-local hot index.
4. Add typed query APIs and owned snapshots.
5. Add durable local segments/outbox for metadata events.
6. Add Ipto mapping and idempotent writer.
7. Add Raft-backed cluster control state.
8. Add checkpoint and recovery semantics.
9. Add richer metadata impact analysis.
10. Add cluster rebalancing and placement policy only after ownership is clear.
