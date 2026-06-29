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

# Vannak Architecture

## 1. Purpose

Vannak is a clustered operational knowledge plane for data pipelines and
metadata systems.

It combines:

- Durga-style pipeline/process execution state;
- Ipto-style metadata, lineage, and semantic object management;
- Sitas-style shard-local hot indexing and explicit message passing;
- Raft-backed replicated control state.

The system should answer operational questions that ordinary monitoring,
metadata catalogs, and workflow engines answer only partially:

- What is running?
- What failed?
- Which data or metadata objects are affected?
- What passive and active metadata has been captured for a specific data
  individual?
- What changed before the failure?
- Which process instance produced or modified a dataset?
- Which Ipto repository instance owns the durable metadata for this data
  individual?
- Which cluster node owns the hot state, checkpoints, and sealed event
  segments for a pipeline or metadata object?

The thesis is that process events become much more valuable when they are
joined with metadata meaning and kept in a low-latency shard-local index. A
second, equally important thesis is that metadata about individual flowing data
items must be captured durably as provenance facts, not treated as disposable
monitoring telemetry.

## 2. System Summary

Vannak ingests two related event streams:

- process events: Durga monitor lifecycle events describing what the pipeline
  is doing;
- data-individual metadata events: provenance facts describing what happened to
  each flowing data item.

It stores recent operational state in memory, writes raw event history and
metadata outbox entries to append-only segments, writes durable metadata to
Ipto, and uses Raft for cluster coordination.

```text
Durga pipeline runtimes
    |
typed process events + data-individual metadata events
    |
Vannak ingest API
    |
Sitas shard-local hot indexes
    |
metadata correlation and Ipto placement
    |
durable metadata outbox
    |
Ipto PostgreSQL instance selected by DataIndividualShardId
    |
query API / snapshots / alerting
    |
append-only event segments
    |
Raft-backed cluster control state
```

The system should not put every event through Raft. Raft is for decisions that
must be agreed across the cluster: membership, shard ownership, metadata version
activation, checkpoint manifests, sealed segment manifests, leases, and recovery
coordination.

High-volume event ingest should remain shard-local and append-only where
possible. Metadata capture that must not be lost needs a durable handoff:
either an idempotent write to the selected Ipto instance or a persisted outbox
entry that can be replayed.

## 3. Architectural Principles

### Separate the Hot Path from the Agreement Path

Event ingest can be high volume. Consensus is intentionally not the first
mechanism on that path.

The hot path:

```text
event -> validate -> route -> shard-local reducer/index -> segment writer
```

The agreement path:

```text
ownership decision -> Raft log -> committed cluster state
checkpoint decision -> Raft log -> committed manifest
sealed segment -> Raft log -> committed durable index of segments
```

This separation keeps latency and throughput reasonable while still giving the
cluster a durable, agreed view of ownership and recovery points.

### Treat Pipeline Events as Process Facts

Durga events should not be flattened into generic log lines. They represent
process facts:

- process definition;
- process instance;
- activity identity;
- activity lifecycle;
- transition;
- gateway decision;
- timer;
- retry;
- compensation;
- incident;
- completion.

The reducer for a process instance should use those facts to maintain a current
state model, not just a text index.

### Treat Metadata References as First-Class

Ipto metadata gives runtime events meaning. Events should carry typed references
to metadata objects where possible:

- dataset;
- schema;
- field;
- pipeline definition;
- data contract;
- owner;
- classification;
- lineage edge;
- environment;
- version.

The query layer should be able to answer impact questions by traversing or
consulting metadata relationships rather than scanning raw event payloads.

### Treat Data-Individual Provenance as Durable State

A flowing data item is not only an input to a process activity. It can carry
metadata that must survive the monitoring window:

- passive metadata: when and where it was created or received, source system,
  input topic/offset, format, schema, encoding, size, checksum, and
  classification hints;
- active metadata: transformations, masking, validation, enrichment, field
  additions/removals/changes, before/after checksums, and plugin identities.

This is provenance data. It should be captured as append-only facts and written
to Ipto through a durable outbox or equivalent idempotent path. It is related to
monitoring but must not depend on monitoring retention or best-effort delivery.

### Separate Domain Placement from Runtime Placement

Sitas shard ids identify executor/runtime placement. Ipto placement should be
based on domain ownership, for example `DataIndividualShardId`. These are
different axes.

```text
Sitas shard id
    where this event is currently processed

DataIndividualShardId
    which Ipto repository instance owns this individual's durable metadata
```

The runtime may use Sitas shards for buffering, indexing, and writer tasks, but
the final Ipto destination must be selected by an explicit placement resolver.

### Preserve Shard-Local Ownership

Sitas should own the hot mutable state on shards. Cross-shard interaction should
be explicit and typed. Query fanout is acceptable, but a query should receive
owned partial results rather than borrowed references into shard-local indexes.

### Make Snapshots Owned

Runtime and service observability should return owned snapshots:

- shard status;
- ingest lag;
- queue depth;
- current process counts;
- segment writer state;
- checkpoint progress;
- Raft control state view.

Snapshots must not expose live references into shard-local maps or indexes.

## 4. Core Domain Model

### Event Identity

Every ingested event should have enough identity to support deduplication,
ordering policy, and traceability.

Vannak's process event model is anchored to Durga's canonical monitor event
(`org.gautelis.durga.ProcessEvent`) and then extended with Vannak metadata
references. The Durga-compatible fields are:

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

Vannak adds ingestion and multi-tenant context around that event:

```text
event_id
source_id
source_sequence
tenant_id
environment_id
metadata_refs
metadata_version
causal_parent
```

`event_id` should be globally unique where possible. For Durga monitor events,
the initial Rust adapter derives a stable event id from `source_id`,
`process_instance_id`, and `source_sequence`. A later JSON/Kafka adapter can use
Kafka topic/partition/offset or a native event id if Durga adds one.

### Process Event Types

The initial process event categories should match Durga's monitor
`EventType`:

- `PROCESS_STARTED`;
- `ACTIVITY_ENTERED`;
- `ACTIVITY_COMPLETED`;
- `ACTIVITY_ESCALATED`;
- `ACTIVITY_CANCELLED`;
- `GATEWAY_TAKEN`;
- `PROCESS_COMPLETED`;
- `PROCESS_FAILED`.

Durga's coarse `Status` is also preserved:

- `STARTED`;
- `COMPLETED`;
- `FAILED`;
- `ESCALATED`;
- `CANCELLED`.

Vannak can add richer internal event categories later, but the Durga monitor
contract is the compatibility baseline. Additional categories should be adapter
extensions, not replacements for the Durga event model.

### Metadata Reference Types

Initial metadata reference categories:

- dataset id;
- schema id and version;
- field id;
- pipeline definition id and version;
- metadata object id;
- lineage edge id;
- data contract id;
- owner id;
- classification id.

The metadata adapter should resolve these through Ipto where available, but the
event should remain useful if metadata is temporarily unavailable.

### Data-Individual Metadata Events

Data-individual metadata events describe one flowing data item. They are not
only process-monitoring events, although they should carry enough process
context to correlate them with Durga activity state.

Suggested envelope:

```text
metadata_event_id
data_individual_id
data_individual_shard_id
tenant_id
environment_id
pipeline_id
process_instance_id
activity_id
timestamp
operation
passive_metadata
active_metadata
source_payload_ref
ipto_target
idempotency_key
```

Core identities:

- `DataIndividualId`: stable identity for the flowing data item;
- `DataIndividualShardId`: domain placement key for Ipto routing;
- `MetadataEventId`: identity of the provenance event;
- `IptoInstanceId`: selected metadata repository instance;
- `IptoPlacement`: mapping from domain shard id to repository endpoint.

Passive metadata examples:

- created timestamp;
- received timestamp;
- source system;
- source topic, partition, and offset;
- source file or object locator;
- format;
- content type;
- encoding;
- schema name and version;
- size in bytes;
- checksum;
- tenant and environment;
- classification hints.

Active metadata examples:

- transformed by activity;
- plugin name and version;
- operation type;
- masked fields;
- removed fields;
- added fields;
- changed fields;
- normalized fields;
- validation result;
- enrichment sources;
- before checksum;
- after checksum.

The exact metadata vocabulary can remain dynamic and Ipto-backed. Vannak should
still keep the envelope typed so routing, durability, idempotency, and
correlation are not inferred from arbitrary payload strings.

## 5. Sharding Model

The first routing strategy should be simple and explicit.

Candidate primary routes:

- `process_instance_id` for process-state ownership;
- `pipeline_id` for pipeline-current-state ownership;
- `dataset_id` or `metadata_object_id` for impact indexes;
- `data_individual_id` for recent provenance lookup;
- `data_individual_shard_id` for Ipto writer placement;
- time bucket plus key hash for event-history indexes.

A single event may update multiple logical indexes. The implementation should
avoid hidden shared mutable state by making each update explicit:

```text
ingest shard
    |
route to process owner
    |
route compact metadata-impact update if needed
    |
route segment writer update if separate
```

For a first implementation, keep process state and recent event index together
on the same shard. Add separate work-unit mailboxes for metadata impact indexes
only when there is a measured need.

Ipto writer work should route by `DataIndividualShardId`, not by the Sitas shard
that happened to ingest the metadata event. Multiple Sitas shards may enqueue
work for the same Ipto placement target.

## 6. Hot Indexes

Each shard can own:

- current state by process instance;
- current state by pipeline;
- recent event time buckets;
- status indexes;
- metadata reference indexes;
- unresolved metadata reference queue;
- local segment writer state;
- local statistics.

Example shard-local state:

```text
ShardLocal<VannakShardState>

VannakShardState
  process_instances
  pipeline_status
  recent_events_by_time
  events_by_metadata_ref
  metadata_events_by_data_individual
  metadata_outbox_status
  events_by_status
  segment_writer
  ingest_counters
```

Indexes should initially optimize for recent operational queries, not unlimited
historical search. Cold historical queries can be served from sealed segments
later.

## 7. Query Model

Queries should be typed and should return owned results.

Initial query families:

- get process instance state;
- list active or failed process instances by pipeline;
- find events for process instance;
- find events by metadata object;
- find datasets affected by pipeline failure;
- find pipelines touching a dataset or classification;
- find metadata events for a data individual;
- find data individuals touched by a process instance or activity;
- find failed or pending Ipto metadata writes by placement shard;
- get current shard and cluster snapshots;
- get segment/checkpoint status.

Point queries should route to one shard when possible. Broader queries should
fan out, collect bounded partial results, and merge deterministically.

Scatter/gather queries need explicit limits:

- time range;
- maximum result count;
- per-shard result count;
- continuation token;
- ordering key.

## 8. Storage Model

Raw event history and durable metadata handoff should use append-only local
segments.

Segment lifecycle:

1. Open segment for a shard or logical partition.
2. Append validated events.
3. Flush periodically or by size/time threshold.
4. Seal segment.
5. Build or persist segment summary.
6. Publish sealed segment manifest through Raft.

Segment manifests should include:

- segment id;
- owning shard or partition;
- node id;
- path or storage locator;
- first and last timestamp;
- event count;
- byte size;
- checksum;
- metadata reference summary if cheap;
- process/pipeline summary if cheap.

Raft should replicate the manifest, not the segment contents.

### Metadata Outbox

Data-individual metadata capture needs a stronger durability posture than
best-effort monitoring. The recommended flow is:

1. Validate metadata event.
2. Resolve `DataIndividualShardId` to an `IptoInstanceId`.
3. Persist an outbox entry or write idempotently to Ipto.
4. Acknowledge capture only after step 3 has succeeded according to the
   configured durability mode.
5. Retry pending outbox entries until Ipto acknowledges them.
6. Record committed write status and checkpoints.

Outbox entries should include:

- metadata event id;
- data individual id;
- data individual shard id;
- selected Ipto instance id;
- idempotency key;
- mapped Ipto unit/attribute payload;
- retry count;
- last error;
- write status.

Idempotency is required because replay after failure may resend the same
metadata event to Ipto.

The current Rust foundation includes a segment-backed metadata outbox boundary:

- `IptoWritePayload` has a deterministic dependency-free binary codec;
- `SegmentBackedMetadataOutbox` appends and syncs the encoded payload to a
  local segment before inserting it into the pending in-memory outbox;
- `replay_metadata_outbox_segment` rebuilds pending delivery state by reading
  payload records from a segment;
- `replay_metadata_outbox_segment_after` rebuilds only entries after a committed
  checkpoint offset and returns an owned `MetadataOutboxReplaySummary`;
- `MetadataOutboxSnapshot` and `SegmentBackedMetadataOutboxSnapshot` expose
  owned observability state without borrowing outbox internals;
- acknowledged segment-backed entries retain record offsets and can produce
  `MetadataOutboxCheckpoint` data for the Raft control plane.

This is intentionally only the local persistence, replay, and checkpoint-data
boundary. Retry backoff, writer leases, and checkpoint publication remain
separate concerns so they can be attached to Raft-controlled ownership without
changing code that produces metadata write payloads.

## 9. Raft-Controlled State

Raft should own compact cluster state:

- cluster membership;
- node identity;
- shard ownership;
- logical partition ownership;
- leadership or lease records;
- active metadata version;
- Ipto placement map from `DataIndividualShardId` ranges or buckets to
  `IptoInstanceId`;
- sealed segment manifests;
- metadata outbox checkpoint manifests;
- checkpoint manifests;
- recovery assignments;
- retention policy;
- schema/config version for Vannak itself.

Raft should not initially own:

- every raw event;
- every individual metadata write;
- every index mutation;
- every query result;
- every metric sample.

This boundary is important. If it blurs, Vannak becomes a slow replicated event
log instead of a fast operational index with agreed control state.

### Raft Boundary

Raft is for cluster agreement, not event throughput. The values committed
through Raft should be small, durable decisions that every node must interpret
the same way.

Good Raft payloads:

- `DataIndividualShardId` to `IptoInstanceId` placement maps;
- writer leases for an Ipto target or data-individual shard range;
- ownership epochs for process, metadata, or writer partitions;
- active metadata mapping versions;
- sealed segment manifests;
- metadata outbox checkpoints;
- recovery assignments after node failure;
- retention decisions for sealed segments and checkpoints;
- cluster-wide durability and degraded-mode policy.

Bad Raft payloads:

- raw Durga process events;
- data-individual metadata events;
- large data payloads;
- every hot-index mutation;
- every Ipto write attempt;
- query results;
- high-frequency metrics.

Those high-volume records should flow through Sitas-owned hot state,
append-only segments, outbox replay, and Ipto writers. Raft should commit the
metadata that makes those paths recoverable and unambiguous.

The first useful Raft-backed feature should be:

```text
Ipto placement map
    +
writer lease
    +
metadata outbox checkpoint
```

That gives the cluster three concrete guarantees:

1. all nodes agree where durable metadata for a data individual belongs;
2. only the lease holder actively writes a given Ipto target or placement range;
3. failover can resume replay from an agreed checkpoint instead of guessing.

## 10. Checkpoints and Recovery

A checkpoint records enough information to rebuild hot state without replaying
unbounded history.

Possible checkpoint contents:

- process-instance state snapshot for a shard;
- data-individual metadata outbox watermark;
- pipeline status summary;
- recent index watermark;
- segment offsets consumed;
- metadata version used for correlation;
- reducer version;
- checksum.

Checkpoint flow:

1. Shard creates owned checkpoint data.
2. Data is written to local or shared storage.
3. Checkpoint manifest is proposed through Raft.
4. Once committed, the checkpoint becomes a valid recovery point.

Recovery flow:

1. Read committed ownership and checkpoint manifests from Raft.
2. Load latest valid checkpoint for assigned shard.
3. Replay sealed and open segment data after checkpoint watermark. For metadata
   outbox recovery, replay can skip records at or before the committed
   acknowledged segment offset.
4. Resume ingest.

## 11. Metadata Correlation and Ipto Persistence

The metadata adapter should isolate Ipto integration for two related but
different responsibilities:

- correlation: resolving metadata references and answering impact questions;
- persistence: writing durable data-individual metadata/provenance facts to the
  correct Ipto repository instance.

Responsibilities:

- resolve metadata references;
- expose lineage and dependency queries;
- expose classification and ownership;
- expose metadata version identity;
- handle missing or stale metadata;
- return owned metadata snapshots or compact derived facts;
- resolve `DataIndividualShardId` to an `IptoInstanceId`;
- map data-individual metadata event fields to Ipto units, attributes, records,
  and relations;
- write mapped metadata idempotently;
- expose durable write status for outbox replay.

The current code exposes the first writer boundary as `IptoWriter`. It is a
synchronous trait over an owned `IptoWritePayload`, with retryable/permanent
write errors and `deliver_next_pending` to move one pending outbox entry to
acknowledged or failed. `drain_pending_outbox` adds a bounded delivery loop that
can later become the inner unit of a Sitas shard-local writer task. A real
adapter should implement this trait against the Rust Ipto port or direct
PostgreSQL repositories. Async execution, batching, backoff, and target-specific
writer tasks should be layered outside this trait so the domain outbox remains
independent of the runtime.

Events should record the metadata version used for enrichment when possible.
This makes later analysis reproducible:

```text
event E was correlated using metadata version V
```

When metadata is missing, the event should still be accepted if it is otherwise
valid. Missing references can be placed in a shard-local unresolved queue and
reprocessed when metadata changes.

When an Ipto repository instance is unavailable, data-individual metadata
capture should remain durable through the outbox. The system may report degraded
write status, but it should not silently drop metadata events.

### Mapping Flowing Data to Ipto

Ipto is dynamic with respect to content. Vannak should therefore support a
mapping layer from names in the flowing data individual to Ipto attributes or
records.

Example mapping intent:

```text
data.customer.email      -> attr:customer.email
data.order.total         -> attr:order.total
metadata.source_system   -> attr:provenance.source_system
metadata.received_at     -> attr:provenance.received_at
mask.customer.email      -> attr:provenance.masked_field
transform.plugin_name    -> attr:provenance.transform_plugin
```

Mappings should be versioned and observable. A metadata event should be able to
record which mapping version produced the Ipto write payload.

## 12. Durga Integration

The Durga adapter converts canonical Durga monitor events into Vannak process
events. The Rust core currently mirrors the Durga monitor envelope in
`src/durga.rs` and converts it into the internal `PipelineEvent` used by the hot
index.

The adapter should preserve:

- process instance identity;
- process id;
- activity identity;
- token identity;
- correlation id;
- business key;
- process version;
- lifecycle transition;
- coarse status;
- structured error message/code;
- timestamps;
- payload;
- metadata references emitted or derived by the pipeline;
- data-individual identifiers and metadata emitted by activities when present.

Durga remains the process-management system. Vannak observes, indexes,
correlates, and answers operational questions.

Durga activities that transform or mask data should be encouraged to emit
data-individual metadata events in addition to ordinary process lifecycle
events. The process lifecycle says the activity ran; the metadata event says
what happened to a specific data item.

## 13. Sitas Integration

Sitas is the hot-state runtime.

Useful Sitas mechanisms:

- `ShardedExecutor` for shard-per-thread async execution;
- `ShardLocal<T>` for owned shard-local state;
- `ShardMailboxSet<M>` or work-unit mailboxes for typed event transfer;
- `ShardedSubmitter` for explicit cross-shard work;
- owned snapshots for observability;
- CPU placement and future NUMA memory placement for hot indexes;
- bounded queues and backpressure for ingest control.

For Ipto persistence, useful Sitas shapes are:

- one or more shard-local metadata outbox queues;
- work-unit mailboxes keyed by `IptoInstanceId` or `DataIndividualShardId`;
- bounded writer tasks per Ipto target;
- owned snapshots for pending, failed, and acknowledged metadata writes. [done:
  local outbox and segment-backed outbox snapshots]

NUMA guidance:

- construct large hot indexes on their owning shard;
- avoid first-touching large long-lived buffers on a coordinator thread;
- keep mailbox payloads compact where possible;
- use destination-owned buffer construction for large long-lived state when
  such helpers exist.

## 14. Observability

Vannak must observe itself.

Snapshots should include:

- ingest accepted/rejected counts;
- validation errors;
- duplicate or out-of-order event counts;
- per-shard queue depth;
- per-shard reducer lag;
- process instances by state;
- metadata references unresolved;
- metadata outbox pending/failed/acknowledged counts;
- Ipto write latency and retry counts;
- Ipto placement map version;
- segment writer state;
- sealed segment count;
- checkpoint age;
- Raft commit index and applied index;
- shard ownership view;
- runtime placement status.

Snapshots should be owned values and safe to serialize.

## 15. Failure Handling

Initial policies should be explicit:

- duplicate event: ignore or record duplicate counter;
- out-of-order event: buffer, reject, or apply reducer-specific policy;
- missing metadata: accept event with unresolved reference;
- Ipto placement missing: reject or park metadata event according to configured
  policy;
- Ipto write failure: keep durable outbox entry pending and retry with
  idempotency key;
- segment write failure: mark shard degraded and stop accepting affected
  partition if durability is required;
- Raft unavailable: continue local hot ingest only if configured to allow
  degraded mode;
- ownership change: stop accepting writes for old owner before new owner
  replays checkpoint/segments.

These policies should be typed configuration, not scattered conditionals.

## 16. MVP Plan

### Phase 1: Single-Node Hot State

- Define typed event model.
- Define metadata reference model.
- Build single-node Sitas ingest service.
- Maintain current process-instance state.
- Query by process instance and pipeline.
- Expose owned snapshots.

### Phase 2: Metadata-Aware Indexing

- Add Ipto adapter boundary.
- Index events by metadata object and dataset.
- Add impact queries.
- Track metadata version used for enrichment.
- Add unresolved-reference handling.

### Phase 3: Data-Individual Metadata Capture

- Define `DataIndividualId`, `DataIndividualShardId`, and metadata event model.
- Separate passive and active metadata facts.
- Add Ipto placement resolver.
- Add mapping from data names to Ipto attributes/templates.
- Add idempotency keys for metadata writes.

### Phase 4: Local Durability

- Add append-only segment writer. [done: local checksummed segment writer]
- Add durable metadata outbox. [done: segment-backed payload enqueue/replay]
- Seal segments by size/time.
- Persist segment summaries.
- Rebuild hot state from segments.

### Phase 5: Ipto Writer

- Add writer boundary. [done: minimal `IptoWriter` trait and delivery helpers]
- Add writer tasks per Ipto target.
- Replay pending outbox entries. [partly done: single-entry delivery helper]
- Track acknowledged metadata writes. [partly done: segment offsets and
  checkpoint data for acknowledged durable entries]
- Surface degraded Ipto placement/write status.

### Phase 6: Raft Control Plane

- Add node identity and membership.
- Add shard/partition ownership.
- Replicate Ipto placement map.
- Replicate sealed segment manifests.
- Replicate metadata outbox checkpoint manifests.
- Replicate checkpoint manifests.
- Add recovery flow from committed manifests.

### Phase 7: Cluster Operation

- Add ownership transfer.
- Add degraded-mode policy.
- Add retention policy.
- Add cluster snapshots.
- Add placement-aware routing.

## 17. Non-Goals

Vannak is not initially:

- a workflow engine replacing Durga;
- a metadata catalog replacing Ipto;
- a distributed SQL engine;
- a generic full-text log search product;
- a consensus-replicated event firehose;
- a global scheduler;
- a storage system for arbitrary blobs.
- the source of truth for arbitrary data payload content.

It is a runtime and metadata-aware operational index for data-pipeline state,
plus a durable provenance capture and delivery layer for data-individual
metadata.

## 18. Open Questions

- Should Durga add a native event id, or should Vannak continue deriving one
  from source identity and stream position?
- Which Ipto identifiers are stable enough to embed directly in events?
- What is the canonical identity scheme for data individuals?
- How is `DataIndividualShardId` assigned and kept stable?
- What is the first Ipto placement-map format?
- Which passive metadata fields should be part of the core envelope?
- Which active metadata operations should be standardized first?
- How are flowing-data names mapped to Ipto attributes and records?
- What durability mode is required before acknowledging metadata capture?
- What ordering guarantees can pipeline event producers provide?
- Should process reducers be strict state machines or tolerant state repairers?
- Which state must be durable before an ingest acknowledgment is returned?
- What is the first segment format?
- What is the minimum Raft state required for useful clustering?
- How should metadata version changes reprocess unresolved events?
- How much historical query should hot indexes support before reading segments?

These questions should be answered with narrow prototypes rather than broad
framework design.
