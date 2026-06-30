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

# Shard Placement and Ipto Cluster Reconfiguration

## Status

Draft, 2026-06-30.

## 1. The Question

When the Ipto cluster is reconfigured — an instance is added or removed — the
mapping from `DataIndividualShardId` to `IptoInstanceId` changes. Data written
under the old scheme lives on an old instance. Data written under the new scheme
goes to a new instance. A query for a given data individual must find its
metadata regardless of which placement epoch governed the write.

The question is: how should Vannak handle this transitional state without
immediate rebalancing of existing metadata between Ipto instances?

## 2. What Raft Consensus Is Actually About Here

Consensus is not about agreeing on the placement of _each_ shard ID. It is about
agreeing on the _scheme_ that maps ranges of shard IDs to Ipto instances.

```
Not:  Raft agrees that ShardId(42) → Ipto instance A
But:  Raft agrees that ShardId[0..49] → A, ShardId[50..99] → B
```

The shard ID for a given data individual is derived from a stable hash of the
`DataIndividualId`. It does not change across epochs. What changes is the
placement map that resolves that shard ID to a concrete Ipto instance.

Vannak already models this:
- `DataIndividualShardId` (in `data.rs`) — stable domain placement key
- `IptoPlacementMap` with `PlacementEpoch` (in `cluster.rs`) — versioned
  range-to-instance mapping, replicated through Raft
- `IptoPlacementRange { start, end, target }` — contiguous shard ranges
  assigned to one Ipto instance

When a new Ipto instance joins, Raft commits a new placement map with a higher
epoch. The map may split an existing range: `[0..99] → A` becomes
`[0..49] → A` and `[50..99] → B`. The consensus is on _this new split_. After
commit, all nodes agree that new writes for `ShardId(60)` go to B. The question
is what to do about existing data for `ShardId(60)` that already lives on A.

## 3. The Teradata Analogy (and Why It Partially Applies)

In Teradata, data rows are hash-distributed across Access Module Processors
(AMPs). When you join two tables, rows that share a hash value on the join
column co-locate on the same AMP. If they do not, data is redistributed across
the Banyan interconnect to the correct AMP, the join runs, and the result is
sent back.

The relevant insight: redistribution is a _query-time operation_, not a
_storage-time migration_. Teradata does not automatically rebalance tables when
AMPs are added. Redistribution happens during joins because the query engine
knows both the current hash scheme and where data should be for the join to
work.

Vannak is not joining anything. But the principle applies: the query layer can
know about placement epochs and route accordingly. Data does not need to move
before it can be found.

A better analogy may be DynamoDB or Cassandra, where consistent hashing with
virtual nodes means adding a node only affects a subset of the token range,
and during transition the system may need to query both the old and new owner
until hinted handoff or repair completes.

## 4. Approaches

### 4.1 Versioned Placement History with Query Fallback (Recommended for MVP)

**How it works:**

1. Raft commits placement map at epoch N.
2. Writes always use the _current_ (latest committed) epoch. New data goes to
   the instance specified by the current map.
3. Queries try the current epoch first. If the data is not found, the query
   falls back to the previous epoch (or epochs), trying the instance(s) that
   owned the shard range in earlier epochs.
4. No rebalancing is required. Data stays where it was written.

**Data structures (extending `ClusterControlState`):**

```rust
pub struct ClusterControlState {
    // Current placement map (latest epoch)
    placement_map: Option<IptoPlacementMap>,

    // All placement maps, keyed by epoch, kept bounded (e.g. last 3)
    placement_map_history: BTreeMap<PlacementEpoch, IptoPlacementMap>,
    // ...
}
```

**Query logic:**

```text
fn resolve_data_individual(shard_id: DataIndividualShardId) -> Option<DataIndividualMetadata> {
    // Try current epoch
    let target = state.placement_map().resolve(shard_id);
    if let Ok(data) = query_ipto_instance(target, shard_id) {
        return Some(data);
    }

    // Fall back through previous epochs, newest first
    for (epoch, map) in state.placement_map_history.iter().rev() {
        if let Some(target) = map.resolve(shard_id) {
            if let Ok(data) = query_ipto_instance(target, shard_id) {
                return Some(data);
            }
        }
    }

    None
}
```

**Distance limited.** The history window is bounded (last N epochs). After
N reconfigurations without rebalancing, data from epoch N+1 ago is unfindable
through fallback alone — a broadcast query to all registered Ipto instances
serves as the final fallback.

**Advantages:**
- No data movement. Zero rebalancing network traffic.
- Idempotent — same query always finds the same data.
- `IptoPlacementMap` is already epoch-versioned and replicated through Raft.
- Query latency impact is small (second Ipto call only on first miss).
- Degrades gracefully: if the history window is exhausted, fall back to
  broadcast query.

**Disadvantages:**
- Query may need 2-3 Ipto calls for data written under older epochs.
- Over many reconfigurations without rebalancing, each Ipto instance
  accumulates data from shards it no longer owns. Storage grows, and
  broadcast fallback becomes more frequent.
- Operational metadata queries are infrequent enough that this is a tolerable
  tradeoff for the MVP.

### 4.2 Consistent Hashing with Virtual Nodes

**How it works:**

Instead of range-based shard mapping, use a hash ring:

```text
ring = [hash(ipto-a-vnode1), hash(ipto-a-vnode2), hash(ipto-b-vnode1), ...]
```

Each Ipto instance owns multiple virtual nodes. Adding an instance inserts new
vnodes into the ring. Only the shard IDs that fall between the new vnodes and
their predecessors are affected.

**Advantages:**
- Adding/removing instances affects only a small, predictable fraction of the
  shard space.
- Standard technique, well-understood.

**Disadvantages:**
- Vannak currently uses range-based mapping (`IptoPlacementRange`). Moving to
  consistent hashing means changing the placement model and the Raft-replicated
  map structure.
- Still requires fallback queries for the affected fraction during transition.
- The Teradata insight applies here too: even with consistent hashing, during
  transition some data is in the "wrong" place.

### 4.3 Rebalancing with Idempotent Writes (Post-MVP)

**How it works:**

After Raft commits a new placement map:

1. New writes go to the current epoch's target (as with 4.1).
2. A background rebalancing process is triggered. For each shard range whose
   owner changed:
   - Read all data individuals in the range from the old Ipto instance.
   - Write idempotently to the new Ipto instance (using `correlation_id`
     / `IdempotencyKey` — Ipto already supports this).
3. Once rebalancing is confirmed for a range, Raft commits a
   `RebalancingComplete` marker, and the old instance can archive or delete
   the now-duplicated data.
4. During rebalancing, queries try the new instance first, then the old
   instance (as with 4.1).

**Advantages:**
- After rebalancing completes, each Ipto instance owns exactly its assigned
  shards. Queries are single-hop.
- The transitional state is temporary and bounded.

**Disadvantages:**
- Rebalancing involves reading all data in affected shard ranges and writing
  to the new instance. This is network I/O proportional to shard range size.
- During rebalancing, both old and new instances may have partial data.
  Queries must handle this.
- If the old instance is being removed because it failed, rebalancing from it
  is impossible. Recovery must replay from the durable outbox instead.
- Adds significant operational complexity.

### 4.4 Outbox Replay Instead of Instance-to-Instance Migration (Vannak-Specific)

Because Vannak already maintains a durable metadata outbox (segment-backed, in
`ipto.rs`), rebalancing can be simpler:

1. Raft commits a new placement map. New writes follow it.
2. For data that needs to move, do _not_ read from the old Ipto instance.
   Instead, replay the outbox segment(s) from the acknowledged checkpoint
   offset onward, redirecting each payload to the new Ipto instance.
3. Since writes are idempotent, replay is safe. The new instance ignores
   duplicates.

**Advantages:**
- No dependency on the old Ipto instance being available.
- Uses existing infrastructure (outbox segments, idempotency keys).
- The outbox is the source of truth for what was written.

**Disadvantages:**
- Outbox segments may be large. Replaying all of them for a shard range
  migration could be expensive.
- Requires outbox segments to be retained until rebalancing completes (or
  until a policy decides to skip rebalancing for old data).

## 5. Recommended Path for Vannak

The recommendation is **Approach 4.1 (versioned placement history with query
fallback) for the MVP**, with an explicit path to optional rebalancing later.

### Why this fits Vannak's design:

1. **Placement maps are already epoch-versioned.** `PlacementEpoch`, `IptoPlacementMap`,
   and `ClusterControlCommand::SetIptoPlacementMap` exist in `cluster.rs`.
   Adding a bounded history is a small extension.

2. **Queries are infrequent, not high-throughput transactional.** The cost of
   a second Ipto call on a cache miss is negligible compared to the operational
   value of knowing a data individual's provenance or a dataset's lineage.

3. **Writes are already idempotent.** If rebalancing is added later, outbox
   replay can populate new instances without coordination with old instances.

4. **No data movement in the MVP.** This keeps the system simpler, avoids the
   "intermediate state" problem, and defers rebalancing complexity until operational 
   patterns justify it.

5. **Alignment with the architecture principle: separate domain placement from
   runtime placement.** `DataIndividualShardId` is stable. The mapping from
   shard to Ipto instance is what changes. The query layer is the right place
   to absorb that change.

### Extending ClusterControlState

The current `ClusterControlState` stores only the latest placement map. To
support fallback queries:

```rust
const MAX_PLACEMENT_HISTORY: usize = 5;

pub struct ClusterControlState {
    placement_maps: BTreeMap<PlacementEpoch, IptoPlacementMap>,
    // ... existing fields
}

impl ClusterControlState {
    pub fn resolve_with_fallback(
        &self,
        shard_id: DataIndividualShardId,
    ) -> Vec<IptoInstanceId> {
        let mut targets = Vec::new();
        // Current epoch first
        for (_, map) in self.placement_maps.iter().rev() {
            if let Some(target) = map.resolve(shard_id) {
                if !targets.contains(target) {
                    targets.push(target.clone());
                }
            }
        }
        targets
    }

    pub fn apply(&mut self, command: ClusterControlCommand) -> Result<(), ClusterControlError> {
        match command {
            ClusterControlCommand::SetIptoPlacementMap(map) => {
                // ... existing validation ...
                self.placement_maps.insert(map.epoch, map);
                // Prune history beyond MAX_PLACEMENT_HISTORY
                while self.placement_maps.len() > MAX_PLACEMENT_HISTORY {
                    if let Some(oldest) = self.placement_maps.keys().next().copied() {
                        self.placement_maps.remove(&oldest);
                    }
                }
            }
            // ... other commands unchanged
        }
    }
}
```

### Query Routing Logic (Pseudocode)

```text
fn find_metadata_for_individual(
    shard_id: DataIndividualShardId,
    individual_id: DataIndividualId,
) -> Result<DataIndividualMetadata, NotFound> {

    // 1. Try targets from placement history, newest epoch first
    for target in cluster_state.resolve_with_fallback(shard_id) {
        if let Ok(data) = ipto_backend.fetch_individual(target, individual_id) {
            return Ok(data);
        }
    }

    // 2. Broadcast to all known Ipto instances (last resort)
    for instance in cluster_state.all_ipto_instances() {
        if let Ok(data) = ipto_backend.fetch_individual(instance, individual_id) {
            return Ok(data);
        }
    }

    Err(NotFound)
}
```

### Rebalancing (Future)

When rebalancing becomes desirable (operational need, not architectural
necessity), the path is:

1. Add a `RebalancingStatus` type to `ClusterControlCommand`:
   `RebalanceShardRange { range, from_target, to_target, epoch }`.
2. A per-node background task listens for new placement maps, identifies
   shard ranges where ownership changed, and replays the relevant outbox
   segments against the new target.
3. Once replay completes, commit `RebalancingComplete` to Raft, which
   authorizes the old target to archive the shard range.
4. During rebalancing, queries use the fallback logic from 4.1.

## 6. The "Intermediate State" Problem

Rebalancing creates a window where some data is in the right place and some is not. 
The fallback approach in 4.1 eliminates this window entirely — there is no 
intermediate state because data never moves. The "cost" is that a query may 
need to probe multiple Ipto instances.

For Vannak's use case, this is the right tradeoff:

| Concern | Fallback Approach | Rebalancing Approach |
|---|---|---|
| Query latency | 2-3 Ipto calls (ms) | 1 Ipto call + migration overhead |
| Write path | Single-hop, always current | Single-hop, always current |
| Data consistency | No migration, always consistent | Migration window with partial data |
| Operational complexity | Low (history + fallback) | High (migration, fencing, archival) |
| Storage | Data never cleaned from old instances | Data cleaned after migration |
| Recovery from node loss | Outbox replay to new instance | Outbox replay to new instance |
| Impact of adding 1 instance | O(1) Raft commit, no data movement | O(shard range size) data movement |

For an operational knowledge plane where queries answer questions like "what
failed?" and "what provenance does this data item have?", the fallback approach
is clearly preferable in the early phases. Rebalancing can be added later when
storage pressure or query latency patterns justify it.

## 7. Relationship to Existing Vannak Types

| Type | Location | Role in This Design |
|---|---|---|
| `DataIndividualShardId` | `data.rs` | Stable hash-derived key. Never changes. |
| `IptoPlacement` | `ipto.rs` | Simple hash-based resolver for single-instance use. Not cluster-aware. |
| `IptoPlacementMap` | `cluster.rs` | Epoch-versioned, range-based cluster placement. The Raft-replicated truth. |
| `IptoPlacementRange` | `cluster.rs` | Contiguous shard range → instance binding. |
| `PlacementEpoch` | `cluster.rs` | Monotonic version counter for placement maps. |
| `WriterLease` / `LeaseEpoch` | `cluster.rs` | Controls who actively writes to a target. Prevents split-brain. |
| `MetadataOutboxCheckpoint` | `cluster.rs` | Per-shard, per-target acknowledged offset. Enables outbox replay for recovery or migration. |
| `ClusterControlState` | `cluster.rs` | Reducer for Raft-committed cluster commands. Would gain placement map history. |

## 8. Rebalancing via Shard-Aware Segment Replay (Implemented)

The fallback approach handles queries during reconfiguration. But for
long-lived clusters with many reconfigurations, data accumulates on old
instances and broadcast fallback becomes more frequent. Targeted rebalancing
addresses this without full-segment replay.

### 8.1 Key Enabler: `shard_id` on `IptoWritePayload`

The outbox payload now carries `DataIndividualShardId`. This means every
segment record knows which shard it belongs to. The binary codec includes
`shard_id` as a `u64` after the target string.

```rust
pub struct IptoWritePayload {
    pub target: IptoInstanceId,
    pub shard_id: DataIndividualShardId,   // <-- added
    pub idempotency_key: IdempotencyKey,
    pub mapping_version: String,
    pub attributes: BTreeMap<IptoAttributeName, MetadataValue>,
}
```

### 8.2 Shard-Range Segment Replay

```rust
pub fn replay_metadata_outbox_segment_for_shard_range(
    path: impl AsRef<Path>,
    start: DataIndividualShardId,
    end: DataIndividualShardId,
) -> Result<MetadataOutbox, MetadataOutboxStorageError>
```

This replays an outbox segment, extracting only the entries whose `shard_id`
falls within `[start, end]`. The returned `MetadataOutbox` contains only the
matching pending entries — ready for delivery through a writer connected to
the new Ipto instance.

### 8.3 Rebalancing Flow

When a placement map change moves shard range `[S..E]` from instance A to
instance B:

1. **Raft commits the new placement map** — all nodes agree shards `[S..E]`
   now belong to B. New writes go to B.

2. **Extract affected entries.** For each sealed or open outbox segment:
   ```rust
   let pending = replay_metadata_outbox_segment_for_shard_range(
       segment_path, shard_start, shard_end,
   )?;
   ```

3. **Deliver to new target.** Drain the pending entries through an
   `IptoWriter` connected to instance B:
   ```rust
   let mut writer = IptoRepoWriter::new(repo_for_b, tenant_id);
   drain_pending_outbox(&mut pending, &mut writer, /* max_attempts */ 100);
   ```

4. **Idempotency handles safety.** Since each payload carries an
   `idempotency_key` and the writer checks `get_unit_by_corrid_json()`
   before writing, re-delivering the same payload is safe. The new instance
   ignores duplicates.

5. **Record rebalancing progress.** After draining, record a
   `MetadataOutboxCheckpoint` for the new target with the last acknowledged
   offset. This ensures restart after partial rebalancing picks up where it
   left off.

6. **Archive old data (optional).** Once rebalancing is confirmed and the
   new instance has all data, the old instance can archive or drop the
   migrated shard range. A `RetentionPolicy` in the Raft state can authorize
   this.

### 8.4 Advantages Over Full-Segment Replay

| Approach | Replay scope | Network cost |
|---|---|---|
| Full-segment replay | All entries | ~segment size |
| Shard-range replay | Only affected shards | ~(segment size / instance count) |

With the shard-aware codec, rebalancing reads the same segment but filters
in-memory — matching entries are enqueued, non-matching entries are skipped.
The segment is not rewritten.

### 8.5 When to Rebalance

Rebalancing is optional, not required. The query fallback approach (section
4.1) handles reconfiguration without data movement. Rebalancing becomes
worthwhile when:

- Storage pressure: an instance accumulates data from shards it no longer
  owns.
- Query latency: broadcast fallback hits too many instances for frequently-
  queried data.
- Instance decommissioning: an instance being removed permanently needs its
  data moved before shutdown.

### 8.6 What's Not Implemented (Yet)

- **Automatic rebalancing trigger.** Currently manual — an operator or a
  cluster management task initiates replay.
- **Rebalancing progress tracking in Raft.** No `RebalancingStatus` command
  yet. Partial rebalancing restarts from the outbox checkpoint offset.
- **Retention policy for archived shards.** The old instance keeps data
  indefinitely unless manually cleaned up.
- **Cross-segment rebalancing.** The replay function works on one segment at
  a time. A bulk rebalancing task would iterate over all relevant segments.

## 9. Open Questions

- **Placement history window:** _Resolved — 5 epochs, bounded in
  `ClusterControlState` with automatic pruning._
- **Broadcast fallback:** _Resolved — `resolve_with_fallback` returns an
  ordered Vec of candidates. Broadcast (`all_ipto_instances()`) is a
  separate explicit call for the last-resort case._
- When permanently removing an Ipto instance, rebalancing via shard-range
  replay can move data first. Retention policy for archived shards is
  still future work.
- Should rebalancing be automatic (triggered by placement map change) or
  operator-initiated?
- What granularity should rebalancing progress tracking have? Per-segment?
  Per-shard-range? The outbox checkpoint offset per (shard, target) is the
  existing pattern.
- Should `IptoWritePayload.shard_id` be validated on ingest against the
  derived `DataIndividualShardId` from `DataIndividualId`?
