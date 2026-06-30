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

# Replacing Range-Based Placement with Consistent Hashing

## Status

Draft proposal, 2026-06-30.

## 1. Why Now

Vannak has two placement models, both with reconfiguration problems:

**`IptoPlacement` (simple modulo, in `ipto.rs`):**

```rust
let idx = shard_id.0 as usize % self.buckets.len();
```

Adding or removing an Ipto instance changes the bucket count. Every shard ID
remaps to a different instance. This is fine for a static single-node setup but
catastrophic for any cluster that may grow. It exists today because Vannak's
dependency-free core needed something simple for tests; it was never intended
as the cluster placement model.

**`IptoPlacementMap` + `IptoPlacementRange` (range-based, in `cluster.rs`):**

```rust
pub struct IptoPlacementRange {
    pub start: DataIndividualShardId,
    pub end: DataIndividualShardId,
    pub target: IptoInstanceId,
}
```

Adding an instance requires an operator to manually split an existing range.
The split is arbitrary — which range do you split? By how much? If shard IDs
are sparse or clustered (because callers assign them non-uniformly), some
instances get hot ranges and others get cold ones. The model works but puts
the burden of load distribution on the operator.

**The moment to change is now.** There are no production deployments, no
backward compatibility, and no data on disk that assumes either model. Once
Vannak starts writing durable segments keyed by instance placement, changing
the placement model becomes a migration.

## 2. Consistent Hashing Fits Vannak's Use Case

### Why consistent hashing?

| Property | Simple Modulo | Range-Based | Consistent Hashing |
|---|---|---|---|
| Adding 1 instance | All shards remap | Operator splits a range | ~1/N shards remap |
| Removing 1 instance | All shards remap | Operator merges a range | ~1/N shards remap |
| Load distribution | Uniform if shard IDs are uniform | Operator-dependent | Uniform with enough virtual nodes |
| Raft-replicated state | Instance list only | Instance list + range boundaries | Instance list + vnode count per instance |
| Deterministic from config | Yes | Yes | Yes |

The key insight: Vannak's `DataIndividualShardId` is a `u64`. The u64 space is
a natural ring. Consistent hashing partitions the u64 ring across instances by
placing each instance at multiple points on the ring (virtual nodes). A shard
ID maps to the instance whose virtual node is the next point clockwise on the
ring.

When a new instance joins, its virtual nodes are inserted into the ring. Only
the shard IDs that fall between the new virtual nodes and their predecessors
are affected — approximately 1/N of the total shard space. When an instance
leaves, its virtual nodes are removed, and the affected shard IDs fall to the
next instance on the ring.

The ring is deterministic given the set of (instance, vnode_count) pairs. Raft
only needs to replicate this compact configuration — not the ring itself.

### The ring does not need to be replicated.

Only the configuration parameters are replicated:

```rust
struct IptoPlacementConfig {
    epoch: PlacementEpoch,
    instances: Vec<IptoPlacementSlot>,
}

struct IptoPlacementSlot {
    instance: IptoInstanceId,
    vnodes: u32,   // number of virtual nodes for this instance
}
```

Every node builds the identical ring from this configuration. The ring
construction is a pure function: hash each (instance_id, vnode_index) onto the
u64 ring, sort the points.

## 3. Proposed Design

### 3.1 Consistent Hash Ring

```rust
/// A point on the u64 ring belonging to one Ipto instance.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RingPoint {
    position: u64,
    instance: IptoInstanceId,
}

/// Deterministic consistent-hash ring.
#[derive(Debug, Clone)]
pub struct IptoPlacementRing {
    epoch: PlacementEpoch,
    points: Vec<RingPoint>,  // sorted by position, wraps around
}
```

Construction:

```rust
impl IptoPlacementRing {
    pub fn new(epoch: PlacementEpoch, slots: &[IptoPlacementSlot]) -> Self {
        let mut points = Vec::new();
        for slot in slots {
            for vnode_idx in 0..slot.vnodes {
                let position = hash_vnode(&slot.instance, vnode_idx);
                points.push(RingPoint {
                    position,
                    instance: slot.instance.clone(),
                });
            }
        }
        points.sort_by_key(|p| p.position);
        Self { epoch, points }
    }

    pub fn resolve(&self, shard_id: DataIndividualShardId) -> Option<&IptoInstanceId> {
        if self.points.is_empty() {
            return None;
        }
        let idx = self
            .points
            .binary_search_by_key(&shard_id.0, |p| p.position)
            .unwrap_or_else(|i| i);
        // Wrap around: if idx == len, use points[0]
        Some(&self.points[idx % self.points.len()].instance)
    }
}
```

`hash_vnode` is a deterministic hash of the instance identity and vnode index.
It must be stable across restarts and across nodes so all nodes compute the
same ring. A 64-bit output from a non-cryptographic hash (e.g., FNV-1a or
xxHash with same seed everywhere) suffices.

```rust
fn hash_vnode(instance: &IptoInstanceId, vnode_idx: u32) -> u64 {
    let mut state = 0xcbf2_9ce4_8422_2325u64;
    // Mix instance identity
    for byte in instance.as_str().as_bytes() {
        state ^= u64::from(*byte);
        state = state.wrapping_mul(0x0000_0100_0000_01b3);
    }
    // Mix vnode index
    state ^= u64::from(vnode_idx);
    state = state.wrapping_mul(0x0000_0100_0000_01b3);
    state
}
```

This uses the same FNV-1a-style hash already used for segment checksums in
`storage.rs`. No new dependency. Deterministic and stable.

### 3.2 Virtual Node Count

The number of virtual nodes per instance controls how evenly the ring is
distributed and how much shard space moves on reconfiguration.

With 1 vnode per instance, adding one instance to a 3-instance cluster remaps
~25% of shards. With 128 vnodes per instance, it remaps ~25% with much lower
variance — the affected fraction converges to 1/(N+1) more reliably.

A reasonable default: **64 or 128 virtual nodes per instance**. This gives
good uniformity without making the ring (and Raft state) large. A 10-instance
cluster with 128 vnodes each is 1280 ring points — trivial to binary-search.

The vnode count could also be used as a weighted placement: an instance with
256 vnodes receives roughly twice the shard load of one with 128 vnodes.

### 3.3 Range Overrides (Operator-Defined Placement)

Consistent hashing handles the common case (uniform distribution, minimal
disruption). But operators may need explicit placement: keep all shards for a
specific tenant or pipeline on a dedicated Ipto instance.

The placement map supports both:

```rust
pub struct IptoPlacementMap {
    pub epoch: PlacementEpoch,
    ring: IptoPlacementRing,
    overrides: Vec<IptoPlacementRange>,
}

impl IptoPlacementMap {
    pub fn resolve(&self, shard_id: DataIndividualShardId) -> Option<&IptoInstanceId> {
        // Overrides take priority
        for range in &self.overrides {
            if range.contains(shard_id) {
                return Some(&range.target);
            }
        }
        // Default: consistent hash ring
        self.ring.resolve(shard_id)
    }
}
```

Overrides are optional. A deployment that never needs explicit placement omits
them entirely. When present, they are checked first, providing an escape hatch
for operational requirements without complicating the default path.

Overrides are validated: no overlapping overrides, and no override that
contradicts another override. Overrides do not need to cover the full u64
space — uncovered shard IDs fall through to the ring.

### 3.4 Raft-Replicated Configuration

The configuration replicated through Raft:

```rust
pub struct IptoPlacementMap {
    pub epoch: PlacementEpoch,
    pub slots: Vec<IptoPlacementSlot>,
    pub overrides: Vec<IptoPlacementRange>,
}

pub struct IptoPlacementSlot {
    pub instance: IptoInstanceId,
    pub vnodes: u32,
}
```

When `ClusterControlCommand::SetIptoPlacementMap` is applied:

1. Validate: at least one slot, no duplicate instances, no overlapping
   overrides.
2. Store the configuration.
3. Each node builds the ring locally from the slots.

The ring is never stored or replicated. It is reconstructed on node startup
from the committed configuration, and rebuilt whenever the configuration
changes.

### 3.5 Epoch-Based Query Fallback

The placement map history and query fallback mechanism from
`thoughts/shard-placement-and-reconfiguration.md` remain unchanged. Each epoch's
placement map (with its ring and overrides) is kept in history for query
fallback.

## 4. Shard ID Derivation

The user correctly observes that `correlation_id` (UUID v7) is already
hash-like. The question is how `DataIndividualShardId` is assigned.

### Current state

`DataIndividualShardId` is caller-provided. The `DataIndividualMetadataEvent`
constructor takes it as a parameter. Nothing in Vannak derives it.

### Recommendation: Add a derivation function, keep caller override

```rust
impl DataIndividualShardId {
    /// Derive a shard ID from a data individual's stable identity.
    ///
    /// Uses a deterministic 64-bit hash of the identity string.
    /// This gives uniform distribution across the u64 space, which
    /// works well with consistent hashing placement.
    pub fn from_data_individual(data_individual_id: &DataIndividualId) -> Self {
        let mut state = 0xcbf2_9ce4_8422_2325u64;
        for byte in data_individual_id.as_str().as_bytes() {
            state ^= u64::from(*byte);
            state = state.wrapping_mul(0x0000_0100_0000_01b3);
        }
        Self(state)
    }
}
```

Callers can still assign an explicit `DataIndividualShardId` when the domain
placement key has business meaning (e.g., "all data for tenant X goes to
shard range Y"). But the common case — "hash the data individual identity" —
is one function call.

Using the same FNV-1a hash for shard ID derivation and vnode placement means
both are dependency-free, deterministic, and produce well-distributed u64
values. An adversary could craft collisions, but Vannak is not an adversarial
system and the hash is for distribution, not security.

## 5. What This Changes in the Codebase

### Remove

- `IptoPlacement` (`ipto.rs:69-96`) — the simple modulo resolver. Only used
  in tests and in `IptoWritePayload::from_event()`. Tests adapt trivially;
  the `from_event` method switches to the ring-based placement map.

### Modify

- `IptoPlacementMap` (`cluster.rs:92-128`) — replace `ranges: Vec<IptoPlacementRange>`
  with the ring + overrides design. Validation changes from "no overlapping
  ranges" to "at least one slot, no duplicate instances, no overlapping
  overrides." `resolve()` queries overrides first, then the ring.
- `ClusterControlState` (`cluster.rs`) — `resolve_ipto_target` delegates to
  `IptoPlacementMap::resolve()`. Placement history stores maps per epoch for
  query fallback.
- `ClusterControlCommand::SetIptoPlacementMap` — the command now carries
  `Vec<IptoPlacementSlot>` + optional `Vec<IptoPlacementRange>` overrides
  instead of just ranges.

### Keep

- `IptoPlacementRange` — reused as the override type. No structural change.
- `PlacementEpoch`, `LeaseEpoch`, `CheckpointEpoch` — unchanged.
- `WriterLease`, `MetadataOutboxCheckpoint` — unchanged. These are per-target,
  not per-placement-mechanism.
- All outbox, segment, and ingest types — unchanged. Placement is a resolution
  concern, not a storage concern.

### Add

- `IptoPlacementSlot` — (instance, vnode_count) pair.
- `IptoPlacementRing` — deterministic ring built from slots.
- `hash_vnode()` — hash function for ring construction.
- `DataIndividualShardId::from_data_individual()` — convenience derivation.

## 6. Worked Example

### Initial setup: 3 Ipto instances

```rust
let map = IptoPlacementMap::new(
    PlacementEpoch(1),
    vec![
        IptoPlacementSlot { instance: "ipto-a".into(), vnodes: 128 },
        IptoPlacementSlot { instance: "ipto-b".into(), vnodes: 128 },
        IptoPlacementSlot { instance: "ipto-c".into(), vnodes: 128 },
    ],
    vec![],  // no overrides
);
```

384 ring points, uniformly distributed. Each instance owns ~33% of the u64
space.

### Add a 4th instance

```rust
let map = IptoPlacementMap::new(
    PlacementEpoch(2),
    vec![
        IptoPlacementSlot { instance: "ipto-a".into(), vnodes: 128 },
        IptoPlacementSlot { instance: "ipto-b".into(), vnodes: 128 },
        IptoPlacementSlot { instance: "ipto-c".into(), vnodes: 128 },
        IptoPlacementSlot { instance: "ipto-d".into(), vnodes: 128 },
    ],
    vec![],
);
```

512 ring points. Each existing instance loses ~8.3% of its shard space to the
new instance (1/3 → 1/4 of the ring). ~25% of all shard IDs now resolve to a
different instance than in epoch 1. Queries fall back through epochs 1 and 2
if the current epoch's instance does not have the data.

### Remove an instance

```rust
let map = IptoPlacementMap::new(
    PlacementEpoch(3),
    vec![
        IptoPlacementSlot { instance: "ipto-a".into(), vnodes: 128 },
        IptoPlacementSlot { instance: "ipto-b".into(), vnodes: 128 },
        IptoPlacementSlot { instance: "ipto-d".into(), vnodes: 128 },
    ],
    vec![],
);
```

The shards previously owned by "ipto-c" are redistributed to the remaining
three instances. Vnodes that belonged to "ipto-c" are simply gone from the
ring; the nearest clockwise vnode from each of "ipto-c"'s positions now
belongs to one of the survivors. Queries fall back through epochs if data
has not been replayed from the outbox.

### Dedicated instance with override

```rust
let map = IptoPlacementMap::new(
    PlacementEpoch(4),
    vec![
        IptoPlacementSlot { instance: "ipto-a".into(), vnodes: 128 },
        IptoPlacementSlot { instance: "ipto-b".into(), vnodes: 128 },
        IptoPlacementSlot { instance: "ipto-d".into(), vnodes: 128 },
        IptoPlacementSlot { instance: "ipto-compliance".into(), vnodes: 256 },
    ],
    vec![
        IptoPlacementRange::new(
            DataIndividualShardId(1_000_000),
            DataIndividualShardId(1_999_999),
            IptoInstanceId::from("ipto-compliance"),
        ).unwrap(),
    ],
);
```

Shards `[1_000_000 ..= 1_999_999]` resolve to `ipto-compliance` regardless of
ring position (override). All other shards fall through to the ring, where
`ipto-compliance` also participates with double weight (256 vnodes vs 128).

## 7. No New Dependencies

All components of this design use the same FNV-1a hash already present in
`storage.rs`. The ring is a sorted `Vec` with binary search. No external crate
is needed:

```rust
// Already in storage.rs:
fn checksum(payload: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in payload {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}
```

The same hash function, applied to `(instance_as_string, vnode_index)`, gives
us the ring positions and shard ID derivations. One hash, three uses.

## 8. Tradeoffs

| Concern | Consistent Hashing | Range-Based (current) |
|---|---|---|
| Reconfiguration disruption | ~1/N shards affected | Operator decides; can be 0 or 1/N or all |
| Load distribution | Uniform with ≥64 vnodes | Depends on shard ID distribution and range sizing |
| Operator mental model | "add instance, ring adjusts" | "split range 0-999 at 500" |
| Explicit placement | Via overrides | Via ranges (the primary mechanism) |
| Raft state size | Instance list + vnodes (compact) | Range list (compact) |
| Ring determinism | Same config → same ring | Same ranges → same map |
| Debugging | Harder: "why is shard X on instance Y?" without computing the ring | Easier: "shard X is in range [a..b], which maps to Y" |

The debugging concern is real. Mitigation: `IptoPlacementRing` should implement
`Display` or have a `describe_shard(shard_id)` method that shows which vnode
and instance a shard maps to and why. Range-based debugging is more transparent
by default; consistent hashing requires tooling to be equally transparent.

## 9. Open Questions

- **Virtual node count per instance.** 64, 128, 256? More vnodes mean better
  uniformity but more Raft state (linear growth). For 10 instances at 128
  vnodes: 1280 ring points, ~10 KiB of configuration. Negligible.
- **Should overrides be validated against the ring?** If an override places a
  shard range on an instance that is not in the ring, should that be an error?
  The instance must exist in the cluster, but it may not need vnodes if it
  only serves overrides.
- **Should `DataIndividualShardId` be replaced by `DataIndividualId` itself
  as the ring key?** If we always hash `DataIndividualId` to get the shard
  ID, and never use caller-assigned shard IDs, the shard ID type becomes
  redundant. But caller-assigned shard IDs support domain-driven placement
  (tenant-scoped shard ranges). Keeping both gives flexibility.
- **What happens when an instance is removed and its vnodes disappear, but
  the data hasn't been replayed?** Query fallback handles this (the previous
  document's design). The ring just routes new writes; old data is found
  through history fallback and eventually replayed or archived.
- **Should `IptoPlacementSlot` support a `weight` field instead of `vnodes`?**
  `vnodes` _is_ the weight. An instance with 256 vnodes gets ~twice the ring
  coverage of one with 128. Explicit `weight` with automatic vnode scaling
  would be a cosmetic wrapper. Start with vnodes directly.
