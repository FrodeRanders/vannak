/*
 * Copyright (C) 2026 Frode Randers
 * All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Raft-facing cluster control state boundary.
//!
//! This module contains compact records that are suitable for replication
//! through Raft: placement maps, writer leases, sealed segment manifests, and
//! metadata outbox checkpoints. Raw event traffic should not be routed through
//! this boundary.
//!
//! ## Placement model
//!
//! Metadata placement maps `DataIndividualShardId` values to `IptoInstanceId`
//! targets using a deterministic consistent-hash ring. Each Ipto instance
//! contributes a configurable number of virtual nodes placed on the u64 ring.
//! Adding or removing an instance affects only ~1/N of the shard space.
//!
//! Explicit range overrides are supported for operator-defined placement.
//! Overrides take priority over the ring.
//!
//! Placement maps are epoch-versioned and replicated through Raft. The ring
//! itself is not replicated — each node builds it deterministically from the
//! slot configuration.

use crate::data::DataIndividualShardId;
use crate::ipto::IptoInstanceId;
use crate::storage::{RecordOffset, SegmentId, SegmentManifest};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

// ---------------------------------------------------------------------------
// Identity and epoch types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(String);

impl NodeId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for NodeId {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for NodeId {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PlacementEpoch(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LeaseEpoch(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CheckpointEpoch(pub u64);

// ---------------------------------------------------------------------------
// Consistent hash ring
// ---------------------------------------------------------------------------

/// A weighted Ipto instance entry in the placement configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IptoPlacementSlot {
    pub instance: IptoInstanceId,
    pub vnodes: u32,
}

impl IptoPlacementSlot {
    pub fn new(instance: IptoInstanceId, vnodes: u32) -> Result<Self, ClusterControlError> {
        if vnodes == 0 {
            return Err(ClusterControlError::ZeroVnodes {
                instance: instance.as_str().to_string(),
            });
        }
        Ok(Self { instance, vnodes })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RingPoint {
    position: u64,
    instance: IptoInstanceId,
}

/// Deterministic consistent-hash ring over the u64 space.
///
/// Each Ipto instance contributes `vnodes` points placed by hashing
/// `(instance_id, vnode_index)` with a stable FNV-1a derivative. The ring is
/// sorted and binary-searched to resolve a `DataIndividualShardId` to an
/// instance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IptoPlacementRing {
    epoch: PlacementEpoch,
    points: Vec<RingPoint>,
}

impl IptoPlacementRing {
    pub fn new(epoch: PlacementEpoch, slots: &[IptoPlacementSlot]) -> Self {
        let mut points = Vec::new();
        for slot in slots {
            for vnode_idx in 0..slot.vnodes {
                let position = ring_hash(slot.instance.as_str(), vnode_idx);
                points.push(RingPoint {
                    position,
                    instance: slot.instance.clone(),
                });
            }
        }
        points.sort_by_key(|p| p.position);
        Self { epoch, points }
    }

    pub fn epoch(&self) -> PlacementEpoch {
        self.epoch
    }

    pub fn resolve(&self, shard_id: DataIndividualShardId) -> Option<&IptoInstanceId> {
        if self.points.is_empty() {
            return None;
        }
        let idx = self
            .points
            .binary_search_by_key(&shard_id.0, |p| p.position)
            .unwrap_or_else(|i| i);
        Some(&self.points[idx % self.points.len()].instance)
    }

    pub fn instances(&self) -> Vec<IptoInstanceId> {
        let mut seen = BTreeSet::new();
        let mut result = Vec::new();
        for point in &self.points {
            if seen.insert(point.instance.clone()) {
                result.push(point.instance.clone());
            }
        }
        result
    }

    pub fn is_empty(&self) -> bool {
        self.points.is_empty()
    }
}

/// Stable FNV-1a derivative for placing virtual nodes on the u64 ring.
///
/// Same family as the segment checksum in `storage.rs`. The instance name and
/// vnode index are both fed through the full mixing loop for good distribution.
/// Deterministic and dependency-free.
fn ring_hash(instance: &str, vnode_idx: u32) -> u64 {
    let mut state = 0xcbf2_9ce4_8422_2325u64;
    for byte in instance.as_bytes() {
        state ^= u64::from(*byte);
        state = state.wrapping_mul(0x0000_0100_0000_01b3);
    }
    for byte in &vnode_idx.to_le_bytes() {
        state ^= u64::from(*byte);
        state = state.wrapping_mul(0x0000_0100_0000_01b3);
    }
    state
}

// ---------------------------------------------------------------------------
// Placement range (override)
// ---------------------------------------------------------------------------

/// Inclusive data-individual shard range assigned to one Ipto instance.
///
/// Used as an override in an `IptoPlacementMap`. Ranges take priority over
/// the consistent-hash ring.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IptoPlacementRange {
    pub start: DataIndividualShardId,
    pub end: DataIndividualShardId,
    pub target: IptoInstanceId,
}

impl IptoPlacementRange {
    pub fn new(
        start: DataIndividualShardId,
        end: DataIndividualShardId,
        target: IptoInstanceId,
    ) -> Result<Self, ClusterControlError> {
        if end < start {
            return Err(ClusterControlError::InvalidPlacementRange { start, end });
        }
        Ok(Self { start, end, target })
    }

    pub fn contains(&self, shard_id: DataIndividualShardId) -> bool {
        self.start <= shard_id && shard_id <= self.end
    }
}

// ---------------------------------------------------------------------------
// Placement map (ring + optional overrides)
// ---------------------------------------------------------------------------

/// Versioned placement map replicated through Raft.
///
/// The primary placement mechanism is a consistent-hash ring built from
/// `IptoPlacementSlot` entries. Optional `IptoPlacementRange` overrides take
/// priority when present.
///
/// The ring is not persisted — it is built from the slots whenever the map is
/// constructed. Raft replicates the slots and overrides, not the ring points.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IptoPlacementMap {
    pub epoch: PlacementEpoch,
    slots: Vec<IptoPlacementSlot>,
    ring: IptoPlacementRing,
    overrides: Vec<IptoPlacementRange>,
}

impl IptoPlacementMap {
    pub fn new(
        epoch: PlacementEpoch,
        slots: Vec<IptoPlacementSlot>,
        overrides: Vec<IptoPlacementRange>,
    ) -> Result<Self, ClusterControlError> {
        if slots.is_empty() {
            return Err(ClusterControlError::NoPlacementSlots);
        }

        let mut sorted_overrides = overrides;
        sorted_overrides.sort_by_key(|r| r.start);
        for pair in sorted_overrides.windows(2) {
            let left = &pair[0];
            let right = &pair[1];
            if right.start <= left.end {
                return Err(ClusterControlError::OverlappingPlacementRanges {
                    left_start: left.start,
                    left_end: left.end,
                    right_start: right.start,
                    right_end: right.end,
                });
            }
        }

        let ring = IptoPlacementRing::new(epoch, &slots);
        Ok(Self {
            epoch,
            slots,
            ring,
            overrides: sorted_overrides,
        })
    }

    pub fn resolve(&self, shard_id: DataIndividualShardId) -> Option<&IptoInstanceId> {
        for range in &self.overrides {
            if range.contains(shard_id) {
                return Some(&range.target);
            }
        }
        self.ring.resolve(shard_id)
    }

    pub fn slots(&self) -> &[IptoPlacementSlot] {
        &self.slots
    }

    pub fn overrides(&self) -> &[IptoPlacementRange] {
        &self.overrides
    }

    pub fn ring(&self) -> &IptoPlacementRing {
        &self.ring
    }

    pub fn instances(&self) -> Vec<IptoInstanceId> {
        self.ring.instances()
    }
}

// ---------------------------------------------------------------------------
// Leases and checkpoints
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriterLease {
    pub target: IptoInstanceId,
    pub holder: NodeId,
    pub epoch: LeaseEpoch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataOutboxCheckpoint {
    pub data_individual_shard_id: DataIndividualShardId,
    pub target: IptoInstanceId,
    pub segment_id: SegmentId,
    pub last_acknowledged_offset: RecordOffset,
    pub mapping_version: String,
    pub epoch: CheckpointEpoch,
}

/// Shard-level recovery checkpoint replicated through Raft.
///
/// Records segment consumption progress so a recovering node can skip
/// already-processed segment data. The `segment_offsets` map records
/// (segment_id → last_consumed_offset) for every segment that has been
/// fully or partially consumed by the time this checkpoint was taken.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointManifest {
    pub checkpoint_id: String,
    pub node_id: NodeId,
    pub epoch: CheckpointEpoch,
    pub segment_offsets: BTreeMap<SegmentId, RecordOffset>,
    pub metadata_version: Option<String>,
    pub checksum: u64,
}

impl CheckpointManifest {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        checkpoint_id: impl Into<String>,
        node_id: NodeId,
        epoch: CheckpointEpoch,
        segment_offsets: BTreeMap<SegmentId, RecordOffset>,
        metadata_version: Option<String>,
        checksum: u64,
    ) -> Self {
        Self {
            checkpoint_id: checkpoint_id.into(),
            node_id,
            epoch,
            segment_offsets,
            metadata_version,
            checksum,
        }
    }

    pub fn latest_offset_for(&self, segment_id: &SegmentId) -> Option<RecordOffset> {
        self.segment_offsets.get(segment_id).copied()
    }
}

// ---------------------------------------------------------------------------
// Cluster control state
// ---------------------------------------------------------------------------

const MAX_PLACEMENT_HISTORY: usize = 5;

/// Dependency-free reducer for the compact state a Raft state machine would
/// apply after log entries commit.
///
/// Stores the current placement map plus a bounded history of previous maps
/// for query fallback during reconfiguration. The history window is pruned to
/// `MAX_PLACEMENT_HISTORY` entries.
#[derive(Debug, Default)]
pub struct ClusterControlState {
    nodes: BTreeSet<NodeId>,
    placement_maps: BTreeMap<PlacementEpoch, IptoPlacementMap>,
    writer_leases: BTreeMap<IptoInstanceId, WriterLease>,
    outbox_checkpoints: BTreeMap<(DataIndividualShardId, IptoInstanceId), MetadataOutboxCheckpoint>,
    sealed_segments: BTreeMap<SegmentId, SegmentManifest>,
    checkpoints: BTreeMap<String, CheckpointManifest>,
}

impl ClusterControlState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn apply(&mut self, command: ClusterControlCommand) -> Result<(), ClusterControlError> {
        match command {
            ClusterControlCommand::AddNode(node_id) => {
                self.nodes.insert(node_id);
            }
            ClusterControlCommand::RemoveNode(node_id) => {
                self.nodes.remove(&node_id);
                self.writer_leases
                    .retain(|_, lease| lease.holder != node_id);
            }
            ClusterControlCommand::SetIptoPlacementMap(map) => {
                self.insert_placement_map(map)?;
            }
            ClusterControlCommand::GrantWriterLease(lease) => {
                if !self.nodes.contains(&lease.holder) {
                    return Err(ClusterControlError::UnknownLeaseHolder {
                        holder: lease.holder,
                    });
                }
                if let Some(current) = self.writer_leases.get(&lease.target)
                    && lease.epoch <= current.epoch
                {
                    return Err(ClusterControlError::StaleLeaseEpoch {
                        target: lease.target,
                        current: current.epoch,
                        proposed: lease.epoch,
                    });
                }
                self.writer_leases.insert(lease.target.clone(), lease);
            }
            ClusterControlCommand::RecordOutboxCheckpoint(checkpoint) => {
                let key = (
                    checkpoint.data_individual_shard_id,
                    checkpoint.target.clone(),
                );
                if let Some(current) = self.outbox_checkpoints.get(&key)
                    && checkpoint.epoch <= current.epoch
                {
                    return Err(ClusterControlError::StaleCheckpointEpoch {
                        shard_id: checkpoint.data_individual_shard_id,
                        target: checkpoint.target,
                        current: current.epoch,
                        proposed: checkpoint.epoch,
                    });
                }
                self.outbox_checkpoints.insert(key, checkpoint);
            }
            ClusterControlCommand::RecordSealedSegment(manifest) => {
                self.sealed_segments
                    .insert(manifest.segment_id.clone(), manifest);
            }
            ClusterControlCommand::RecordCheckpoint(checkpoint) => {
                if let Some(current) = self.checkpoints.get(&checkpoint.checkpoint_id)
                    && checkpoint.epoch <= current.epoch
                {
                    return Err(ClusterControlError::StaleCheckpointManifest {
                        checkpoint_id: checkpoint.checkpoint_id,
                        current: current.epoch,
                        proposed: checkpoint.epoch,
                    });
                }
                self.checkpoints
                    .insert(checkpoint.checkpoint_id.clone(), checkpoint);
            }
        }

        Ok(())
    }

    fn insert_placement_map(
        &mut self,
        map: IptoPlacementMap,
    ) -> Result<(), ClusterControlError> {
        if let Some((_, current)) = self.placement_maps.last_key_value()
            && map.epoch <= current.epoch
        {
            return Err(ClusterControlError::StalePlacementEpoch {
                current: current.epoch,
                proposed: map.epoch,
            });
        }
        let epoch = map.epoch;
        self.placement_maps.insert(epoch, map);
        while self.placement_maps.len() > MAX_PLACEMENT_HISTORY {
            let Some(oldest) = self.placement_maps.keys().next().copied() else {
                break;
            };
            self.placement_maps.remove(&oldest);
        }
        Ok(())
    }

    // -- accessors --

    pub fn nodes(&self) -> &BTreeSet<NodeId> {
        &self.nodes
    }

    /// Return the current (latest-epoch) placement map.
    pub fn placement_map(&self) -> Option<&IptoPlacementMap> {
        self.placement_maps.values().next_back()
    }

    /// Resolve a shard ID through the current placement map only.
    pub fn resolve_ipto_target(&self, shard_id: DataIndividualShardId) -> Option<&IptoInstanceId> {
        self.placement_map()?.resolve(shard_id)
    }

    /// Resolve a shard ID with fallback through placement map history.
    ///
    /// Returns candidate Ipto instances in priority order: current epoch first,
    /// then previous epochs (newest first), deduplicated. If the shard ID is
    /// not covered by any known map, returns an empty vector.
    pub fn resolve_with_fallback(
        &self,
        shard_id: DataIndividualShardId,
    ) -> Vec<IptoInstanceId> {
        let mut candidates = Vec::new();

        for map in self.placement_maps.values().rev() {
            if let Some(target) = map.resolve(shard_id)
                && !candidates.iter().any(|c: &IptoInstanceId| c == target)
            {
                candidates.push(target.clone());
            }
        }

        candidates
    }

    /// Return all known Ipto instances across all placement maps (for broadcast
    /// queries as a last-resort fallback).
    pub fn all_ipto_instances(&self) -> Vec<IptoInstanceId> {
        let mut seen = BTreeSet::new();
        let mut result = Vec::new();
        for map in self.placement_maps.values() {
            for instance in map.instances() {
                if seen.insert(instance.clone()) {
                    result.push(instance);
                }
            }
        }
        result
    }

    pub fn placement_map_history(
        &self,
    ) -> impl Iterator<Item = (&PlacementEpoch, &IptoPlacementMap)> {
        self.placement_maps.iter()
    }

    pub fn writer_leases(&self) -> &BTreeMap<IptoInstanceId, WriterLease> {
        &self.writer_leases
    }

    pub fn writer_lease(&self, target: &IptoInstanceId) -> Option<&WriterLease> {
        self.writer_leases.get(target)
    }

    pub fn outbox_checkpoints(
        &self,
    ) -> &BTreeMap<(DataIndividualShardId, IptoInstanceId), MetadataOutboxCheckpoint> {
        &self.outbox_checkpoints
    }

    pub fn outbox_checkpoint(
        &self,
        shard_id: DataIndividualShardId,
        target: &IptoInstanceId,
    ) -> Option<&MetadataOutboxCheckpoint> {
        self.outbox_checkpoints.get(&(shard_id, target.clone()))
    }

    pub fn sealed_segments(&self) -> &BTreeMap<SegmentId, SegmentManifest> {
        &self.sealed_segments
    }

    pub fn sealed_segment(&self, segment_id: &SegmentId) -> Option<&SegmentManifest> {
        self.sealed_segments.get(segment_id)
    }

    pub fn checkpoint(&self, checkpoint_id: &str) -> Option<&CheckpointManifest> {
        self.checkpoints.get(checkpoint_id)
    }

    pub fn checkpoints(&self) -> &BTreeMap<String, CheckpointManifest> {
        &self.checkpoints
    }
}

// ---------------------------------------------------------------------------
// Commands and errors
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterControlCommand {
    AddNode(NodeId),
    RemoveNode(NodeId),
    SetIptoPlacementMap(IptoPlacementMap),
    GrantWriterLease(WriterLease),
    RecordOutboxCheckpoint(MetadataOutboxCheckpoint),
    RecordSealedSegment(SegmentManifest),
    RecordCheckpoint(CheckpointManifest),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterControlError {
    NoPlacementSlots,
    ZeroVnodes {
        instance: String,
    },
    NoShardTarget {
        shard_id: DataIndividualShardId,
    },
    InvalidPlacementRange {
        start: DataIndividualShardId,
        end: DataIndividualShardId,
    },
    OverlappingPlacementRanges {
        left_start: DataIndividualShardId,
        left_end: DataIndividualShardId,
        right_start: DataIndividualShardId,
        right_end: DataIndividualShardId,
    },
    StalePlacementEpoch {
        current: PlacementEpoch,
        proposed: PlacementEpoch,
    },
    UnknownLeaseHolder {
        holder: NodeId,
    },
    StaleLeaseEpoch {
        target: IptoInstanceId,
        current: LeaseEpoch,
        proposed: LeaseEpoch,
    },
    StaleCheckpointEpoch {
        shard_id: DataIndividualShardId,
        target: IptoInstanceId,
        current: CheckpointEpoch,
        proposed: CheckpointEpoch,
    },
    StaleCheckpointManifest {
        checkpoint_id: String,
        current: CheckpointEpoch,
        proposed: CheckpointEpoch,
    },
}

impl fmt::Display for ClusterControlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoPlacementSlots => {
                f.write_str("Ipto placement map requires at least one slot")
            }
            Self::ZeroVnodes { instance } => write!(
                f,
                "Ipto placement slot for '{}' must have at least one virtual node",
                instance
            ),
            Self::NoShardTarget { shard_id } => write!(
                f,
                "no Ipto instance resolved for shard {} in the current placement map",
                shard_id.0
            ),
            Self::InvalidPlacementRange { start, end } => write!(
                f,
                "invalid Ipto placement range: start {} is greater than end {}",
                start.0, end.0
            ),
            Self::OverlappingPlacementRanges {
                left_start,
                left_end,
                right_start,
                right_end,
            } => write!(
                f,
                "overlapping Ipto placement ranges: {}..={} overlaps {}..={}",
                left_start.0, left_end.0, right_start.0, right_end.0
            ),
            Self::StalePlacementEpoch { current, proposed } => write!(
                f,
                "stale Ipto placement epoch: current {}, proposed {}",
                current.0, proposed.0
            ),
            Self::UnknownLeaseHolder { holder } => {
                write!(
                    f,
                    "writer lease holder '{}' is not a cluster node",
                    holder.as_str()
                )
            }
            Self::StaleLeaseEpoch {
                target,
                current,
                proposed,
            } => write!(
                f,
                "stale writer lease epoch for target '{}': current {}, proposed {}",
                target.as_str(),
                current.0,
                proposed.0
            ),
            Self::StaleCheckpointEpoch {
                shard_id,
                target,
                current,
                proposed,
            } => write!(
                f,
                "stale outbox checkpoint epoch for shard {} target '{}': current {}, proposed {}",
                shard_id.0,
                target.as_str(),
                current.0,
                proposed.0
            ),
            Self::StaleCheckpointManifest {
                checkpoint_id,
                current,
                proposed,
            } => write!(
                f,
                "stale checkpoint manifest epoch for '{}': current {}, proposed {}",
                checkpoint_id, current.0, proposed.0
            ),
        }
    }
}

impl std::error::Error for ClusterControlError {}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn slot(instance: &str, vnodes: u32) -> IptoPlacementSlot {
        IptoPlacementSlot::new(IptoInstanceId::from(instance), vnodes).unwrap()
    }

    fn test_shard(seed: u64) -> DataIndividualShardId {
        let mut state = 0xcbf2_9ce4_8422_2325u64;
        for byte in &seed.to_le_bytes() {
            state ^= u64::from(*byte);
            state = state.wrapping_mul(0x0000_0100_0000_01b3);
        }
        DataIndividualShardId(state)
    }

    // -- ring tests --

    #[test]
    fn ring_assigns_every_shard_to_an_instance() {
        let ring = IptoPlacementRing::new(
            PlacementEpoch(1),
            &[slot("ipto-a", 64), slot("ipto-b", 64)],
        );
        assert!(!ring.is_empty());

        let mut hits: BTreeMap<String, u64> = BTreeMap::new();
        for i in 0..10_000u64 {
            let target = ring.resolve(test_shard(i)).unwrap();
            *hits.entry(target.as_str().to_string()).or_default() += 1;
        }

        let a_share = *hits.get("ipto-a").unwrap_or(&0);
        let b_share = *hits.get("ipto-b").unwrap_or(&0);

        assert!((4000..=6000).contains(&a_share), "a got {a_share}");
        assert!((4000..=6000).contains(&b_share), "b got {b_share}");
    }

    #[test]
    fn ring_is_deterministic() {
        let a = IptoPlacementRing::new(
            PlacementEpoch(1),
            &[slot("ipto-a", 32)],
        );
        let b = IptoPlacementRing::new(
            PlacementEpoch(1),
            &[slot("ipto-a", 32)],
        );

        for i in 0..1000u64 {
            let shard = test_shard(i);
            assert_eq!(a.resolve(shard), b.resolve(shard));
        }
    }

    #[test]
    fn ring_changes_predictably_on_reconfiguration() {
        let ring1 = IptoPlacementRing::new(
            PlacementEpoch(1),
            &[
                slot("ipto-a", 256),
                slot("ipto-b", 256),
                slot("ipto-c", 256),
            ],
        );
        let ring2 = IptoPlacementRing::new(
            PlacementEpoch(2),
            &[
                slot("ipto-a", 256),
                slot("ipto-b", 256),
                slot("ipto-c", 256),
                slot("ipto-d", 256),
            ],
        );

        let mut moved = 0u64;
        let total = 50_000u64;
        for i in 0..total {
            let shard = test_shard(i);
            let from = ring1.resolve(shard).unwrap();
            let to = ring2.resolve(shard).unwrap();
            if from != to {
                moved += 1;
            }
        }
        let fraction = moved as f64 / total as f64;

        assert!(
            (0.15..=0.40).contains(&fraction),
            "expected ~25% moved, got {:.1}%",
            fraction * 100.0
        );
    }

    #[test]
    fn ring_empty_resolves_to_none() {
        let ring = IptoPlacementRing::new(PlacementEpoch(1), &[]);
        assert!(ring.is_empty());
        assert_eq!(ring.resolve(DataIndividualShardId(0)), None);
    }

    #[test]
    fn ring_single_instance_resolves_everything() {
        let ring = IptoPlacementRing::new(PlacementEpoch(1), &[slot("ipto-a", 1)]);
        for i in 0..1000u64 {
            let shard = test_shard(i);
            assert_eq!(
                ring.resolve(shard),
                Some(&IptoInstanceId::from("ipto-a"))
            );
        }
    }

    // -- placement map tests --

    #[test]
    fn placement_map_resolves_via_ring_by_default() {
        let map = IptoPlacementMap::new(
            PlacementEpoch(1),
            vec![slot("ipto-a", 64), slot("ipto-b", 64)],
            vec![],
        )
        .unwrap();

        for i in 0..100u64 {
            let shard = test_shard(i);
            let target = map.resolve(shard);
            assert!(target.is_some());
            let name = target.unwrap().as_str();
            assert!(name == "ipto-a" || name == "ipto-b");
        }
    }

    #[test]
    fn placement_map_overrides_take_priority() {
        let map = IptoPlacementMap::new(
            PlacementEpoch(1),
            vec![slot("ipto-a", 64), slot("ipto-b", 64)],
            vec![
                IptoPlacementRange::new(
                    DataIndividualShardId(100),
                    DataIndividualShardId(199),
                    IptoInstanceId::from("ipto-compliance"),
                )
                .unwrap(),
            ],
        )
        .unwrap();

        // Inside override range → compliance.
        assert_eq!(
            map.resolve(DataIndividualShardId(150)),
            Some(&IptoInstanceId::from("ipto-compliance"))
        );

        // Outside override range → ring (must be one of the ring instances).
        let resolved = map.resolve(DataIndividualShardId(50)).unwrap();
        assert!(resolved.as_str() == "ipto-a" || resolved.as_str() == "ipto-b");
    }

    #[test]
    fn placement_map_rejects_empty_slots() {
        let error = IptoPlacementMap::new(PlacementEpoch(1), vec![], vec![]).unwrap_err();
        assert!(matches!(error, ClusterControlError::NoPlacementSlots));
    }

    #[test]
    fn placement_map_rejects_overlapping_overrides() {
        let error = IptoPlacementMap::new(
            PlacementEpoch(1),
            vec![slot("ipto-a", 1)],
            vec![
                IptoPlacementRange::new(
                    DataIndividualShardId(0),
                    DataIndividualShardId(10),
                    IptoInstanceId::from("ipto-a"),
                )
                .unwrap(),
                IptoPlacementRange::new(
                    DataIndividualShardId(10),
                    DataIndividualShardId(20),
                    IptoInstanceId::from("ipto-b"),
                )
                .unwrap(),
            ],
        )
        .unwrap_err();

        assert!(matches!(
            error,
            ClusterControlError::OverlappingPlacementRanges { .. }
        ));
    }

    // -- cluster control state tests --

    #[test]
    fn control_state_applies_placement_lease_checkpoint_and_segment_manifest() {
        let mut state = ClusterControlState::new();
        state
            .apply(ClusterControlCommand::AddNode(NodeId::from("node-a")))
            .unwrap();

        let map = IptoPlacementMap::new(
            PlacementEpoch(1),
            vec![slot("ipto-a", 64), slot("ipto-b", 64)],
            vec![],
        )
        .unwrap();
        state
            .apply(ClusterControlCommand::SetIptoPlacementMap(map))
            .unwrap();

        assert!(state
            .resolve_ipto_target(DataIndividualShardId(42))
            .is_some());

        state
            .apply(ClusterControlCommand::GrantWriterLease(WriterLease {
                target: IptoInstanceId::from("ipto-a"),
                holder: NodeId::from("node-a"),
                epoch: LeaseEpoch(1),
            }))
            .unwrap();
        assert_eq!(
            state
                .writer_lease(&IptoInstanceId::from("ipto-a"))
                .unwrap()
                .holder,
            NodeId::from("node-a")
        );

        state
            .apply(ClusterControlCommand::RecordOutboxCheckpoint(
                MetadataOutboxCheckpoint {
                    data_individual_shard_id: DataIndividualShardId(42),
                    target: IptoInstanceId::from("ipto-a"),
                    segment_id: SegmentId::from("segment-a"),
                    last_acknowledged_offset: RecordOffset(128),
                    mapping_version: String::from("mapping-v1"),
                    epoch: CheckpointEpoch(1),
                },
            ))
            .unwrap();
        assert_eq!(
            state
                .outbox_checkpoint(DataIndividualShardId(42), &IptoInstanceId::from("ipto-a"))
                .unwrap()
                .last_acknowledged_offset,
            RecordOffset(128)
        );

        state
            .apply(ClusterControlCommand::RecordSealedSegment(
                SegmentManifest {
                    segment_id: SegmentId::from("segment-a"),
                    node_id: NodeId::from("node-a"),
                    path: PathBuf::from("segment-a.seg"),
                    record_count: 3,
                    byte_len: 128,
                    checksum: 42,
                },
            ))
            .unwrap();
        assert!(
            state
                .sealed_segment(&SegmentId::from("segment-a"))
                .is_some()
        );
    }

    #[test]
    fn control_state_rejects_stale_lease_epoch() {
        let mut state = ClusterControlState::new();
        state
            .apply(ClusterControlCommand::AddNode(NodeId::from("node-a")))
            .unwrap();
        state
            .apply(ClusterControlCommand::GrantWriterLease(WriterLease {
                target: IptoInstanceId::from("ipto-a"),
                holder: NodeId::from("node-a"),
                epoch: LeaseEpoch(2),
            }))
            .unwrap();

        let error = state
            .apply(ClusterControlCommand::GrantWriterLease(WriterLease {
                target: IptoInstanceId::from("ipto-a"),
                holder: NodeId::from("node-a"),
                epoch: LeaseEpoch(1),
            }))
            .unwrap_err();

        assert!(matches!(error, ClusterControlError::StaleLeaseEpoch { .. }));
    }

    #[test]
    fn control_state_keeps_placement_history_and_prunes_old_epochs() {
        let mut state = ClusterControlState::new();
        state
            .apply(ClusterControlCommand::AddNode(NodeId::from("node-a")))
            .unwrap();

        for epoch in 1..=7u64 {
            let map = IptoPlacementMap::new(
                PlacementEpoch(epoch),
                vec![slot("ipto-a", 1)],
                vec![],
            )
            .unwrap();
            state
                .apply(ClusterControlCommand::SetIptoPlacementMap(map))
                .unwrap();
        }

        assert_eq!(state.placement_maps.len(), MAX_PLACEMENT_HISTORY);
        assert_eq!(state.placement_map().unwrap().epoch, PlacementEpoch(7));

        let oldest = state.placement_maps.keys().next().copied().unwrap();
        assert_eq!(oldest, PlacementEpoch(3));
    }

    #[test]
    fn resolve_with_fallback_returns_candidates_in_epoch_order() {
        let mut state = ClusterControlState::new();

        let map1 = IptoPlacementMap::new(
            PlacementEpoch(1),
            vec![slot("ipto-a", 1)],
            vec![],
        )
        .unwrap();
        let map2 = IptoPlacementMap::new(
            PlacementEpoch(2),
            vec![slot("ipto-b", 1)],
            vec![],
        )
        .unwrap();

        state
            .apply(ClusterControlCommand::SetIptoPlacementMap(map1))
            .unwrap();
        state
            .apply(ClusterControlCommand::SetIptoPlacementMap(map2))
            .unwrap();

        // With single-instance rings, every shard maps to the sole instance.
        let candidates = state.resolve_with_fallback(DataIndividualShardId(42));

        // Epoch 2 (ipto-b) first, then epoch 1 (ipto-a).
        assert_eq!(candidates.len(), 2);
        assert_eq!(candidates[0], IptoInstanceId::from("ipto-b"));
        assert_eq!(candidates[1], IptoInstanceId::from("ipto-a"));
    }
}
