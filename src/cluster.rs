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

use crate::data::DataIndividualShardId;
use crate::ipto::IpToInstanceId;
use crate::storage::{RecordOffset, SegmentId, SegmentManifest};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

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

/// Inclusive data-individual shard range assigned to one IpTo instance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IpToPlacementRange {
    pub start: DataIndividualShardId,
    pub end: DataIndividualShardId,
    pub target: IpToInstanceId,
}

impl IpToPlacementRange {
    pub fn new(
        start: DataIndividualShardId,
        end: DataIndividualShardId,
        target: IpToInstanceId,
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

/// Versioned placement map replicated through Raft.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IpToPlacementMap {
    pub epoch: PlacementEpoch,
    ranges: Vec<IpToPlacementRange>,
}

impl IpToPlacementMap {
    pub fn new(
        epoch: PlacementEpoch,
        mut ranges: Vec<IpToPlacementRange>,
    ) -> Result<Self, ClusterControlError> {
        ranges.sort_by_key(|range| range.start);
        for pair in ranges.windows(2) {
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
        Ok(Self { epoch, ranges })
    }

    pub fn resolve(&self, shard_id: DataIndividualShardId) -> Option<&IpToInstanceId> {
        self.ranges
            .iter()
            .find(|range| range.contains(shard_id))
            .map(|range| &range.target)
    }

    pub fn ranges(&self) -> &[IpToPlacementRange] {
        &self.ranges
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriterLease {
    pub target: IpToInstanceId,
    pub holder: NodeId,
    pub epoch: LeaseEpoch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataOutboxCheckpoint {
    pub data_individual_shard_id: DataIndividualShardId,
    pub target: IpToInstanceId,
    pub segment_id: SegmentId,
    pub last_acknowledged_offset: RecordOffset,
    pub mapping_version: String,
    pub epoch: CheckpointEpoch,
}

/// Dependency-free reducer for the compact state a Raft state machine would
/// apply after log entries commit.
#[derive(Debug, Default)]
pub struct ClusterControlState {
    nodes: BTreeSet<NodeId>,
    placement_map: Option<IpToPlacementMap>,
    writer_leases: BTreeMap<IpToInstanceId, WriterLease>,
    outbox_checkpoints: BTreeMap<(DataIndividualShardId, IpToInstanceId), MetadataOutboxCheckpoint>,
    sealed_segments: BTreeMap<SegmentId, SegmentManifest>,
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
            ClusterControlCommand::SetIpToPlacementMap(map) => {
                if let Some(current) = &self.placement_map
                    && map.epoch <= current.epoch
                {
                    return Err(ClusterControlError::StalePlacementEpoch {
                        current: current.epoch,
                        proposed: map.epoch,
                    });
                }
                self.placement_map = Some(map);
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
        }

        Ok(())
    }

    pub fn nodes(&self) -> &BTreeSet<NodeId> {
        &self.nodes
    }

    pub fn placement_map(&self) -> Option<&IpToPlacementMap> {
        self.placement_map.as_ref()
    }

    pub fn resolve_ipto_target(&self, shard_id: DataIndividualShardId) -> Option<&IpToInstanceId> {
        self.placement_map()?.resolve(shard_id)
    }

    pub fn writer_lease(&self, target: &IpToInstanceId) -> Option<&WriterLease> {
        self.writer_leases.get(target)
    }

    pub fn outbox_checkpoint(
        &self,
        shard_id: DataIndividualShardId,
        target: &IpToInstanceId,
    ) -> Option<&MetadataOutboxCheckpoint> {
        self.outbox_checkpoints.get(&(shard_id, target.clone()))
    }

    pub fn sealed_segment(&self, segment_id: &SegmentId) -> Option<&SegmentManifest> {
        self.sealed_segments.get(segment_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterControlCommand {
    AddNode(NodeId),
    RemoveNode(NodeId),
    SetIpToPlacementMap(IpToPlacementMap),
    GrantWriterLease(WriterLease),
    RecordOutboxCheckpoint(MetadataOutboxCheckpoint),
    RecordSealedSegment(SegmentManifest),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterControlError {
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
        target: IpToInstanceId,
        current: LeaseEpoch,
        proposed: LeaseEpoch,
    },
    StaleCheckpointEpoch {
        shard_id: DataIndividualShardId,
        target: IpToInstanceId,
        current: CheckpointEpoch,
        proposed: CheckpointEpoch,
    },
}

impl fmt::Display for ClusterControlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPlacementRange { start, end } => write!(
                f,
                "invalid IpTo placement range: start {} is greater than end {}",
                start.0, end.0
            ),
            Self::OverlappingPlacementRanges {
                left_start,
                left_end,
                right_start,
                right_end,
            } => write!(
                f,
                "overlapping IpTo placement ranges: {}..={} overlaps {}..={}",
                left_start.0, left_end.0, right_start.0, right_end.0
            ),
            Self::StalePlacementEpoch { current, proposed } => write!(
                f,
                "stale IpTo placement epoch: current {}, proposed {}",
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
        }
    }
}

impl std::error::Error for ClusterControlError {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn placement_map_resolves_non_overlapping_ranges() {
        let map = IpToPlacementMap::new(
            PlacementEpoch(1),
            vec![
                IpToPlacementRange::new(
                    DataIndividualShardId(10),
                    DataIndividualShardId(19),
                    IpToInstanceId::from("ipto-b"),
                )
                .unwrap(),
                IpToPlacementRange::new(
                    DataIndividualShardId(0),
                    DataIndividualShardId(9),
                    IpToInstanceId::from("ipto-a"),
                )
                .unwrap(),
            ],
        )
        .unwrap();

        assert_eq!(
            map.resolve(DataIndividualShardId(3)),
            Some(&IpToInstanceId::from("ipto-a"))
        );
        assert_eq!(
            map.resolve(DataIndividualShardId(12)),
            Some(&IpToInstanceId::from("ipto-b"))
        );
        assert_eq!(map.resolve(DataIndividualShardId(20)), None);
    }

    #[test]
    fn placement_map_rejects_overlaps() {
        let error = IpToPlacementMap::new(
            PlacementEpoch(1),
            vec![
                IpToPlacementRange::new(
                    DataIndividualShardId(0),
                    DataIndividualShardId(10),
                    IpToInstanceId::from("ipto-a"),
                )
                .unwrap(),
                IpToPlacementRange::new(
                    DataIndividualShardId(10),
                    DataIndividualShardId(20),
                    IpToInstanceId::from("ipto-b"),
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

    #[test]
    fn control_state_applies_placement_lease_checkpoint_and_segment_manifest() {
        let mut state = ClusterControlState::new();
        state
            .apply(ClusterControlCommand::AddNode(NodeId::from("node-a")))
            .unwrap();
        state
            .apply(ClusterControlCommand::SetIpToPlacementMap(
                IpToPlacementMap::new(
                    PlacementEpoch(1),
                    vec![
                        IpToPlacementRange::new(
                            DataIndividualShardId(0),
                            DataIndividualShardId(99),
                            IpToInstanceId::from("ipto-a"),
                        )
                        .unwrap(),
                    ],
                )
                .unwrap(),
            ))
            .unwrap();

        assert_eq!(
            state.resolve_ipto_target(DataIndividualShardId(42)),
            Some(&IpToInstanceId::from("ipto-a"))
        );

        state
            .apply(ClusterControlCommand::GrantWriterLease(WriterLease {
                target: IpToInstanceId::from("ipto-a"),
                holder: NodeId::from("node-a"),
                epoch: LeaseEpoch(1),
            }))
            .unwrap();
        assert_eq!(
            state
                .writer_lease(&IpToInstanceId::from("ipto-a"))
                .unwrap()
                .holder,
            NodeId::from("node-a")
        );

        state
            .apply(ClusterControlCommand::RecordOutboxCheckpoint(
                MetadataOutboxCheckpoint {
                    data_individual_shard_id: DataIndividualShardId(42),
                    target: IpToInstanceId::from("ipto-a"),
                    segment_id: SegmentId::from("segment-a"),
                    last_acknowledged_offset: RecordOffset(128),
                    mapping_version: String::from("mapping-v1"),
                    epoch: CheckpointEpoch(1),
                },
            ))
            .unwrap();
        assert_eq!(
            state
                .outbox_checkpoint(DataIndividualShardId(42), &IpToInstanceId::from("ipto-a"))
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
                target: IpToInstanceId::from("ipto-a"),
                holder: NodeId::from("node-a"),
                epoch: LeaseEpoch(2),
            }))
            .unwrap();

        let error = state
            .apply(ClusterControlCommand::GrantWriterLease(WriterLease {
                target: IpToInstanceId::from("ipto-a"),
                holder: NodeId::from("node-a"),
                epoch: LeaseEpoch(1),
            }))
            .unwrap_err();

        assert!(matches!(error, ClusterControlError::StaleLeaseEpoch { .. }));
    }
}
