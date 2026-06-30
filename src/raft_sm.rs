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

//! Raft state machine adapter for `ClusterControlState`.
//!
//! Enabled with the `raft` feature flag. Implements `graft_core::StateMachine`
//! and `graft_core::QueryableStateMachine` for Vannak's cluster control state.
//!
//! The adapter uses interior mutability (`parking_lot::RwLock`) so the state
//! can be shared across Raft threads behind an `Arc`.

use crate::cluster::{
    CheckpointEpoch, CheckpointManifest, ClusterControlCommand, ClusterControlError,
    ClusterControlState, IptoPlacementMap, IptoPlacementSlot, IptoPlacementRange, LeaseEpoch,
    MetadataOutboxCheckpoint, NodeId, PlacementEpoch, WriterLease,
};
use crate::data::DataIndividualShardId;
use crate::ipto::IptoInstanceId;
use crate::storage::{RecordOffset, SegmentId, SegmentManifest};
use graft_core::state_machine::{QueryableStateMachine, StateMachine};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::RwLock;

// ---------------------------------------------------------------------------
// Serializable forms of cluster types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SerPlacementSlot {
    instance: String,
    vnodes: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SerPlacementRange {
    start: u64,
    end: u64,
    target: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SerPlacementMap {
    epoch: u64,
    slots: Vec<SerPlacementSlot>,
    overrides: Vec<SerPlacementRange>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SerWriterLease {
    target: String,
    holder: String,
    epoch: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SerOutboxCheckpoint {
    data_individual_shard_id: u64,
    target: String,
    segment_id: String,
    last_acknowledged_offset: u64,
    mapping_version: String,
    epoch: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SerSegmentManifest {
    segment_id: String,
    node_id: String,
    path: String,
    record_count: u64,
    byte_len: u64,
    checksum: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SerCheckpointManifest {
    checkpoint_id: String,
    node_id: String,
    epoch: u64,
    segment_offsets: Vec<SerSegmentOffset>,
    metadata_version: Option<String>,
    checksum: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SerSegmentOffset {
    segment_id: String,
    offset: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SerClusterControlSnapshot {
    nodes: Vec<String>,
    placement_maps: Vec<SerPlacementMap>,
    writer_leases: Vec<SerWriterLease>,
    outbox_checkpoints: Vec<SerOutboxCheckpoint>,
    sealed_segments: Vec<SerSegmentManifest>,
    checkpoints: Vec<SerCheckpointManifest>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SerClusterControlCommand {
    #[serde(flatten)]
    variant: SerCommandVariant,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "cmd", content = "data")]
enum SerCommandVariant {
    AddNode {
        node_id: String,
    },
    RemoveNode {
        node_id: String,
    },
    SetIptoPlacementMap {
        epoch: u64,
        slots: Vec<SerPlacementSlot>,
        overrides: Vec<SerPlacementRange>,
    },
    GrantWriterLease {
        target: String,
        holder: String,
        epoch: u64,
    },
    RecordOutboxCheckpoint {
        data_individual_shard_id: u64,
        target: String,
        segment_id: String,
        last_acknowledged_offset: u64,
        mapping_version: String,
        epoch: u64,
    },
    RecordSealedSegment {
        segment_id: String,
        node_id: String,
        path: String,
        record_count: u64,
        byte_len: u64,
        checksum: u64,
    },
    RecordCheckpoint {
        checkpoint_id: String,
        node_id: String,
        epoch: u64,
        segment_offsets: Vec<SerSegmentOffset>,
        metadata_version: Option<String>,
        checksum: u64,
    },
}

// ---------------------------------------------------------------------------
// Conversion helpers
// ---------------------------------------------------------------------------

fn to_ser_slots(slots: &[IptoPlacementSlot]) -> Vec<SerPlacementSlot> {
    slots
        .iter()
        .map(|s| SerPlacementSlot {
            instance: s.instance.as_str().to_string(),
            vnodes: s.vnodes,
        })
        .collect()
}

fn from_ser_slots(
    ser: &[SerPlacementSlot],
) -> Result<Vec<IptoPlacementSlot>, ClusterControlError> {
    ser.iter()
        .map(|s| {
            IptoPlacementSlot::new(IptoInstanceId::from(s.instance.as_str()), s.vnodes)
        })
        .collect::<Result<Vec<_>, _>>()
}

fn to_ser_overrides(ranges: &[IptoPlacementRange]) -> Vec<SerPlacementRange> {
    ranges
        .iter()
        .map(|r| SerPlacementRange {
            start: r.start.0,
            end: r.end.0,
            target: r.target.as_str().to_string(),
        })
        .collect()
}

fn from_ser_overrides(
    ser: &[SerPlacementRange],
) -> Result<Vec<IptoPlacementRange>, ClusterControlError> {
    ser.iter()
        .map(|r| {
            IptoPlacementRange::new(
                DataIndividualShardId(r.start),
                DataIndividualShardId(r.end),
                IptoInstanceId::from(r.target.as_str()),
            )
        })
        .collect::<Result<Vec<_>, _>>()
}

#[allow(dead_code)]
pub(crate) fn encode_command(cmd: &ClusterControlCommand) -> Result<Vec<u8>, serde_json::Error> {
    let ser_cmd = match cmd {
        ClusterControlCommand::AddNode(node_id) => SerClusterControlCommand {
            variant: SerCommandVariant::AddNode {
                node_id: node_id.as_str().to_string(),
            },
        },
        ClusterControlCommand::RemoveNode(node_id) => SerClusterControlCommand {
            variant: SerCommandVariant::RemoveNode {
                node_id: node_id.as_str().to_string(),
            },
        },
        ClusterControlCommand::SetIptoPlacementMap(map) => SerClusterControlCommand {
            variant: SerCommandVariant::SetIptoPlacementMap {
                epoch: map.epoch.0,
                slots: to_ser_slots(map.slots()),
                overrides: to_ser_overrides(map.overrides()),
            },
        },
        ClusterControlCommand::GrantWriterLease(lease) => SerClusterControlCommand {
            variant: SerCommandVariant::GrantWriterLease {
                target: lease.target.as_str().to_string(),
                holder: lease.holder.as_str().to_string(),
                epoch: lease.epoch.0,
            },
        },
        ClusterControlCommand::RecordOutboxCheckpoint(cp) => SerClusterControlCommand {
            variant: SerCommandVariant::RecordOutboxCheckpoint {
                data_individual_shard_id: cp.data_individual_shard_id.0,
                target: cp.target.as_str().to_string(),
                segment_id: cp.segment_id.as_str().to_string(),
                last_acknowledged_offset: cp.last_acknowledged_offset.0,
                mapping_version: cp.mapping_version.clone(),
                epoch: cp.epoch.0,
            },
        },
        ClusterControlCommand::RecordSealedSegment(manifest) => SerClusterControlCommand {
            variant: SerCommandVariant::RecordSealedSegment {
                segment_id: manifest.segment_id.as_str().to_string(),
                node_id: manifest.node_id.as_str().to_string(),
                path: manifest.path.to_string_lossy().to_string(),
                record_count: manifest.record_count,
                byte_len: manifest.byte_len,
                checksum: manifest.checksum,
            },
        },
        ClusterControlCommand::RecordCheckpoint(cp) => SerClusterControlCommand {
            variant: SerCommandVariant::RecordCheckpoint {
                checkpoint_id: cp.checkpoint_id.clone(),
                node_id: cp.node_id.as_str().to_string(),
                epoch: cp.epoch.0,
                segment_offsets: cp
                    .segment_offsets
                    .iter()
                    .map(|(seg_id, offset)| SerSegmentOffset {
                        segment_id: seg_id.as_str().to_string(),
                        offset: offset.0,
                    })
                    .collect(),
                metadata_version: cp.metadata_version.clone(),
                checksum: cp.checksum,
            },
        },
    };
    serde_json::to_vec(&ser_cmd)
}

pub(crate) fn decode_command(data: &[u8]) -> Result<ClusterControlCommand, String> {
    let ser_cmd: SerClusterControlCommand = serde_json::from_slice(data).map_err(|e| e.to_string())?;
    match ser_cmd.variant {
        SerCommandVariant::AddNode { node_id } => {
            Ok(ClusterControlCommand::AddNode(NodeId::from(node_id.as_str())))
        }
        SerCommandVariant::RemoveNode { node_id } => {
            Ok(ClusterControlCommand::RemoveNode(NodeId::from(node_id.as_str())))
        }
        SerCommandVariant::SetIptoPlacementMap {
            epoch,
            slots,
            overrides,
        } => {
            let slots = from_ser_slots(&slots).map_err(|e| e.to_string())?;
            let overrides = from_ser_overrides(&overrides).map_err(|e| e.to_string())?;
            let map = IptoPlacementMap::new(PlacementEpoch(epoch), slots, overrides)
                .map_err(|e| e.to_string())?;
            Ok(ClusterControlCommand::SetIptoPlacementMap(map))
        }
        SerCommandVariant::GrantWriterLease {
            target,
            holder,
            epoch,
        } => Ok(ClusterControlCommand::GrantWriterLease(WriterLease {
            target: IptoInstanceId::from(target.as_str()),
            holder: NodeId::from(holder.as_str()),
            epoch: LeaseEpoch(epoch),
        })),
        SerCommandVariant::RecordOutboxCheckpoint {
            data_individual_shard_id,
            target,
            segment_id,
            last_acknowledged_offset,
            mapping_version,
            epoch,
        } => Ok(ClusterControlCommand::RecordOutboxCheckpoint(
            MetadataOutboxCheckpoint {
                data_individual_shard_id: DataIndividualShardId(data_individual_shard_id),
                target: IptoInstanceId::from(target.as_str()),
                segment_id: SegmentId::from(segment_id.as_str()),
                last_acknowledged_offset: RecordOffset(last_acknowledged_offset),
                mapping_version,
                epoch: CheckpointEpoch(epoch),
            },
        )),
        SerCommandVariant::RecordSealedSegment {
            segment_id,
            node_id,
            path,
            record_count,
            byte_len,
            checksum,
        } => Ok(ClusterControlCommand::RecordSealedSegment(SegmentManifest {
            segment_id: SegmentId::from(segment_id.as_str()),
            node_id: NodeId::from(node_id.as_str()),
            path: PathBuf::from(path),
            record_count,
            byte_len,
            checksum,
        })),
        SerCommandVariant::RecordCheckpoint {
            checkpoint_id,
            node_id,
            epoch,
            segment_offsets,
            metadata_version,
            checksum,
        } => {
            let offsets: BTreeMap<SegmentId, RecordOffset> = segment_offsets
                .iter()
                .map(|so| {
                    (
                        SegmentId::from(so.segment_id.as_str()),
                        RecordOffset(so.offset),
                    )
                })
                .collect();
            Ok(ClusterControlCommand::RecordCheckpoint(
                CheckpointManifest {
                    checkpoint_id,
                    node_id: NodeId::from(node_id.as_str()),
                    epoch: CheckpointEpoch(epoch),
                    segment_offsets: offsets,
                    metadata_version,
                    checksum,
                },
            ))
        },
    }
}

fn encode_snapshot(state: &ClusterControlState) -> Result<Vec<u8>, serde_json::Error> {
    let snapshot = SerClusterControlSnapshot {
        nodes: state
            .nodes()
            .iter()
            .map(|n| n.as_str().to_string())
            .collect(),
        placement_maps: state
            .placement_map_history()
            .map(|(epoch, map)| SerPlacementMap {
                epoch: epoch.0,
                slots: to_ser_slots(map.slots()),
                overrides: to_ser_overrides(map.overrides()),
            })
            .collect(),
        writer_leases: state
            .writer_leases()
            .iter()
            .map(|(target, lease)| SerWriterLease {
                target: target.as_str().to_string(),
                holder: lease.holder.as_str().to_string(),
                epoch: lease.epoch.0,
            })
            .collect(),
        outbox_checkpoints: state
            .outbox_checkpoints()
            .iter()
            .map(|((shard_id, target), cp)| SerOutboxCheckpoint {
                data_individual_shard_id: shard_id.0,
                target: target.as_str().to_string(),
                segment_id: cp.segment_id.as_str().to_string(),
                last_acknowledged_offset: cp.last_acknowledged_offset.0,
                mapping_version: cp.mapping_version.clone(),
                epoch: cp.epoch.0,
            })
            .collect(),
        sealed_segments: state
            .sealed_segments()
            .values()
            .map(|manifest| SerSegmentManifest {
                segment_id: manifest.segment_id.as_str().to_string(),
                node_id: manifest.node_id.as_str().to_string(),
                path: manifest.path.to_string_lossy().to_string(),
                record_count: manifest.record_count,
                byte_len: manifest.byte_len,
                checksum: manifest.checksum,
            })
            .collect(),
        checkpoints: state
            .checkpoints()
            .values()
            .map(|cp| SerCheckpointManifest {
                checkpoint_id: cp.checkpoint_id.clone(),
                node_id: cp.node_id.as_str().to_string(),
                epoch: cp.epoch.0,
                segment_offsets: cp
                    .segment_offsets
                    .iter()
                    .map(|(seg_id, offset)| SerSegmentOffset {
                        segment_id: seg_id.as_str().to_string(),
                        offset: offset.0,
                    })
                    .collect(),
                metadata_version: cp.metadata_version.clone(),
                checksum: cp.checksum,
            })
            .collect(),
    };
    serde_json::to_vec(&snapshot)
}

fn decode_snapshot(data: &[u8]) -> Result<ClusterControlState, String> {
    let snapshot: SerClusterControlSnapshot =
        serde_json::from_slice(data).map_err(|e| e.to_string())?;

    let mut state = ClusterControlState::new();

    for node in &snapshot.nodes {
        state
            .apply(ClusterControlCommand::AddNode(NodeId::from(
                node.as_str(),
            )))
            .map_err(|e| e.to_string())?;
    }

    for map_data in &snapshot.placement_maps {
        let slots = from_ser_slots(&map_data.slots).map_err(|e| e.to_string())?;
        let overrides = from_ser_overrides(&map_data.overrides).map_err(|e| e.to_string())?;
        let map =
            IptoPlacementMap::new(PlacementEpoch(map_data.epoch), slots, overrides)
                .map_err(|e| e.to_string())?;
        state
            .apply(ClusterControlCommand::SetIptoPlacementMap(map))
            .map_err(|e| e.to_string())?;
    }

    for lease_data in &snapshot.writer_leases {
        state
            .apply(ClusterControlCommand::GrantWriterLease(WriterLease {
                target: IptoInstanceId::from(lease_data.target.as_str()),
                holder: NodeId::from(lease_data.holder.as_str()),
                epoch: LeaseEpoch(lease_data.epoch),
            }))
            .map_err(|e| e.to_string())?;
    }

    for cp_data in &snapshot.outbox_checkpoints {
        state
            .apply(ClusterControlCommand::RecordOutboxCheckpoint(
                MetadataOutboxCheckpoint {
                    data_individual_shard_id: DataIndividualShardId(
                        cp_data.data_individual_shard_id,
                    ),
                    target: IptoInstanceId::from(cp_data.target.as_str()),
                    segment_id: SegmentId::from(cp_data.segment_id.as_str()),
                    last_acknowledged_offset: RecordOffset(cp_data.last_acknowledged_offset),
                    mapping_version: cp_data.mapping_version.clone(),
                    epoch: CheckpointEpoch(cp_data.epoch),
                },
            ))
            .map_err(|e| e.to_string())?;
    }

    for seg_data in &snapshot.sealed_segments {
        state
            .apply(ClusterControlCommand::RecordSealedSegment(SegmentManifest {
                segment_id: SegmentId::from(seg_data.segment_id.as_str()),
                node_id: NodeId::from(seg_data.node_id.as_str()),
                path: PathBuf::from(seg_data.path.as_str()),
                record_count: seg_data.record_count,
                byte_len: seg_data.byte_len,
                checksum: seg_data.checksum,
            }))
            .map_err(|e| e.to_string())?;
    }

    for cp_data in &snapshot.checkpoints {
        let offsets: BTreeMap<SegmentId, RecordOffset> = cp_data
            .segment_offsets
            .iter()
            .map(|so| {
                (
                    SegmentId::from(so.segment_id.as_str()),
                    RecordOffset(so.offset),
                )
            })
            .collect();
        state
            .apply(ClusterControlCommand::RecordCheckpoint(
                CheckpointManifest {
                    checkpoint_id: cp_data.checkpoint_id.clone(),
                    node_id: NodeId::from(cp_data.node_id.as_str()),
                    epoch: CheckpointEpoch(cp_data.epoch),
                    segment_offsets: offsets,
                    metadata_version: cp_data.metadata_version.clone(),
                    checksum: cp_data.checksum,
                },
            ))
            .map_err(|e| e.to_string())?;
    }

    Ok(state)
}

// ---------------------------------------------------------------------------
// Public query enumeration
// ---------------------------------------------------------------------------

/// Read-only query against the cluster control state.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "query", content = "data")]
pub enum ClusterQuery {
    /// Resolve a shard ID to an Ipto instance (current epoch only).
    ResolveTarget { shard_id: u64 },
    /// Resolve with fallback through placement history.
    ResolveWithFallback { shard_id: u64 },
    /// List all known Ipto instances.
    ListIptoInstances,
    /// Get the current placement map epoch.
    CurrentPlacementEpoch,
    /// Get the writer lease for a target.
    GetWriterLease { target: String },
    /// Get an outbox checkpoint.
    GetOutboxCheckpoint { shard_id: u64, target: String },
    /// Get a sealed segment manifest.
    GetSealedSegment { segment_id: String },
}

fn handle_query(state: &ClusterControlState, query: &ClusterQuery) -> Vec<u8> {
    match query {
        ClusterQuery::ResolveTarget { shard_id } => {
            let result = state
                .resolve_ipto_target(DataIndividualShardId(*shard_id))
                .map(|t| t.as_str().to_string());
            serde_json::to_vec(&result).unwrap_or_default()
        }
        ClusterQuery::ResolveWithFallback { shard_id } => {
            let result: Vec<String> = state
                .resolve_with_fallback(DataIndividualShardId(*shard_id))
                .iter()
                .map(|t| t.as_str().to_string())
                .collect();
            serde_json::to_vec(&result).unwrap_or_default()
        }
        ClusterQuery::ListIptoInstances => {
            let result: Vec<String> = state
                .all_ipto_instances()
                .iter()
                .map(|t| t.as_str().to_string())
                .collect();
            serde_json::to_vec(&result).unwrap_or_default()
        }
        ClusterQuery::CurrentPlacementEpoch => {
            let result = state.placement_map().map(|m| m.epoch.0);
            serde_json::to_vec(&result).unwrap_or_default()
        }
        ClusterQuery::GetWriterLease { target } => {
            let target_id = IptoInstanceId::from(target.as_str());
            let result = state
                .writer_lease(&target_id)
                .map(|l| l.holder.as_str().to_string());
            serde_json::to_vec(&result).unwrap_or_default()
        }
        ClusterQuery::GetOutboxCheckpoint { shard_id, target } => {
            let target_id = IptoInstanceId::from(target.as_str());
            let result = state
                .outbox_checkpoint(DataIndividualShardId(*shard_id), &target_id)
                .map(|cp| cp.last_acknowledged_offset.0);
            serde_json::to_vec(&result).unwrap_or_default()
        }
        ClusterQuery::GetSealedSegment { segment_id } => {
            let result = state
                .sealed_segment(&SegmentId::from(segment_id.as_str()))
                .map(|m| m.checksum);
            serde_json::to_vec(&result).unwrap_or_default()
        }
    }
}

// ---------------------------------------------------------------------------
// State machine adapter
// ---------------------------------------------------------------------------

/// Thread-safe Raft state machine wrapping `ClusterControlState`.
///
/// Uses interior mutability via `parking_lot::RwLock`. The Raft runtime calls
/// `apply` from the commit path (write lock) and `query` from the read path
/// (read lock). Snapshot and restore also take the write lock.
pub struct ClusterStateMachine {
    state: RwLock<ClusterControlState>,
}

impl ClusterStateMachine {
    pub fn new() -> Self {
        Self {
            state: RwLock::new(ClusterControlState::new()),
        }
    }

    pub fn state(&self) -> std::sync::RwLockReadGuard<'_, ClusterControlState> {
        self.state.read().unwrap()
    }
}

impl Default for ClusterStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

impl StateMachine for ClusterStateMachine {
    fn apply(&self, _term: u64, command: &[u8]) {
        let cmd = match decode_command(command) {
            Ok(cmd) => cmd,
            Err(_) => return,
        };
        let _ = self.state.write().unwrap().apply(cmd);
    }

    fn apply_with_result(&self, _term: u64, command: &[u8]) -> Vec<u8> {
        let cmd = match decode_command(command) {
            Ok(cmd) => cmd,
            Err(e) => return serde_json::to_vec(&e).unwrap_or_default(),
        };
        match self.state.write().unwrap().apply(cmd) {
            Ok(()) => Vec::new(),
            Err(e) => serde_json::to_vec(&e.to_string()).unwrap_or_default(),
        }
    }

    fn snapshot(&self) -> Vec<u8> {
        encode_snapshot(&self.state.read().unwrap()).unwrap_or_default()
    }

    fn restore(&self, snapshot_data: &[u8]) {
        if let Ok(state) = decode_snapshot(snapshot_data) {
            *self.state.write().unwrap() = state;
        }
    }

    fn as_queryable(&self) -> Option<&dyn QueryableStateMachine> {
        Some(self)
    }
}

impl QueryableStateMachine for ClusterStateMachine {
    fn query(&self, request: &[u8]) -> Vec<u8> {
        let query: ClusterQuery = match serde_json::from_slice(request) {
            Ok(q) => q,
            Err(e) => return serde_json::to_vec(&e.to_string()).unwrap_or_default(),
        };
        handle_query(&self.state.read().unwrap(), &query)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn slot(instance: &str, vnodes: u32) -> IptoPlacementSlot {
        IptoPlacementSlot::new(IptoInstanceId::from(instance), vnodes).unwrap()
    }

    fn build_test_state() -> ClusterControlState {
        let mut state = ClusterControlState::new();
        state
            .apply(ClusterControlCommand::AddNode(NodeId::from("node-a")))
            .unwrap();
        state
            .apply(ClusterControlCommand::AddNode(NodeId::from("node-b")))
            .unwrap();
        state
            .apply(ClusterControlCommand::SetIptoPlacementMap(
                IptoPlacementMap::new(
                    PlacementEpoch(1),
                    vec![slot("ipto-a", 64), slot("ipto-b", 64)],
                    vec![],
                )
                .unwrap(),
            ))
            .unwrap();
        state
            .apply(ClusterControlCommand::GrantWriterLease(WriterLease {
                target: IptoInstanceId::from("ipto-a"),
                holder: NodeId::from("node-a"),
                epoch: LeaseEpoch(1),
            }))
            .unwrap();
        state
            .apply(ClusterControlCommand::RecordSealedSegment(
                SegmentManifest {
                    segment_id: SegmentId::from("seg-1"),
                    node_id: NodeId::from("node-a"),
                    path: PathBuf::from("seg-1.seg"),
                    record_count: 42,
                    byte_len: 1024,
                    checksum: 12345,
                },
            ))
            .unwrap();
        state
    }

    #[test]
    fn command_round_trips_through_json() {
        let cmd = ClusterControlCommand::AddNode(NodeId::from("node-a"));
        let encoded = encode_command(&cmd).unwrap();
        let decoded = decode_command(&encoded).unwrap();
        assert_eq!(decoded, cmd);
    }

    #[test]
    fn placement_map_command_round_trips() {
        let map = IptoPlacementMap::new(
            PlacementEpoch(2),
            vec![slot("ipto-x", 128)],
            vec![IptoPlacementRange::new(
                DataIndividualShardId(10),
                DataIndividualShardId(20),
                IptoInstanceId::from("ipto-override"),
            )
            .unwrap()],
        )
        .unwrap();
        let cmd = ClusterControlCommand::SetIptoPlacementMap(map);
        let encoded = encode_command(&cmd).unwrap();
        let decoded = decode_command(&encoded).unwrap();

        match (&cmd, &decoded) {
            (
                ClusterControlCommand::SetIptoPlacementMap(orig),
                ClusterControlCommand::SetIptoPlacementMap(round),
            ) => {
                assert_eq!(orig.epoch, round.epoch);
                assert_eq!(orig.slots().len(), round.slots().len());
                assert_eq!(orig.overrides(), round.overrides());
            }
            _ => panic!("expected SetIptoPlacementMap"),
        }
    }

    #[test]
    fn snapshot_round_trips() {
        let state = build_test_state();
        let encoded = encode_snapshot(&state).unwrap();
        let decoded = decode_snapshot(&encoded).unwrap();

        assert_eq!(state.nodes().len(), decoded.nodes().len());
        assert_eq!(
            state.placement_map_history().count(),
            decoded.placement_map_history().count()
        );
        assert!(decoded.resolve_ipto_target(DataIndividualShardId(42)).is_some());
    }

    #[test]
    fn state_machine_apply_and_snapshot() {
        let sm = ClusterStateMachine::new();

        let cmd = ClusterControlCommand::AddNode(NodeId::from("node-a"));
        let encoded = encode_command(&cmd).unwrap();
        sm.apply(1, &encoded);

        let snapshot = sm.snapshot();
        let sm2 = ClusterStateMachine::new();
        sm2.restore(&snapshot);

        assert_eq!(sm.state().nodes().len(), sm2.state().nodes().len());
    }

    #[test]
    fn state_machine_query() {
        let sm = ClusterStateMachine::new();
        let state = build_test_state();
        *sm.state.write().unwrap() = state;

        let query = serde_json::to_vec(&ClusterQuery::ResolveWithFallback { shard_id: 42 }).unwrap();
        let result: Vec<String> = serde_json::from_slice(&sm.query(&query)).unwrap();
        assert!(!result.is_empty());
    }

    #[test]
    fn checkpoint_manifest_round_trips() {
        let mut offsets = BTreeMap::new();
        offsets.insert(SegmentId::from("seg-1"), RecordOffset(1024));
        offsets.insert(SegmentId::from("seg-2"), RecordOffset(2048));

        let cp = CheckpointManifest::new(
            "cp-001",
            NodeId::from("node-a"),
            CheckpointEpoch(1),
            offsets,
            Some("mapping-v2".into()),
            12345,
        );

        let cmd = ClusterControlCommand::RecordCheckpoint(cp);
        let encoded = encode_command(&cmd).unwrap();
        let decoded = decode_command(&encoded).unwrap();

        match decoded {
            ClusterControlCommand::RecordCheckpoint(round) => {
                assert_eq!(round.checkpoint_id, "cp-001");
                assert_eq!(round.node_id, NodeId::from("node-a"));
                assert_eq!(round.epoch, CheckpointEpoch(1));
                assert_eq!(round.segment_offsets.len(), 2);
                assert_eq!(
                    round.segment_offsets.get(&SegmentId::from("seg-1")),
                    Some(&RecordOffset(1024))
                );
                assert_eq!(round.metadata_version, Some("mapping-v2".into()));
                assert_eq!(round.checksum, 12345);
            }
            _ => panic!("expected RecordCheckpoint"),
        }
    }
}
