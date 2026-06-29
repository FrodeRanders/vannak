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

//! Vannak core types.
//!
//! This crate starts with the dependency-free domain layer: typed process
//! events, metadata references, process-state reduction, and a single-node hot
//! index. Sitas, Durga, Ipto, and Raft integrations should attach at the module
//! boundaries rather than changing the core event model.

pub mod cluster;
pub mod data;
pub mod durga;
pub mod index;
pub mod ingest;
pub mod ipto;
pub mod metadata;
pub mod observability;
pub mod process;
pub mod query;
pub mod runtime;
pub mod storage;

pub use cluster::{
    CheckpointEpoch, ClusterControlCommand, ClusterControlError, ClusterControlState,
    IptoPlacementMap, IptoPlacementRange, LeaseEpoch, MetadataOutboxCheckpoint, NodeId,
    PlacementEpoch, WriterLease,
};
pub use data::{
    ActiveMetadata, DataIndividualId, DataIndividualMetadataEvent, DataIndividualShardId,
    IdempotencyKey, MetadataEventId, MetadataFieldName, MetadataOperation, MetadataValue,
    PassiveMetadata, PayloadRef, PluginName, PluginVersion,
};
pub use index::{HotIndex, IngestOutcome};
pub use ingest::{EventId, EventTimestamp, IngestError, PipelineEvent, SourceId, SourceSequence};
pub use ipto::{
    DurableOutboxEnqueueResult, IptoAttributeName, IptoInstanceId, IptoMapping,
    IptoPayloadDecodeError, IptoPlacement, IptoPlacementError, IptoWriteError, IptoWritePayload,
    IptoWriter, MetadataOutbox, MetadataOutboxDeliveryResult, MetadataOutboxDrainSummary,
    MetadataOutboxEntry, MetadataOutboxSnapshot, MetadataOutboxStorageError, OutboxEnqueueResult,
    OutboxStatus, SegmentBackedMetadataOutbox, SegmentBackedMetadataOutboxSnapshot,
    deliver_next_pending, drain_pending_outbox, replay_metadata_outbox_segment,
    replay_metadata_outbox_segment_after,
};
pub use metadata::{
    ClassificationId, DataContractId, DatasetId, FieldId, LineageEdgeId, MetadataObjectId,
    MetadataRef, MetadataVersion, OwnerId, PipelineDefinitionId, SchemaId,
};
pub use process::{
    ActivityId, ActivityState, BusinessKey, CorrelationId, EnvironmentId, ErrorInfo, EventKind,
    EventStatus, PipelineId, ProcessDefinitionId, ProcessInstanceId, ProcessInstanceSnapshot,
    ProcessInstanceState, ProcessStatus, ProcessVersion, TenantId, TokenId,
};
pub use query::{
    EventQuery, ImpactQuery, PipelineQuery, ProcessInstanceQuery, QueryLimit, QueryResult,
};
pub use storage::{
    RecordOffset, SegmentError, SegmentId, SegmentManifest, SegmentReader, SegmentRecord,
    SegmentWriter,
};
