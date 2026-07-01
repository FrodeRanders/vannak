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
#[cfg(feature = "ipto-writer")]
pub mod ipto_adapter;
#[cfg(feature = "kafka-client")]
pub mod kafka_client;
#[cfg(feature = "sitas-runtime")]
pub mod kafka_ingest;
pub mod metadata;
pub mod observability;
pub mod process;
pub mod query;
#[cfg(feature = "raft")]
pub mod raft_sm;
pub mod runtime;
pub mod service;
#[cfg(feature = "sitas-runtime")]
pub mod sitas_runtime;
pub mod storage;

pub use cluster::{
    CheckpointEpoch, CheckpointManifest, ClusterControlCommand, ClusterControlError,
    ClusterControlState, IptoPlacementMap, IptoPlacementRange, IptoPlacementRing,
    IptoPlacementSlot, LeaseEpoch, MetadataOutboxCheckpoint, NodeId, PlacementEpoch, WriterLease,
};
pub use data::{
    ActiveMetadata, DataIndividualId, DataIndividualMetadataEvent, DataIndividualShardId,
    DataProvenanceIndex, DataProvenanceIndexSnapshot, DataProvenanceIngestOutcome, IdempotencyKey,
    MetadataEventId, MetadataFieldName, MetadataOperation, MetadataValue, PassiveMetadata,
    PayloadRef, PluginName, PluginVersion,
};
pub use index::{HotIndex, IngestOutcome};
pub use ingest::{
    EventId, EventTimestamp, IngestError, JournaledPipelineEvent, PipelineEvent,
    ProcessEventDecodeError, ProcessEventJournal, ProcessEventJournalError,
    ProcessEventJournalRecovery, ProcessEventReplay, ProcessEventReplaySummary, SourceId,
    SourceSequence, replay_process_event_segment,
};
pub use ipto::{
    DurableOutboxEnqueueResult, IptoAttributeName, IptoInstanceId, IptoMapping,
    IptoPayloadDecodeError, IptoWriteError, IptoWritePayload, IptoWriter, MetadataOutbox,
    MetadataOutboxDeliveryResult, MetadataOutboxDrainSummary, MetadataOutboxEntry,
    MetadataOutboxRebalanceSummary, MetadataOutboxReplay, MetadataOutboxReplaySummary,
    MetadataOutboxSnapshot, MetadataOutboxStorageError, OutboxEnqueueResult, OutboxStatus,
    SegmentBackedMetadataOutbox, SegmentBackedMetadataOutboxRecovery,
    SegmentBackedMetadataOutboxSnapshot, deliver_next_pending, deliver_next_pending_for_target,
    drain_pending_outbox, drain_pending_outbox_for_target, rebalance_shard_range_to,
    replay_metadata_outbox_segment, replay_metadata_outbox_segment_after,
    replay_metadata_outbox_segment_for_shard_range,
};
#[cfg(feature = "kafka-client")]
pub use kafka_client::{
    KafkaClientError, KafkaPayloadFormat, KafkaProcessConsumer, KafkaProcessConsumerConfig,
    KafkaProcessConsumerSnapshot, KafkaRebalanceEvent, KafkaRebalanceSnapshot,
};
#[cfg(feature = "sitas-runtime")]
pub use kafka_ingest::{
    KafkaIngestError, KafkaOffset, KafkaPendingRecord, KafkaProcessRecord, KafkaSubmitOutcome,
    KafkaTopicPartition, submit_kafka_process_record, try_submit_kafka_pending_record,
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
    ActivityMetadataQuery, DataIndividualMetadataQuery, EventQuery, ImpactQuery, PipelineQuery,
    ProcessInstanceQuery, ProcessMetadataQuery, ProcessStatusQuery, QueryLimit, QueryResult,
    TimeRangeQuery,
};
pub use runtime::{
    BoundedIngestRuntime, BoundedIngestRuntimeSnapshot, IngestDrainSummary, IngestQueueSnapshot,
    LogicalShardId, QueuedIngestOutcome, RuntimeError, ShardIngestOutcome, ShardLocalRuntime,
    ShardRuntimeSnapshot, ShardSnapshot,
};
pub use service::{
    DurableProcessIngestResult, MetadataCaptureResult, VannakService, VannakServiceError,
    VannakServiceRecovery, VannakServiceSnapshot,
};
#[cfg(feature = "sitas-runtime")]
pub use sitas_runtime::{
    SitasMailboxSnapshot, SitasRuntimeConfig, SitasRuntimeError, SitasRuntimeSnapshot,
    SitasShardRuntime,
};
pub use storage::{
    RecordOffset, SegmentError, SegmentId, SegmentManifest, SegmentReader, SegmentRecord,
    SegmentWriter,
};
