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

//! Single-node service orchestration boundary.
//!
//! This module wires the dependency-free primitives together without pulling in
//! external runtimes. It is the bridge between the current library pieces and a
//! future Sitas-backed service: process events are reduced into a shard-local
//! hot index, metadata events are placed, mapped, durably enqueued, and drained
//! through target-aware writer calls.

use crate::cluster::{
    CheckpointEpoch, ClusterControlState, IptoPlacementMap, MetadataOutboxCheckpoint, NodeId,
};
use crate::data::{
    DataIndividualId, DataIndividualMetadataEvent, DataIndividualShardId, DataProvenanceIndex,
    DataProvenanceIndexSnapshot,
};
use crate::index::{HotIndex, IngestOutcome};
use crate::ingest::{
    IngestError, PipelineEvent, ProcessEventJournal, ProcessEventJournalError,
    ProcessEventReplaySummary,
};
use crate::ipto::{
    DurableOutboxEnqueueResult, IptoInstanceId, IptoMapping, IptoWritePayload, IptoWriter,
    MetadataOutboxDrainSummary, MetadataOutboxReplaySummary, MetadataOutboxStorageError,
    SegmentBackedMetadataOutbox, SegmentBackedMetadataOutboxSnapshot,
    drain_pending_outbox_for_target, replay_metadata_outbox_segment_after,
};
use crate::observability::HotIndexSnapshot;
use crate::query::{
    ActivityMetadataQuery, DataIndividualMetadataQuery, EventQuery, ImpactQuery, PipelineQuery,
    ProcessInstanceQuery, ProcessMetadataQuery, ProcessStatusQuery, QueryResult, TimeRangeQuery,
};
use crate::storage::{RecordOffset, SegmentId, SegmentManifest};
use std::fmt;
use std::path::Path;

/// Single-node Vannak service state.
///
/// This type is intentionally not concurrent. A Sitas integration should keep
/// one value, or equivalent shard-local state, inside each owning shard.
#[derive(Debug)]
pub struct VannakService {
    hot_index: HotIndex,
    placement_map: IptoPlacementMap,
    mapping: IptoMapping,
    metadata_outbox: SegmentBackedMetadataOutbox,
    provenance_index: DataProvenanceIndex,
    process_event_journal: Option<ProcessEventJournal>,
}

impl VannakService {
    pub fn new(
        placement_map: IptoPlacementMap,
        mapping: IptoMapping,
        metadata_outbox: SegmentBackedMetadataOutbox,
    ) -> Self {
        Self {
            hot_index: HotIndex::new(),
            placement_map,
            mapping,
            metadata_outbox,
            provenance_index: DataProvenanceIndex::new(),
            process_event_journal: None,
        }
    }

    pub fn with_process_event_journal(mut self, journal: ProcessEventJournal) -> Self {
        self.process_event_journal = Some(journal);
        self
    }

    pub fn create(
        placement_map: IptoPlacementMap,
        mapping: IptoMapping,
        outbox_path: impl AsRef<Path>,
        outbox_segment_id: SegmentId,
        node_id: NodeId,
    ) -> Result<Self, MetadataOutboxStorageError> {
        Ok(Self::new(
            placement_map,
            mapping,
            SegmentBackedMetadataOutbox::create(outbox_path, outbox_segment_id, node_id)?,
        ))
    }

    pub fn recover_after(
        placement_map: IptoPlacementMap,
        mapping: IptoMapping,
        outbox_path: impl AsRef<Path>,
        outbox_segment_id: SegmentId,
        node_id: NodeId,
        checkpoint_offset: Option<RecordOffset>,
    ) -> Result<VannakServiceRecovery, MetadataOutboxStorageError> {
        let recovery = SegmentBackedMetadataOutbox::recover_after(
            outbox_path,
            outbox_segment_id,
            node_id,
            checkpoint_offset,
        )?;
        Ok(VannakServiceRecovery {
            service: Self::new(placement_map, mapping, recovery.outbox),
            summary: recovery.summary,
        })
    }

    pub fn ingest_process_event(
        &mut self,
        event: PipelineEvent,
    ) -> Result<IngestOutcome, IngestError> {
        self.hot_index.ingest(event)
    }

    pub fn ingest_process_event_durable(
        &mut self,
        event: PipelineEvent,
    ) -> Result<DurableProcessIngestResult, VannakServiceError> {
        let offset = self
            .process_event_journal
            .as_mut()
            .ok_or(VannakServiceError::ProcessEventJournalNotConfigured)?
            .append_durable(&event)?;
        let outcome = self.hot_index.ingest(event)?;
        Ok(DurableProcessIngestResult { outcome, offset })
    }

    pub fn recover_process_events_into_hot_index(
        &mut self,
        path: impl AsRef<Path>,
    ) -> Result<ProcessEventReplaySummary, VannakServiceError> {
        let replay = crate::ingest::replay_process_event_segment(path)?;
        for event in &replay.events {
            self.hot_index.ingest(event.event.clone())?;
        }
        Ok(replay.summary)
    }

    pub fn capture_metadata_event(
        &mut self,
        event: &DataIndividualMetadataEvent,
    ) -> Result<MetadataCaptureResult, VannakServiceError> {
        let target = self
            .placement_map
            .resolve(event.data_individual_shard_id())
            .cloned()
            .ok_or(VannakServiceError::NoPlacementTarget {
                shard_id: event.data_individual_shard_id(),
            })?;
        let payload = IptoWritePayload::from_event(event, &target, &self.mapping);
        let result = self.metadata_outbox.enqueue_durable(payload)?;
        let _ = self.provenance_index.ingest(event.clone());
        Ok(MetadataCaptureResult { target, result })
    }

    pub fn drain_metadata_for_target(
        &mut self,
        target: &IptoInstanceId,
        writer: &mut (impl IptoWriter + ?Sized),
        max_attempts: usize,
    ) -> MetadataOutboxDrainSummary {
        drain_pending_outbox_for_target(
            self.metadata_outbox.outbox_mut(),
            target,
            writer,
            max_attempts,
        )
    }

    pub fn drain_metadata_for_target_if_lease_held(
        &mut self,
        control_state: &ClusterControlState,
        node_id: &NodeId,
        target: &IptoInstanceId,
        writer: &mut (impl IptoWriter + ?Sized),
        max_attempts: usize,
    ) -> Result<MetadataOutboxDrainSummary, VannakServiceError> {
        let Some(lease) = control_state.writer_lease(target) else {
            return Err(VannakServiceError::WriterLeaseNotHeld {
                target: target.clone(),
                holder: node_id.clone(),
            });
        };
        if &lease.holder != node_id {
            return Err(VannakServiceError::WriterLeaseNotHeld {
                target: target.clone(),
                holder: node_id.clone(),
            });
        }
        Ok(self.drain_metadata_for_target(target, writer, max_attempts))
    }

    pub fn acknowledged_checkpoint(
        &self,
        shard_id: DataIndividualShardId,
        target: &IptoInstanceId,
        epoch: CheckpointEpoch,
    ) -> Option<MetadataOutboxCheckpoint> {
        self.metadata_outbox
            .acknowledged_checkpoint(shard_id, target, epoch)
    }

    pub fn snapshot(&self) -> VannakServiceSnapshot {
        VannakServiceSnapshot {
            hot_index: self.hot_index.snapshot(),
            metadata_outbox: self.metadata_outbox.snapshot(),
            provenance_index: self.provenance_index.snapshot(),
            process_event_journal: self
                .process_event_journal
                .as_ref()
                .map(ProcessEventJournal::manifest),
            placement_epoch: self.placement_map.epoch,
        }
    }

    pub fn replay_metadata_outbox_after(
        path: impl AsRef<Path>,
        checkpoint_offset: Option<RecordOffset>,
    ) -> Result<crate::ipto::MetadataOutboxReplay, MetadataOutboxStorageError> {
        replay_metadata_outbox_segment_after(path, checkpoint_offset)
    }

    pub fn hot_index(&self) -> &HotIndex {
        &self.hot_index
    }

    pub fn process_instance(
        &self,
        query: &ProcessInstanceQuery,
    ) -> Option<crate::process::ProcessInstanceSnapshot> {
        self.hot_index.process_instance(query)
    }

    pub fn pipeline_instances(&self, query: &PipelineQuery) -> QueryResult {
        self.hot_index.pipeline_instances(query)
    }

    pub fn events(&self, query: &EventQuery) -> QueryResult {
        self.hot_index.events(query)
    }

    pub fn impact(&self, query: &ImpactQuery) -> QueryResult {
        self.hot_index.impact(query)
    }

    pub fn process_instances_by_status(&self, query: &ProcessStatusQuery) -> QueryResult {
        self.hot_index.process_instances_by_status(query)
    }

    pub fn events_in_time_range(&self, query: &TimeRangeQuery) -> QueryResult {
        self.hot_index.events_in_time_range(query)
    }

    pub fn metadata_for_data_individual(&self, query: &DataIndividualMetadataQuery) -> QueryResult {
        self.provenance_index.metadata_for_data_individual(query)
    }

    pub fn metadata_for_process(&self, query: &ProcessMetadataQuery) -> QueryResult {
        self.provenance_index.metadata_for_process(query)
    }

    pub fn metadata_for_activity(&self, query: &ActivityMetadataQuery) -> QueryResult {
        self.provenance_index.metadata_for_activity(query)
    }

    pub fn data_individuals_for_process(
        &self,
        process_instance_id: &crate::process::ProcessInstanceId,
    ) -> Vec<DataIndividualId> {
        self.provenance_index
            .data_individuals_for_process(process_instance_id)
    }

    pub fn data_individuals_for_activity(
        &self,
        process_instance_id: &crate::process::ProcessInstanceId,
        activity_id: &crate::process::ActivityId,
    ) -> Vec<DataIndividualId> {
        self.provenance_index
            .data_individuals_for_activity(process_instance_id, activity_id)
    }

    pub fn metadata_outbox(&self) -> &SegmentBackedMetadataOutbox {
        &self.metadata_outbox
    }

    pub fn metadata_outbox_mut(&mut self) -> &mut SegmentBackedMetadataOutbox {
        &mut self.metadata_outbox
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableProcessIngestResult {
    pub outcome: IngestOutcome,
    pub offset: RecordOffset,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataCaptureResult {
    pub target: IptoInstanceId,
    pub result: DurableOutboxEnqueueResult,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VannakServiceSnapshot {
    pub hot_index: HotIndexSnapshot,
    pub metadata_outbox: SegmentBackedMetadataOutboxSnapshot,
    pub provenance_index: DataProvenanceIndexSnapshot,
    pub process_event_journal: Option<SegmentManifest>,
    pub placement_epoch: crate::cluster::PlacementEpoch,
}

#[derive(Debug)]
pub struct VannakServiceRecovery {
    pub service: VannakService,
    pub summary: MetadataOutboxReplaySummary,
}

#[derive(Debug)]
pub enum VannakServiceError {
    NoPlacementTarget {
        shard_id: DataIndividualShardId,
    },
    WriterLeaseNotHeld {
        target: IptoInstanceId,
        holder: NodeId,
    },
    ProcessEventJournalNotConfigured,
    ProcessEventJournal(ProcessEventJournalError),
    MetadataOutboxStorage(MetadataOutboxStorageError),
    ProcessIngest(IngestError),
}

impl fmt::Display for VannakServiceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoPlacementTarget { shard_id } => {
                write!(f, "no Ipto placement target for shard {}", shard_id.0)
            }
            Self::WriterLeaseNotHeld { target, holder } => write!(
                f,
                "node '{}' does not hold writer lease for Ipto target '{}'",
                holder.as_str(),
                target.as_str()
            ),
            Self::ProcessEventJournalNotConfigured => {
                f.write_str("process event journal is not configured")
            }
            Self::ProcessEventJournal(error) => write!(f, "{error}"),
            Self::MetadataOutboxStorage(error) => write!(f, "{error}"),
            Self::ProcessIngest(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for VannakServiceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::MetadataOutboxStorage(error) => Some(error),
            Self::ProcessEventJournal(error) => Some(error),
            Self::ProcessIngest(error) => Some(error),
            _ => None,
        }
    }
}

impl From<MetadataOutboxStorageError> for VannakServiceError {
    fn from(value: MetadataOutboxStorageError) -> Self {
        Self::MetadataOutboxStorage(value)
    }
}

impl From<ProcessEventJournalError> for VannakServiceError {
    fn from(value: ProcessEventJournalError) -> Self {
        Self::ProcessEventJournal(value)
    }
}

impl From<IngestError> for VannakServiceError {
    fn from(value: IngestError) -> Self {
        Self::ProcessIngest(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::{
        ClusterControlCommand, IptoPlacementSlot, LeaseEpoch, PlacementEpoch, WriterLease,
    };
    use crate::data::{
        ActiveMetadata, DataIndividualId, MetadataEventId, MetadataOperation, MetadataValue,
        PassiveMetadata,
    };
    use crate::ingest::{EventId, EventTimestamp, PipelineEvent, SourceId, SourceSequence};
    use crate::ipto::{
        IptoAttributeName, IptoWriteError, MetadataOutboxDeliveryResult, OutboxStatus,
    };
    use crate::process::{
        ActivityId, EnvironmentId, EventKind, PipelineId, ProcessDefinitionId, ProcessInstanceId,
        ProcessStatus, TenantId,
    };
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn service_captures_metadata_durably_and_drains_with_lease() {
        let path = temp_segment_path("service-capture");
        let target = IptoInstanceId::from("ipto-a");
        let mut service = service(&path, target.clone());
        let event = metadata_event("data-1", "meta-1")
            .with_passive_metadata(
                PassiveMetadata::new()
                    .insert("vannak:dataIndividualId", MetadataValue::string("data-1")),
            )
            .with_active_metadata(
                ActiveMetadata::new().insert("vannak:activityId", MetadataValue::string("extract")),
            );

        let capture = service.capture_metadata_event(&event).unwrap();

        assert_eq!(capture.target, target);
        assert!(matches!(
            capture.result,
            DurableOutboxEnqueueResult::Enqueued { .. }
        ));
        assert_eq!(service.snapshot().metadata_outbox.outbox.pending, 1);
        assert_eq!(service.snapshot().provenance_index.metadata_event_count, 1);

        let mut control = ClusterControlState::new();
        control
            .apply(ClusterControlCommand::AddNode(NodeId::from("node-a")))
            .unwrap();
        control
            .apply(ClusterControlCommand::GrantWriterLease(WriterLease {
                target: target.clone(),
                holder: NodeId::from("node-a"),
                epoch: LeaseEpoch(1),
            }))
            .unwrap();

        let mut writer = RecordingWriter::default();
        let summary = service
            .drain_metadata_for_target_if_lease_held(
                &control,
                &NodeId::from("node-a"),
                &target,
                &mut writer,
                10,
            )
            .unwrap();

        assert_eq!(summary.acknowledged, 1);
        assert_eq!(writer.writes.len(), 1);
        assert_eq!(service.snapshot().metadata_outbox.outbox.acknowledged, 1);

        let checkpoint = service
            .acknowledged_checkpoint(
                event.data_individual_shard_id(),
                &target,
                CheckpointEpoch(1),
            )
            .unwrap();
        let replay = VannakService::replay_metadata_outbox_after(
            &path,
            Some(checkpoint.last_acknowledged_offset),
        )
        .unwrap();
        assert_eq!(replay.summary.scanned_records, 1);
        assert_eq!(replay.summary.skipped_records, 1);
        assert!(replay.outbox.next_pending().is_none());

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn service_indexes_metadata_capture_for_local_provenance_queries() {
        let path = temp_segment_path("service-provenance");
        let target = IptoInstanceId::from("ipto-a");
        let mut service = service(&path, target);
        let first =
            metadata_event("data-1", "meta-1").with_activity_id(ActivityId::from("extract"));
        let second = metadata_event("data-1", "meta-2").with_activity_id(ActivityId::from("load"));
        let third =
            metadata_event("data-2", "meta-3").with_activity_id(ActivityId::from("extract"));

        service.capture_metadata_event(&first).unwrap();
        service.capture_metadata_event(&second).unwrap();
        service.capture_metadata_event(&third).unwrap();

        let QueryResult::MetadataEvents(data_events) =
            service.metadata_for_data_individual(&DataIndividualMetadataQuery {
                data_individual_id: DataIndividualId::from("data-1"),
                limit: crate::query::QueryLimit::new(10),
            })
        else {
            panic!("data query returns metadata events");
        };
        assert_eq!(data_events, vec![first.clone(), second]);

        let QueryResult::MetadataEvents(activity_events) =
            service.metadata_for_activity(&ActivityMetadataQuery {
                process_instance_id: ProcessInstanceId::from("instance-a"),
                activity_id: ActivityId::from("extract"),
                limit: crate::query::QueryLimit::new(10),
            })
        else {
            panic!("activity query returns metadata events");
        };
        assert_eq!(activity_events, vec![first, third]);

        assert_eq!(
            service.data_individuals_for_process(&ProcessInstanceId::from("instance-a")),
            vec![
                DataIndividualId::from("data-1"),
                DataIndividualId::from("data-2")
            ]
        );

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn service_recovers_outbox_after_checkpoint_and_continues_capture() {
        let path = temp_segment_path("service-recover");
        let target = IptoInstanceId::from("ipto-a");
        let first = metadata_event("data-1", "meta-1").with_passive_metadata(
            PassiveMetadata::new()
                .insert("vannak:dataIndividualId", MetadataValue::string("data-1")),
        );
        let second = metadata_event("data-2", "meta-2").with_passive_metadata(
            PassiveMetadata::new()
                .insert("vannak:dataIndividualId", MetadataValue::string("data-2")),
        );

        let checkpoint_offset = {
            let mut service = service(&path, target.clone());
            service.capture_metadata_event(&first).unwrap();
            service.capture_metadata_event(&second).unwrap();
            let mut writer = RecordingWriter::default();
            let summary = service.drain_metadata_for_target(&target, &mut writer, 1);
            assert_eq!(summary.acknowledged, 1);
            service
                .acknowledged_checkpoint(
                    first.data_individual_shard_id(),
                    &target,
                    CheckpointEpoch(1),
                )
                .unwrap()
                .last_acknowledged_offset
        };

        let recovery = VannakService::recover_after(
            placement(target.clone()),
            mapping(),
            &path,
            SegmentId::from("outbox-segment-a"),
            NodeId::from("node-a"),
            Some(checkpoint_offset),
        )
        .unwrap();

        assert_eq!(recovery.summary.scanned_records, 2);
        assert_eq!(recovery.summary.skipped_records, 1);
        assert_eq!(recovery.summary.replayed_records, 1);
        let mut service = recovery.service;
        assert_eq!(service.snapshot().metadata_outbox.outbox.pending, 1);

        let third = metadata_event("data-3", "meta-3").with_passive_metadata(
            PassiveMetadata::new()
                .insert("vannak:dataIndividualId", MetadataValue::string("data-3")),
        );
        service.capture_metadata_event(&third).unwrap();
        assert_eq!(service.snapshot().metadata_outbox.segment.record_count, 3);
        assert_eq!(service.snapshot().metadata_outbox.outbox.pending, 2);

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn service_refuses_to_drain_without_target_lease() {
        let path = temp_segment_path("service-lease");
        let target = IptoInstanceId::from("ipto-a");
        let mut service = service(&path, target.clone());
        service
            .capture_metadata_event(&metadata_event("data-1", "meta-1"))
            .unwrap();

        let mut control = ClusterControlState::new();
        control
            .apply(ClusterControlCommand::AddNode(NodeId::from("node-b")))
            .unwrap();
        control
            .apply(ClusterControlCommand::GrantWriterLease(WriterLease {
                target: target.clone(),
                holder: NodeId::from("node-b"),
                epoch: LeaseEpoch(1),
            }))
            .unwrap();

        let mut writer = RecordingWriter::default();
        let error = service
            .drain_metadata_for_target_if_lease_held(
                &control,
                &NodeId::from("node-a"),
                &target,
                &mut writer,
                10,
            )
            .unwrap_err();

        assert!(matches!(
            error,
            VannakServiceError::WriterLeaseNotHeld { .. }
        ));
        assert!(writer.writes.is_empty());
        assert_eq!(service.snapshot().metadata_outbox.outbox.pending, 1);

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn service_ingests_process_events_into_hot_index() {
        let path = temp_segment_path("service-process");
        let mut service = service(&path, IptoInstanceId::from("ipto-a"));
        let event = PipelineEvent::new(
            EventId::from("event-1"),
            SourceId::from("durga"),
            SourceSequence(1),
            TenantId::from("tenant-a"),
            EnvironmentId::from("prod"),
            PipelineId::from("pipeline-a"),
            ProcessDefinitionId::from("definition-a"),
            ProcessInstanceId::from("instance-a"),
            EventTimestamp::from("2026-06-30T10:00:00Z"),
            EventKind::ProcessStarted,
        );

        assert_eq!(
            service.ingest_process_event(event).unwrap(),
            IngestOutcome::Accepted
        );
        assert_eq!(service.snapshot().hot_index.event_count, 1);

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn service_durably_ingests_process_events_before_indexing() {
        let outbox_path = temp_segment_path("service-durable-process-outbox");
        let process_path = temp_segment_path("service-durable-process");
        let process_journal = ProcessEventJournal::create(
            &process_path,
            SegmentId::from("process-segment-a"),
            NodeId::from("node-a"),
        )
        .unwrap();
        let mut service = service(&outbox_path, IptoInstanceId::from("ipto-a"))
            .with_process_event_journal(process_journal);
        let event = process_event("event-1", "instance-a", EventKind::ProcessStarted);

        let result = service.ingest_process_event_durable(event.clone()).unwrap();

        assert_eq!(result.outcome, IngestOutcome::Accepted);
        assert_eq!(service.snapshot().hot_index.event_count, 1);
        assert_eq!(
            service
                .snapshot()
                .process_event_journal
                .as_ref()
                .unwrap()
                .record_count,
            1
        );

        let replay = crate::ingest::replay_process_event_segment(&process_path).unwrap();
        assert_eq!(replay.summary.replayed_records, 1);
        assert_eq!(replay.events[0].event, event);

        fs::remove_file(outbox_path).unwrap();
        fs::remove_file(process_path).unwrap();
    }

    #[test]
    fn service_replays_process_segment_into_hot_index() {
        let outbox_path = temp_segment_path("service-replay-process-outbox");
        let process_path = temp_segment_path("service-replay-process");
        let mut journal = ProcessEventJournal::create(
            &process_path,
            SegmentId::from("process-segment-a"),
            NodeId::from("node-a"),
        )
        .unwrap();
        journal
            .append_durable(&process_event(
                "event-1",
                "instance-a",
                EventKind::ProcessStarted,
            ))
            .unwrap();
        journal
            .append_durable(&process_event(
                "event-2",
                "instance-a",
                EventKind::ProcessCompleted,
            ))
            .unwrap();
        drop(journal);

        let mut service = service(&outbox_path, IptoInstanceId::from("ipto-a"));
        let summary = service
            .recover_process_events_into_hot_index(&process_path)
            .unwrap();

        assert_eq!(summary.replayed_records, 2);
        assert_eq!(service.snapshot().hot_index.event_count, 2);
        assert!(
            service
                .hot_index()
                .process_instance(&crate::query::ProcessInstanceQuery {
                    process_instance_id: ProcessInstanceId::from("instance-a"),
                })
                .is_some()
        );

        fs::remove_file(outbox_path).unwrap();
        fs::remove_file(process_path).unwrap();
    }

    #[test]
    fn service_exposes_typed_query_wrappers() {
        let path = temp_segment_path("service-query");
        let mut service = service(&path, IptoInstanceId::from("ipto-a"));
        service
            .ingest_process_event(process_event(
                "event-1",
                "instance-a",
                EventKind::ProcessStarted,
            ))
            .unwrap();
        service
            .ingest_process_event(process_event(
                "event-2",
                "instance-a",
                EventKind::ProcessFailed,
            ))
            .unwrap();

        let QueryResult::ProcessInstances(failed) =
            service.process_instances_by_status(&ProcessStatusQuery {
                status: ProcessStatus::Failed,
                limit: crate::query::QueryLimit::new(10),
            })
        else {
            panic!("status query returns process instances");
        };
        assert_eq!(failed.len(), 1);

        let QueryResult::Events(events) = service.events_in_time_range(&TimeRangeQuery {
            start: Some(EventTimestamp::from("2026-06-30T10:00:00Z")),
            end: Some(EventTimestamp::from("2026-06-30T10:00:00Z")),
            limit: crate::query::QueryLimit::new(10),
        }) else {
            panic!("time range query returns events");
        };
        assert_eq!(events.len(), 2);

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn target_aware_drain_leaves_other_targets_pending() {
        let path = temp_segment_path("service-target-drain");
        let target_a = IptoInstanceId::from("ipto-a");
        let target_b = IptoInstanceId::from("ipto-b");
        let placement = IptoPlacementMap::new(
            PlacementEpoch(1),
            vec![
                IptoPlacementSlot::new(target_a.clone(), 1).unwrap(),
                IptoPlacementSlot::new(target_b.clone(), 1).unwrap(),
            ],
            vec![],
        )
        .unwrap();
        let mapping =
            IptoMapping::new("v1").map_field("vannak:dataIndividualId", "vannak:dataIndividualId");
        let outbox = SegmentBackedMetadataOutbox::create(
            &path,
            SegmentId::from("segment-a"),
            NodeId::from("node-a"),
        )
        .unwrap();
        let mut service = VannakService::new(placement, mapping, outbox);

        let payload_a = IptoWritePayload {
            target: target_a.clone(),
            shard_id: DataIndividualShardId(1),
            idempotency_key: crate::data::IdempotencyKey::from("a"),
            mapping_version: "v1".to_string(),
            attributes: [(
                IptoAttributeName::from("vannak:dataIndividualId"),
                MetadataValue::string("a"),
            )]
            .into_iter()
            .collect(),
        };
        let payload_b = IptoWritePayload {
            target: target_b.clone(),
            shard_id: DataIndividualShardId(2),
            idempotency_key: crate::data::IdempotencyKey::from("b"),
            mapping_version: "v1".to_string(),
            attributes: [(
                IptoAttributeName::from("vannak:dataIndividualId"),
                MetadataValue::string("b"),
            )]
            .into_iter()
            .collect(),
        };
        service
            .metadata_outbox_mut()
            .enqueue_durable(payload_a)
            .unwrap();
        service
            .metadata_outbox_mut()
            .enqueue_durable(payload_b)
            .unwrap();

        let mut writer = RecordingWriter::default();
        let summary = service.drain_metadata_for_target(&target_b, &mut writer, 10);

        assert_eq!(summary.acknowledged, 1);
        assert_eq!(writer.writes, vec![target_b]);
        assert_eq!(service.snapshot().metadata_outbox.outbox.pending, 1);
        assert_eq!(
            service
                .metadata_outbox()
                .outbox()
                .next_pending_for_target(&target_a)
                .unwrap()
                .status(),
            OutboxStatus::Pending
        );

        fs::remove_file(path).unwrap();
    }

    fn service(path: &std::path::Path, target: IptoInstanceId) -> VannakService {
        VannakService::create(
            placement(target),
            mapping(),
            path,
            SegmentId::from("outbox-segment-a"),
            NodeId::from("node-a"),
        )
        .unwrap()
    }

    fn placement(target: IptoInstanceId) -> IptoPlacementMap {
        IptoPlacementMap::new(
            PlacementEpoch(1),
            vec![IptoPlacementSlot::new(target, 1).unwrap()],
            vec![],
        )
        .unwrap()
    }

    fn mapping() -> IptoMapping {
        IptoMapping::new("v1")
            .map_field("vannak:dataIndividualId", "vannak:dataIndividualId")
            .map_field("vannak:activityId", "vannak:activityId")
    }

    fn metadata_event(data_id: &str, metadata_event_id: &str) -> DataIndividualMetadataEvent {
        let data_id = DataIndividualId::from(data_id);
        let shard_id = DataIndividualShardId::from_data_individual(&data_id);
        DataIndividualMetadataEvent::new(
            MetadataEventId::from(metadata_event_id),
            data_id,
            shard_id,
            TenantId::from("tenant-a"),
            EnvironmentId::from("prod"),
            PipelineId::from("pipeline-a"),
            ProcessInstanceId::from("instance-a"),
            EventTimestamp::from("2026-06-30T10:00:00Z"),
            MetadataOperation::Received,
        )
        .with_activity_id(ActivityId::from("extract"))
    }

    fn process_event(event_id: &str, process_instance_id: &str, kind: EventKind) -> PipelineEvent {
        PipelineEvent::new(
            EventId::from(event_id),
            SourceId::from("durga"),
            SourceSequence(1),
            TenantId::from("tenant-a"),
            EnvironmentId::from("prod"),
            PipelineId::from("pipeline-a"),
            ProcessDefinitionId::from("definition-a"),
            ProcessInstanceId::from(process_instance_id),
            EventTimestamp::from("2026-06-30T10:00:00Z"),
            kind,
        )
    }

    #[derive(Default)]
    struct RecordingWriter {
        writes: Vec<IptoInstanceId>,
    }

    impl IptoWriter for RecordingWriter {
        fn write(&mut self, payload: &IptoWritePayload) -> Result<(), IptoWriteError> {
            self.writes.push(payload.target.clone());
            Ok(())
        }
    }

    #[allow(dead_code)]
    fn _assert_delivery_result_send(_: MetadataOutboxDeliveryResult) {}

    fn temp_segment_path(name: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("vannak-{name}-{nanos}.seg"))
    }
}
