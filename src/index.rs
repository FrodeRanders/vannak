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

use crate::ingest::{EventId, EventTimestamp, IngestError, PipelineEvent};
use crate::metadata::MetadataRef;
use crate::observability::HotIndexSnapshot;
use crate::process::{
    PipelineId, ProcessInstanceId, ProcessInstanceSnapshot, ProcessInstanceState,
};
use crate::query::{
    EventQuery, ImpactQuery, PipelineQuery, ProcessInstanceQuery, ProcessStatusQuery, QueryResult,
    TimeRangeQuery,
};
use std::collections::{BTreeMap, BTreeSet};

/// Dependency-free single-node hot index.
///
/// This is intentionally not concurrent. The expected Sitas integration is one
/// `HotIndex` inside shard-local state per owning shard.
#[derive(Debug, Default)]
pub struct HotIndex {
    process_instances: BTreeMap<ProcessInstanceId, ProcessInstanceState>,
    events_by_id: BTreeMap<EventId, PipelineEvent>,
    events_by_process: BTreeMap<ProcessInstanceId, Vec<EventId>>,
    events_by_pipeline: BTreeMap<PipelineId, Vec<EventId>>,
    events_by_metadata_ref: BTreeMap<MetadataRef, Vec<EventId>>,
    events_by_time: BTreeMap<EventTimestamp, Vec<EventId>>,
    duplicate_events: u64,
    rejected_events: u64,
}

impl HotIndex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn ingest(&mut self, event: PipelineEvent) -> Result<IngestOutcome, IngestError> {
        if let Err(error) = event.validate() {
            self.rejected_events += 1;
            return Err(error);
        }

        if self.events_by_id.contains_key(event.event_id()) {
            self.duplicate_events += 1;
            return Ok(IngestOutcome::Duplicate);
        }

        let event_id = event.event_id().clone();
        let process_id = event.process_instance_id().clone();
        let pipeline_id = event.pipeline_id().clone();
        let timestamp = event.timestamp().clone();
        let metadata_refs = event.metadata_refs().to_vec();

        self.process_instances
            .entry(process_id.clone())
            .or_insert_with(|| ProcessInstanceState::new(&event))
            .apply(&event);

        self.events_by_process
            .entry(process_id)
            .or_default()
            .push(event_id.clone());
        self.events_by_pipeline
            .entry(pipeline_id)
            .or_default()
            .push(event_id.clone());
        self.events_by_time
            .entry(timestamp)
            .or_default()
            .push(event_id.clone());
        for metadata_ref in metadata_refs {
            self.events_by_metadata_ref
                .entry(metadata_ref)
                .or_default()
                .push(event_id.clone());
        }

        self.events_by_id.insert(event_id, event);
        Ok(IngestOutcome::Accepted)
    }

    pub fn process_instance(
        &self,
        query: &ProcessInstanceQuery,
    ) -> Option<ProcessInstanceSnapshot> {
        self.process_instances
            .get(&query.process_instance_id)
            .map(ProcessInstanceState::snapshot)
    }

    pub fn pipeline_instances(&self, query: &PipelineQuery) -> QueryResult {
        let mut instances = Vec::new();
        for state in self.process_instances.values() {
            let snapshot = state.snapshot();
            if snapshot.pipeline_id == query.pipeline_id {
                instances.push(snapshot);
            }
            if query.limit.reached(instances.len()) {
                break;
            }
        }
        QueryResult::ProcessInstances(instances)
    }

    pub fn events(&self, query: &EventQuery) -> QueryResult {
        let event_ids = self
            .events_by_process
            .get(&query.process_instance_id)
            .into_iter()
            .flatten()
            .take(query.limit.value())
            .cloned()
            .collect::<Vec<_>>();
        let events = self.events_for_ids(&event_ids);
        QueryResult::Events(events)
    }

    pub fn impact(&self, query: &ImpactQuery) -> QueryResult {
        let event_ids = self
            .events_by_metadata_ref
            .get(&query.metadata_ref)
            .into_iter()
            .flatten()
            .take(query.limit.value())
            .cloned()
            .collect::<Vec<_>>();
        let events = self.events_for_ids(&event_ids);
        QueryResult::Events(events)
    }

    pub fn process_instances_by_status(&self, query: &ProcessStatusQuery) -> QueryResult {
        let mut instances = Vec::new();
        for state in self.process_instances.values() {
            let snapshot = state.snapshot();
            if snapshot.status == query.status {
                instances.push(snapshot);
            }
            if query.limit.reached(instances.len()) {
                break;
            }
        }
        QueryResult::ProcessInstances(instances)
    }

    pub fn events_in_time_range(&self, query: &TimeRangeQuery) -> QueryResult {
        let mut event_ids = Vec::new();
        for (timestamp, ids) in &self.events_by_time {
            if !query.contains(timestamp) {
                continue;
            }
            for event_id in ids {
                event_ids.push(event_id.clone());
                if query.limit.reached(event_ids.len()) {
                    return QueryResult::Events(self.events_for_ids(&event_ids));
                }
            }
        }
        QueryResult::Events(self.events_for_ids(&event_ids))
    }

    pub fn affected_pipelines(&self, metadata_ref: &MetadataRef) -> Vec<PipelineId> {
        let mut pipelines = BTreeSet::new();
        if let Some(event_ids) = self.events_by_metadata_ref.get(metadata_ref) {
            for event_id in event_ids {
                if let Some(event) = self.events_by_id.get(event_id) {
                    pipelines.insert(event.pipeline_id().clone());
                }
            }
        }
        pipelines.into_iter().collect()
    }

    pub fn snapshot(&self) -> HotIndexSnapshot {
        HotIndexSnapshot {
            process_instance_count: self.process_instances.len(),
            event_count: self.events_by_id.len(),
            pipeline_count: self.events_by_pipeline.len(),
            metadata_ref_count: self.events_by_metadata_ref.len(),
            duplicate_events: self.duplicate_events,
            rejected_events: self.rejected_events,
        }
    }

    pub fn contains_event(&self, event_id: &EventId) -> bool {
        self.events_by_id.contains_key(event_id)
    }

    fn events_for_ids(&self, event_ids: &[EventId]) -> Vec<PipelineEvent> {
        event_ids
            .iter()
            .filter_map(|event_id| self.events_by_id.get(event_id).cloned())
            .collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestOutcome {
    Accepted,
    Duplicate,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::durga::{DurgaEventType, DurgaProcessEvent, DurgaStatus};
    use crate::ingest::{EventId, EventTimestamp, SourceId, SourceSequence};
    use crate::metadata::{DatasetId, MetadataRef};
    use crate::process::{
        ActivityId, EnvironmentId, EventKind, PipelineId, ProcessDefinitionId, ProcessInstanceId,
        ProcessStatus, TenantId,
    };
    use crate::query::{ImpactQuery, ProcessStatusQuery, QueryLimit, TimeRangeQuery};

    #[test]
    fn ingest_reduces_process_state_and_indexes_metadata() {
        let dataset = MetadataRef::Dataset(DatasetId::from("dataset-a"));
        let mut index = HotIndex::new();

        let started =
            event("e1", EventKind::ProcessStarted).with_metadata_refs(vec![dataset.clone()]);
        let activity = event("e2", EventKind::ActivityEntered)
            .with_activity_id(ActivityId::from("extract"))
            .with_metadata_refs(vec![dataset.clone()]);
        let activity_completed =
            event("e3", EventKind::ActivityCompleted).with_activity_id(ActivityId::from("extract"));
        let completed = event("e4", EventKind::ProcessCompleted);

        assert_eq!(index.ingest(started).unwrap(), IngestOutcome::Accepted);
        assert_eq!(index.ingest(activity).unwrap(), IngestOutcome::Accepted);
        assert_eq!(
            index.ingest(activity_completed).unwrap(),
            IngestOutcome::Accepted
        );
        assert_eq!(index.ingest(completed).unwrap(), IngestOutcome::Accepted);

        let process = index
            .process_instance(&ProcessInstanceQuery {
                process_instance_id: ProcessInstanceId::from("instance-a"),
            })
            .unwrap();
        assert_eq!(process.status, ProcessStatus::Completed);
        assert_eq!(
            process.activity_durations.get(&ActivityId::from("extract")),
            Some(&1000)
        );
        assert!(process.metadata_refs.contains(&dataset));

        let result = index.impact(&ImpactQuery {
            metadata_ref: dataset.clone(),
            limit: QueryLimit::new(10),
        });
        let QueryResult::Events(events) = result else {
            panic!("impact query returns events");
        };
        assert_eq!(events.len(), 2);
        assert_eq!(
            index.affected_pipelines(&dataset),
            vec![PipelineId::from("pipeline-a")]
        );
    }

    #[test]
    fn duplicate_event_is_idempotent() {
        let mut index = HotIndex::new();
        let event = event("e1", EventKind::ProcessStarted);

        assert_eq!(
            index.ingest(event.clone()).unwrap(),
            IngestOutcome::Accepted
        );
        assert_eq!(index.ingest(event).unwrap(), IngestOutcome::Duplicate);
        assert_eq!(index.snapshot().event_count, 1);
        assert_eq!(index.snapshot().duplicate_events, 1);
    }

    #[test]
    fn rejected_event_is_counted() {
        let mut index = HotIndex::new();
        let invalid = event("", EventKind::ProcessStarted);

        assert!(index.ingest(invalid).is_err());
        assert_eq!(index.snapshot().event_count, 0);
        assert_eq!(index.snapshot().rejected_events, 1);
    }

    #[test]
    fn queries_process_instances_by_current_status() {
        let mut index = HotIndex::new();
        index
            .ingest(event_for_instance(
                "e1",
                "instance-a",
                EventKind::ProcessStarted,
            ))
            .unwrap();
        index
            .ingest(event_for_instance(
                "e2",
                "instance-b",
                EventKind::ProcessStarted,
            ))
            .unwrap();
        index
            .ingest(event_for_instance(
                "e3",
                "instance-b",
                EventKind::ProcessFailed,
            ))
            .unwrap();

        let QueryResult::ProcessInstances(failed) =
            index.process_instances_by_status(&ProcessStatusQuery {
                status: ProcessStatus::Failed,
                limit: QueryLimit::new(10),
            })
        else {
            panic!("status query returns process instances");
        };
        assert_eq!(failed.len(), 1);
        assert_eq!(
            failed[0].process_instance_id,
            ProcessInstanceId::from("instance-b")
        );

        let QueryResult::ProcessInstances(active) =
            index.process_instances_by_status(&ProcessStatusQuery {
                status: ProcessStatus::Active,
                limit: QueryLimit::new(1),
            })
        else {
            panic!("status query returns process instances");
        };
        assert_eq!(active.len(), 1);
    }

    #[test]
    fn queries_events_by_time_range() {
        let mut index = HotIndex::new();
        index
            .ingest(event_at(
                "e1",
                "instance-a",
                "2026-06-30T10:00:00Z",
                EventKind::ProcessStarted,
            ))
            .unwrap();
        index
            .ingest(event_at(
                "e2",
                "instance-a",
                "2026-06-30T10:00:02Z",
                EventKind::ActivityEntered,
            ))
            .unwrap();
        index
            .ingest(event_at(
                "e3",
                "instance-b",
                "2026-06-30T10:00:03Z",
                EventKind::ProcessStarted,
            ))
            .unwrap();

        let QueryResult::Events(events) = index.events_in_time_range(&TimeRangeQuery {
            start: Some(EventTimestamp::from("2026-06-30T10:00:01Z")),
            end: Some(EventTimestamp::from("2026-06-30T10:00:03Z")),
            limit: QueryLimit::new(1),
        }) else {
            panic!("time range query returns events");
        };

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_id(), &EventId::from("e2"));
    }

    #[test]
    fn late_activity_event_does_not_reopen_terminal_process() {
        let mut index = HotIndex::new();
        index
            .ingest(event_at(
                "e1",
                "instance-a",
                "2026-06-30T10:00:00Z",
                EventKind::ProcessStarted,
            ))
            .unwrap();
        index
            .ingest(event_at(
                "e2",
                "instance-a",
                "2026-06-30T10:00:05Z",
                EventKind::ProcessCompleted,
            ))
            .unwrap();
        index
            .ingest(
                event_at(
                    "e3",
                    "instance-a",
                    "2026-06-30T10:00:03Z",
                    EventKind::ActivityEntered,
                )
                .with_activity_id(ActivityId::from("extract")),
            )
            .unwrap();

        let process = index
            .process_instance(&ProcessInstanceQuery {
                process_instance_id: ProcessInstanceId::from("instance-a"),
            })
            .unwrap();
        assert_eq!(process.status, ProcessStatus::Completed);
        assert_eq!(
            process.completed_at,
            Some(EventTimestamp::from("2026-06-30T10:00:05Z"))
        );
        assert!(
            !process
                .activities
                .contains_key(&ActivityId::from("extract"))
        );
    }

    #[test]
    fn durga_process_event_converts_to_pipeline_event() {
        let mut index = HotIndex::new();
        let durga_event = DurgaProcessEvent {
            process_instance_id: String::from("instance-a"),
            process_id: String::from("pipeline-a"),
            activity_id: Some(String::from("extract")),
            token_id: Some(String::from("token-1")),
            correlation_id: Some(String::from("corr-1")),
            payload: Some(String::from("{\"ok\":true}")),
            status: DurgaStatus::Started,
            error: None,
            event_type: DurgaEventType::ActivityEntered,
            process_version: Some(String::from("v1")),
            business_key: Some(String::from("order-1")),
            timestamp: String::from("2026-06-30T10:00:00Z"),
            metadata_refs: Vec::new(),
            schema_version: None,
        };

        let event = durga_event.into_pipeline_event(
            SourceId::from("durga-monitor"),
            SourceSequence(7),
            TenantId::from("tenant-a"),
            EnvironmentId::from("prod"),
        );

        assert_eq!(event.activity_id(), Some(&ActivityId::from("extract")));
        assert_eq!(event.kind(), EventKind::ActivityEntered);
        assert_eq!(index.ingest(event).unwrap(), IngestOutcome::Accepted);
        let process = index
            .process_instance(&ProcessInstanceQuery {
                process_instance_id: ProcessInstanceId::from("instance-a"),
            })
            .unwrap();
        assert_eq!(
            process.current_activity_id,
            Some(ActivityId::from("extract"))
        );
        assert_eq!(process.status, ProcessStatus::Active);
    }

    fn event(event_id: &str, kind: EventKind) -> PipelineEvent {
        event_for_instance(event_id, "instance-a", kind)
    }

    fn event_for_instance(event_id: &str, instance_id: &str, kind: EventKind) -> PipelineEvent {
        event_at(
            event_id,
            instance_id,
            match event_id {
                "e1" => "2026-06-30T10:00:00Z",
                "e2" => "2026-06-30T10:00:01Z",
                _ => "2026-06-30T10:00:02Z",
            },
            kind,
        )
    }

    fn event_at(
        event_id: &str,
        instance_id: &str,
        timestamp: &str,
        kind: EventKind,
    ) -> PipelineEvent {
        PipelineEvent::new(
            EventId::from(event_id),
            SourceId::from("durga-a"),
            SourceSequence(1),
            TenantId::from("tenant-a"),
            EnvironmentId::from("prod"),
            PipelineId::from("pipeline-a"),
            ProcessDefinitionId::from("definition-a"),
            ProcessInstanceId::from(instance_id),
            EventTimestamp::from(timestamp),
            kind,
        )
    }
}
