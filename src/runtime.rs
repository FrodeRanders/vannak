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

//! Sitas integration boundary.
//!
//! Core domain code stays dependency-free. This module models the shard-local
//! shape expected from a future Sitas adapter: one hot index per logical shard,
//! deterministic process-instance routing, explicit fanout for cross-shard
//! queries, and owned snapshots.

use crate::index::{HotIndex, IngestOutcome};
use crate::ingest::{IngestError, PipelineEvent};
use crate::metadata::MetadataRef;
use crate::observability::HotIndexSnapshot;
use crate::process::{PipelineId, ProcessInstanceId};
use crate::query::{
    EventQuery, ImpactQuery, PipelineQuery, ProcessInstanceQuery, ProcessStatusQuery, QueryLimit,
    QueryResult, TimeRangeQuery,
};
use std::collections::{BTreeSet, VecDeque};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogicalShardId(pub usize);

/// Dependency-free shard-local runtime model.
///
/// This is not the Sitas executor itself. It is the local shape that a Sitas
/// adapter should preserve: shard-local mutable indexes, owned values crossing
/// shard boundaries, and explicit fanout for queries that are not owned by a
/// single process instance.
#[derive(Debug)]
pub struct ShardLocalRuntime {
    shards: Vec<HotIndex>,
}

impl ShardLocalRuntime {
    pub fn new(shard_count: usize) -> Result<Self, RuntimeError> {
        if shard_count == 0 {
            return Err(RuntimeError::NoShards);
        }
        Ok(Self {
            shards: (0..shard_count).map(|_| HotIndex::new()).collect(),
        })
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    pub fn shard_for_process_instance(
        &self,
        process_instance_id: &ProcessInstanceId,
    ) -> LogicalShardId {
        shard_for_process_instance(process_instance_id, self.shards.len())
    }

    pub fn ingest(&mut self, event: PipelineEvent) -> Result<ShardIngestOutcome, RuntimeError> {
        let shard_id = self.shard_for_process_instance(event.process_instance_id());
        let outcome = self.shards[shard_id.0]
            .ingest(event)
            .map_err(RuntimeError::Ingest)?;
        Ok(ShardIngestOutcome { shard_id, outcome })
    }

    pub fn process_instance(
        &self,
        query: &ProcessInstanceQuery,
    ) -> Option<crate::process::ProcessInstanceSnapshot> {
        let shard_id = self.shard_for_process_instance(&query.process_instance_id);
        self.shards[shard_id.0].process_instance(query)
    }

    pub fn events(&self, query: &EventQuery) -> QueryResult {
        let shard_id = self.shard_for_process_instance(&query.process_instance_id);
        self.shards[shard_id.0].events(query)
    }

    pub fn pipeline_instances(&self, query: &PipelineQuery) -> QueryResult {
        let mut instances = Vec::new();
        for shard in &self.shards {
            let remaining = query.limit.value().saturating_sub(instances.len());
            if remaining == 0 {
                break;
            }
            let shard_query = PipelineQuery {
                pipeline_id: query.pipeline_id.clone(),
                limit: QueryLimit::new(remaining),
            };
            if let QueryResult::ProcessInstances(mut partial) =
                shard.pipeline_instances(&shard_query)
            {
                instances.append(&mut partial);
            }
        }
        QueryResult::ProcessInstances(instances)
    }

    pub fn impact(&self, query: &ImpactQuery) -> QueryResult {
        let mut events = Vec::new();
        for shard in &self.shards {
            let remaining = query.limit.value().saturating_sub(events.len());
            if remaining == 0 {
                break;
            }
            let shard_query = ImpactQuery {
                metadata_ref: query.metadata_ref.clone(),
                limit: QueryLimit::new(remaining),
            };
            if let QueryResult::Events(mut partial) = shard.impact(&shard_query) {
                events.append(&mut partial);
            }
        }
        QueryResult::Events(events)
    }

    pub fn process_instances_by_status(&self, query: &ProcessStatusQuery) -> QueryResult {
        let mut instances = Vec::new();
        for shard in &self.shards {
            let remaining = query.limit.value().saturating_sub(instances.len());
            if remaining == 0 {
                break;
            }
            let shard_query = ProcessStatusQuery {
                status: query.status,
                limit: QueryLimit::new(remaining),
            };
            if let QueryResult::ProcessInstances(mut partial) =
                shard.process_instances_by_status(&shard_query)
            {
                instances.append(&mut partial);
            }
        }
        QueryResult::ProcessInstances(instances)
    }

    pub fn events_in_time_range(&self, query: &TimeRangeQuery) -> QueryResult {
        let mut events = Vec::new();
        for shard in &self.shards {
            let remaining = query.limit.value().saturating_sub(events.len());
            if remaining == 0 {
                break;
            }
            let shard_query = TimeRangeQuery {
                start: query.start.clone(),
                end: query.end.clone(),
                limit: QueryLimit::new(remaining),
            };
            if let QueryResult::Events(mut partial) = shard.events_in_time_range(&shard_query) {
                events.append(&mut partial);
            }
        }
        events.sort_by(|left, right| left.timestamp().cmp(right.timestamp()));
        events.truncate(query.limit.value());
        QueryResult::Events(events)
    }

    pub fn affected_pipelines(&self, metadata_ref: &MetadataRef) -> Vec<PipelineId> {
        let mut pipelines = BTreeSet::new();
        for shard in &self.shards {
            for pipeline in shard.affected_pipelines(metadata_ref) {
                pipelines.insert(pipeline);
            }
        }
        pipelines.into_iter().collect()
    }

    pub fn snapshot(&self) -> ShardRuntimeSnapshot {
        let shards = self
            .shards
            .iter()
            .enumerate()
            .map(|(idx, shard)| ShardSnapshot {
                shard_id: LogicalShardId(idx),
                hot_index: shard.snapshot(),
            })
            .collect::<Vec<_>>();
        let totals = HotIndexSnapshot {
            process_instance_count: shards
                .iter()
                .map(|shard| shard.hot_index.process_instance_count)
                .sum(),
            event_count: shards.iter().map(|shard| shard.hot_index.event_count).sum(),
            pipeline_count: shards
                .iter()
                .map(|shard| shard.hot_index.pipeline_count)
                .sum(),
            metadata_ref_count: shards
                .iter()
                .map(|shard| shard.hot_index.metadata_ref_count)
                .sum(),
            duplicate_events: shards
                .iter()
                .map(|shard| shard.hot_index.duplicate_events)
                .sum(),
            rejected_events: shards
                .iter()
                .map(|shard| shard.hot_index.rejected_events)
                .sum(),
        };
        ShardRuntimeSnapshot { shards, totals }
    }

    pub fn shard(&self, shard_id: LogicalShardId) -> Option<&HotIndex> {
        self.shards.get(shard_id.0)
    }
}

impl Default for ShardLocalRuntime {
    fn default() -> Self {
        Self::new(1).expect("one shard is valid")
    }
}

/// Dependency-free bounded ingest boundary.
///
/// This models the submit/drain split expected from a future Sitas adapter:
/// external callers submit owned events into bounded per-shard queues, and the
/// owning shard explicitly drains its queue into shard-local hot state.
#[derive(Debug)]
pub struct BoundedIngestRuntime {
    runtime: ShardLocalRuntime,
    queues: Vec<VecDeque<PipelineEvent>>,
    per_shard_capacity: usize,
}

impl BoundedIngestRuntime {
    pub fn new(shard_count: usize, per_shard_capacity: usize) -> Result<Self, RuntimeError> {
        if per_shard_capacity == 0 {
            return Err(RuntimeError::QueueCapacityZero);
        }
        let runtime = ShardLocalRuntime::new(shard_count)?;
        Ok(Self {
            queues: (0..shard_count)
                .map(|_| VecDeque::with_capacity(per_shard_capacity))
                .collect(),
            runtime,
            per_shard_capacity,
        })
    }

    pub fn shard_count(&self) -> usize {
        self.runtime.shard_count()
    }

    pub fn per_shard_capacity(&self) -> usize {
        self.per_shard_capacity
    }

    pub fn submit(&mut self, event: PipelineEvent) -> Result<QueuedIngestOutcome, RuntimeError> {
        event.validate().map_err(RuntimeError::Ingest)?;
        let shard_id = self
            .runtime
            .shard_for_process_instance(event.process_instance_id());
        let queue = &mut self.queues[shard_id.0];
        if queue.len() >= self.per_shard_capacity {
            return Err(RuntimeError::QueueFull {
                shard_id,
                capacity: self.per_shard_capacity,
            });
        }
        queue.push_back(event);
        Ok(QueuedIngestOutcome {
            shard_id,
            queued_depth: queue.len(),
            capacity: self.per_shard_capacity,
        })
    }

    pub fn drain_shard(
        &mut self,
        shard_id: LogicalShardId,
        max_events: usize,
    ) -> Result<IngestDrainSummary, RuntimeError> {
        let Some(queue) = self.queues.get_mut(shard_id.0) else {
            return Err(RuntimeError::InvalidShard { shard_id });
        };

        let mut summary = IngestDrainSummary {
            shard_id,
            attempted: 0,
            accepted: 0,
            duplicates: 0,
            failed: 0,
            remaining_depth: queue.len(),
        };

        for _ in 0..max_events {
            let Some(event) = queue.pop_front() else {
                break;
            };
            summary.attempted += 1;
            match self.runtime.ingest(event) {
                Ok(ShardIngestOutcome {
                    outcome: IngestOutcome::Accepted,
                    ..
                }) => summary.accepted += 1,
                Ok(ShardIngestOutcome {
                    outcome: IngestOutcome::Duplicate,
                    ..
                }) => summary.duplicates += 1,
                Err(RuntimeError::Ingest(_)) => summary.failed += 1,
                Err(error) => return Err(error),
            }
        }

        summary.remaining_depth = queue.len();
        Ok(summary)
    }

    pub fn drain_all(
        &mut self,
        max_events_per_shard: usize,
    ) -> Result<Vec<IngestDrainSummary>, RuntimeError> {
        let mut summaries = Vec::with_capacity(self.queues.len());
        for idx in 0..self.queues.len() {
            summaries.push(self.drain_shard(LogicalShardId(idx), max_events_per_shard)?);
        }
        Ok(summaries)
    }

    pub fn runtime(&self) -> &ShardLocalRuntime {
        &self.runtime
    }

    pub fn runtime_mut(&mut self) -> &mut ShardLocalRuntime {
        &mut self.runtime
    }

    pub fn snapshot(&self) -> BoundedIngestRuntimeSnapshot {
        let queues = self
            .queues
            .iter()
            .enumerate()
            .map(|(idx, queue)| IngestQueueSnapshot {
                shard_id: LogicalShardId(idx),
                depth: queue.len(),
                capacity: self.per_shard_capacity,
            })
            .collect();
        BoundedIngestRuntimeSnapshot {
            runtime: self.runtime.snapshot(),
            queues,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueuedIngestOutcome {
    pub shard_id: LogicalShardId,
    pub queued_depth: usize,
    pub capacity: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IngestDrainSummary {
    pub shard_id: LogicalShardId,
    pub attempted: usize,
    pub accepted: usize,
    pub duplicates: usize,
    pub failed: usize,
    pub remaining_depth: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestQueueSnapshot {
    pub shard_id: LogicalShardId,
    pub depth: usize,
    pub capacity: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundedIngestRuntimeSnapshot {
    pub runtime: ShardRuntimeSnapshot,
    pub queues: Vec<IngestQueueSnapshot>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardIngestOutcome {
    pub shard_id: LogicalShardId,
    pub outcome: IngestOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardSnapshot {
    pub shard_id: LogicalShardId,
    pub hot_index: HotIndexSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardRuntimeSnapshot {
    pub shards: Vec<ShardSnapshot>,
    pub totals: HotIndexSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeError {
    NoShards,
    QueueCapacityZero,
    QueueFull {
        shard_id: LogicalShardId,
        capacity: usize,
    },
    InvalidShard {
        shard_id: LogicalShardId,
    },
    Ingest(IngestError),
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoShards => f.write_str("shard-local runtime requires at least one shard"),
            Self::QueueCapacityZero => {
                f.write_str("bounded ingest runtime requires non-zero queue capacity")
            }
            Self::QueueFull { shard_id, capacity } => write!(
                f,
                "ingest queue for logical shard {} is full at capacity {}",
                shard_id.0, capacity
            ),
            Self::InvalidShard { shard_id } => {
                write!(f, "logical shard {} does not exist", shard_id.0)
            }
            Self::Ingest(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for RuntimeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Ingest(error) => Some(error),
            Self::NoShards
            | Self::QueueCapacityZero
            | Self::QueueFull { .. }
            | Self::InvalidShard { .. } => None,
        }
    }
}

pub(crate) fn shard_for_process_instance(
    process_instance_id: &ProcessInstanceId,
    shard_count: usize,
) -> LogicalShardId {
    let mut state = 0xcbf2_9ce4_8422_2325u64;
    for byte in process_instance_id.as_str().as_bytes() {
        state ^= u64::from(*byte);
        state = state.wrapping_mul(0x0000_0100_0000_01b3);
    }
    LogicalShardId((state as usize) % shard_count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::{EventId, EventTimestamp, SourceId, SourceSequence};
    use crate::metadata::{DatasetId, MetadataRef};
    use crate::process::{
        EnvironmentId, EventKind, PipelineId, ProcessDefinitionId, ProcessInstanceId,
        ProcessStatus, TenantId,
    };

    #[test]
    fn runtime_routes_same_process_instance_to_same_shard() {
        let mut runtime = ShardLocalRuntime::new(4).unwrap();
        let first = event("event-1", "instance-a", "pipeline-a");
        let second = event("event-2", "instance-a", "pipeline-a");
        let expected = runtime.shard_for_process_instance(first.process_instance_id());

        assert_eq!(runtime.ingest(first).unwrap().shard_id, expected);
        assert_eq!(runtime.ingest(second).unwrap().shard_id, expected);
        assert_eq!(runtime.snapshot().totals.event_count, 2);
        assert_eq!(
            runtime
                .shard(expected)
                .unwrap()
                .snapshot()
                .process_instance_count,
            1
        );
    }

    #[test]
    fn runtime_routes_process_owned_queries_to_one_shard() {
        let mut runtime = ShardLocalRuntime::new(4).unwrap();
        runtime
            .ingest(event("event-1", "instance-a", "pipeline-a"))
            .unwrap();
        runtime
            .ingest(event("event-2", "instance-a", "pipeline-a"))
            .unwrap();

        let process = runtime
            .process_instance(&ProcessInstanceQuery {
                process_instance_id: ProcessInstanceId::from("instance-a"),
            })
            .unwrap();
        assert_eq!(
            process.process_instance_id,
            ProcessInstanceId::from("instance-a")
        );

        let QueryResult::Events(events) = runtime.events(&EventQuery {
            process_instance_id: ProcessInstanceId::from("instance-a"),
            limit: QueryLimit::new(10),
        }) else {
            panic!("events query should return events");
        };
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn runtime_fans_out_pipeline_and_impact_queries_with_global_limit() {
        let dataset = MetadataRef::Dataset(DatasetId::from("dataset-a"));
        let mut runtime = ShardLocalRuntime::new(8).unwrap();
        for idx in 0..6 {
            runtime
                .ingest(
                    event(
                        &format!("event-{idx}"),
                        &format!("instance-{idx}"),
                        "pipeline-a",
                    )
                    .with_metadata_refs(vec![dataset.clone()]),
                )
                .unwrap();
        }

        let QueryResult::ProcessInstances(instances) = runtime.pipeline_instances(&PipelineQuery {
            pipeline_id: PipelineId::from("pipeline-a"),
            limit: QueryLimit::new(3),
        }) else {
            panic!("pipeline query should return process instances");
        };
        assert_eq!(instances.len(), 3);

        let QueryResult::Events(events) = runtime.impact(&ImpactQuery {
            metadata_ref: dataset.clone(),
            limit: QueryLimit::new(4),
        }) else {
            panic!("impact query should return events");
        };
        assert_eq!(events.len(), 4);
        assert_eq!(
            runtime.affected_pipelines(&dataset),
            vec![PipelineId::from("pipeline-a")]
        );
    }

    #[test]
    fn runtime_fans_out_status_and_time_range_queries_with_global_limit() {
        let mut runtime = ShardLocalRuntime::new(8).unwrap();
        runtime
            .ingest(event_at(
                "event-1",
                "instance-a",
                "pipeline-a",
                "2026-06-30T10:00:00Z",
                EventKind::ProcessStarted,
            ))
            .unwrap();
        runtime
            .ingest(event_at(
                "event-2",
                "instance-b",
                "pipeline-a",
                "2026-06-30T10:00:01Z",
                EventKind::ProcessStarted,
            ))
            .unwrap();
        runtime
            .ingest(event_at(
                "event-3",
                "instance-b",
                "pipeline-a",
                "2026-06-30T10:00:03Z",
                EventKind::ProcessFailed,
            ))
            .unwrap();

        let QueryResult::ProcessInstances(failed) =
            runtime.process_instances_by_status(&ProcessStatusQuery {
                status: ProcessStatus::Failed,
                limit: QueryLimit::new(10),
            })
        else {
            panic!("status query should return process instances");
        };
        assert_eq!(failed.len(), 1);
        assert_eq!(
            failed[0].process_instance_id,
            ProcessInstanceId::from("instance-b")
        );

        let QueryResult::Events(events) = runtime.events_in_time_range(&TimeRangeQuery {
            start: Some(EventTimestamp::from("2026-06-30T10:00:01Z")),
            end: Some(EventTimestamp::from("2026-06-30T10:00:03Z")),
            limit: QueryLimit::new(2),
        }) else {
            panic!("time range query should return events");
        };
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_id(), &EventId::from("event-2"));
        assert_eq!(events[1].event_id(), &EventId::from("event-3"));
    }

    #[test]
    fn runtime_rejects_zero_shards() {
        assert!(matches!(
            ShardLocalRuntime::new(0).unwrap_err(),
            RuntimeError::NoShards
        ));
    }

    #[test]
    fn bounded_runtime_queues_by_owning_shard_and_drains_later() {
        let mut runtime = BoundedIngestRuntime::new(4, 8).unwrap();
        let event = event("event-1", "instance-a", "pipeline-a");
        let expected = runtime
            .runtime()
            .shard_for_process_instance(event.process_instance_id());

        let queued = runtime.submit(event).unwrap();

        assert_eq!(queued.shard_id, expected);
        assert_eq!(queued.queued_depth, 1);
        assert_eq!(runtime.snapshot().runtime.totals.event_count, 0);
        assert_eq!(runtime.snapshot().queues[expected.0].depth, 1);

        let summary = runtime.drain_shard(expected, 10).unwrap();

        assert_eq!(summary.accepted, 1);
        assert_eq!(summary.remaining_depth, 0);
        assert_eq!(runtime.snapshot().runtime.totals.event_count, 1);
    }

    #[test]
    fn bounded_runtime_applies_backpressure_when_queue_is_full() {
        let mut runtime = BoundedIngestRuntime::new(2, 1).unwrap();
        let first = event("event-1", "instance-a", "pipeline-a");
        let shard_id = runtime
            .runtime()
            .shard_for_process_instance(first.process_instance_id());

        runtime.submit(first).unwrap();
        let error = runtime
            .submit(event("event-2", "instance-a", "pipeline-a"))
            .unwrap_err();

        assert_eq!(
            error,
            RuntimeError::QueueFull {
                shard_id,
                capacity: 1,
            }
        );
        assert_eq!(runtime.snapshot().queues[shard_id.0].depth, 1);
        assert_eq!(runtime.snapshot().runtime.totals.event_count, 0);
    }

    #[test]
    fn bounded_runtime_drains_with_per_shard_limit_and_counts_duplicates() {
        let mut runtime = BoundedIngestRuntime::new(1, 8).unwrap();
        runtime
            .submit(event("event-1", "instance-a", "pipeline-a"))
            .unwrap();
        runtime
            .submit(event("event-2", "instance-a", "pipeline-a"))
            .unwrap();
        runtime
            .submit(event("event-2", "instance-a", "pipeline-a"))
            .unwrap();

        let first = runtime.drain_shard(LogicalShardId(0), 2).unwrap();
        assert_eq!(first.attempted, 2);
        assert_eq!(first.accepted, 2);
        assert_eq!(first.remaining_depth, 1);

        let second = runtime.drain_all(10).unwrap();
        assert_eq!(second[0].attempted, 1);
        assert_eq!(second[0].duplicates, 1);
        assert_eq!(runtime.snapshot().runtime.totals.event_count, 2);
        assert_eq!(runtime.snapshot().runtime.totals.duplicate_events, 1);
    }

    #[test]
    fn bounded_runtime_rejects_invalid_configuration_and_invalid_shard() {
        assert!(matches!(
            BoundedIngestRuntime::new(0, 1).unwrap_err(),
            RuntimeError::NoShards
        ));
        assert!(matches!(
            BoundedIngestRuntime::new(1, 0).unwrap_err(),
            RuntimeError::QueueCapacityZero
        ));

        let mut runtime = BoundedIngestRuntime::new(1, 1).unwrap();
        assert!(matches!(
            runtime.drain_shard(LogicalShardId(2), 1).unwrap_err(),
            RuntimeError::InvalidShard {
                shard_id: LogicalShardId(2)
            }
        ));
    }

    fn event(event_id: &str, instance_id: &str, pipeline_id: &str) -> PipelineEvent {
        event_at(
            event_id,
            instance_id,
            pipeline_id,
            "2026-06-30T10:00:00Z",
            EventKind::ProcessStarted,
        )
    }

    fn event_at(
        event_id: &str,
        instance_id: &str,
        pipeline_id: &str,
        timestamp: &str,
        kind: EventKind,
    ) -> PipelineEvent {
        PipelineEvent::new(
            EventId::from(event_id),
            SourceId::from("durga"),
            SourceSequence(1),
            TenantId::from("tenant-a"),
            EnvironmentId::from("prod"),
            PipelineId::from(pipeline_id),
            ProcessDefinitionId::from("definition-a"),
            ProcessInstanceId::from(instance_id),
            EventTimestamp::from(timestamp),
            kind,
        )
    }
}
