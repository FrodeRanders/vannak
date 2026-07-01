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

//! Sitas-backed shard-local runtime adapter.
//!
//! This module is intentionally feature-gated. The dependency-free
//! [`crate::runtime`] module remains the default model, while this adapter wires
//! the same hot-index semantics to Sitas `ShardedExecutor`, `ShardLocal<T>`,
//! and `ShardMailboxSet<M>`.

use crate::index::{HotIndex, IngestOutcome};
use crate::ingest::{IngestError, PipelineEvent};
use crate::metadata::MetadataRef;
use crate::observability::HotIndexSnapshot;
use crate::process::{PipelineId, ProcessInstanceId};
use crate::query::{
    EventQuery, ImpactQuery, PipelineQuery, ProcessInstanceQuery, ProcessStatusQuery, QueryResult,
    TimeRangeQuery,
};
use crate::runtime::{
    IngestDrainSummary, LogicalShardId, QueuedIngestOutcome, RuntimeError, ShardIngestOutcome,
    ShardRuntimeSnapshot, ShardSnapshot, shard_for_process_instance,
};
use sitas::executor::{RaceOutput, StopToken, block_on, race};
use sitas::{
    ShardId, ShardLocal, ShardLocalAccessError, ShardMailboxConfig, ShardMailboxSet, ShardReceiver,
    ShardRecvError, ShardSendError, ShardedExecutor, ShardedExecutorConfig,
    StoppableShardLocalWorkers,
};
use std::collections::BTreeSet;
use std::fmt;

/// Configuration for [`SitasShardRuntime`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SitasRuntimeConfig {
    shard_count: usize,
    mailbox_capacity_per_shard: usize,
    thread_name_prefix: String,
}

impl SitasRuntimeConfig {
    /// Creates a Sitas runtime config with explicit shard and mailbox counts.
    pub fn new(shard_count: usize, mailbox_capacity_per_shard: usize) -> Self {
        Self {
            shard_count,
            mailbox_capacity_per_shard,
            thread_name_prefix: String::from("vannak-sitas-shard"),
        }
    }

    /// Sets the Sitas executor thread-name prefix.
    pub fn with_thread_name_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.thread_name_prefix = prefix.into();
        self
    }

    /// Returns the configured executor shard count.
    pub fn shard_count(&self) -> usize {
        self.shard_count
    }

    /// Returns the configured inbound mailbox capacity per shard.
    pub fn mailbox_capacity_per_shard(&self) -> usize {
        self.mailbox_capacity_per_shard
    }

    /// Returns the configured Sitas executor thread-name prefix.
    pub fn thread_name_prefix(&self) -> &str {
        &self.thread_name_prefix
    }
}

impl Default for SitasRuntimeConfig {
    fn default() -> Self {
        Self::new(1, sitas::DEFAULT_MAILBOX_CAPACITY)
    }
}

/// Runtime adapter backed by Sitas executor shards.
pub struct SitasShardRuntime {
    state: Option<ShardLocal<SitasShardState>>,
    mailboxes: Option<ShardMailboxSet<PipelineEvent>>,
    workers: Option<StoppableShardLocalWorkers<Result<IngestWorkerSummary, SitasRuntimeError>>>,
    executor: Option<ShardedExecutor>,
}

struct SitasShardState {
    hot_index: HotIndex,
    receiver: Option<ShardReceiver<PipelineEvent>>,
}

impl SitasShardRuntime {
    /// Starts a Sitas sharded executor and creates one Vannak hot index per
    /// executor shard.
    pub fn start(config: SitasRuntimeConfig) -> Result<Self, SitasRuntimeError> {
        if config.shard_count == 0 {
            return Err(SitasRuntimeError::Runtime(RuntimeError::NoShards));
        }
        if config.mailbox_capacity_per_shard == 0 {
            return Err(SitasRuntimeError::Runtime(RuntimeError::QueueCapacityZero));
        }

        let executor = ShardedExecutor::start_with_config(
            ShardedExecutorConfig::new(config.shard_count)
                .with_thread_name_prefix(config.thread_name_prefix),
        )
        .map_err(SitasRuntimeError::Start)?;
        let submitter = executor.submitter();
        let mailboxes = ShardMailboxSet::new(
            &submitter,
            ShardMailboxConfig::new(config.mailbox_capacity_per_shard),
        )
        .map_err(SitasRuntimeError::CreateMailbox)?;

        let mut receivers = (0..config.shard_count)
            .map(|idx| mailboxes.receiver_for(ShardId(idx)))
            .map(|receiver| receiver.map(Some))
            .collect::<Result<Vec<_>, _>>()
            .map_err(SitasRuntimeError::AddressMailbox)?;
        let state = ShardLocal::new(submitter, move |shard_id| SitasShardState {
            hot_index: HotIndex::new(),
            receiver: Some(
                receivers[shard_id.0]
                    .take()
                    .expect("receiver is initialized once per Sitas shard"),
            ),
        });

        Ok(Self {
            state: Some(state),
            mailboxes: Some(mailboxes),
            workers: None,
            executor: Some(executor),
        })
    }

    /// Returns the number of Sitas executor shards.
    pub fn shard_count(&self) -> usize {
        self.state.as_ref().map_or(0, ShardLocal::shard_count)
    }

    /// Routes a process instance to the Sitas executor shard that owns its hot
    /// index state.
    pub fn shard_for_process_instance(
        &self,
        process_instance_id: &ProcessInstanceId,
    ) -> LogicalShardId {
        shard_for_process_instance(process_instance_id, self.shard_count())
    }

    /// Submits an event directly to the owning Sitas shard and waits for the
    /// shard-local hot index to ingest it.
    pub fn ingest(&self, event: PipelineEvent) -> Result<ShardIngestOutcome, SitasRuntimeError> {
        let shard_id = self.shard_for_process_instance(event.process_instance_id());
        let sitas_shard = to_sitas_shard(shard_id);
        let state = self.state()?;
        let handle = state
            .with_on(sitas_shard, move |state| state.hot_index.ingest(event))
            .map_err(SitasRuntimeError::Spawn)?;
        let (_, outcome) = block_on(handle.join()).map_err(SitasRuntimeError::Join)?;
        let outcome = outcome.map_err(SitasRuntimeError::Ingest)?;
        Ok(ShardIngestOutcome { shard_id, outcome })
    }

    /// Enqueues an owned event into the bounded Sitas mailbox for the owning
    /// shard without waiting for capacity.
    pub fn try_submit(
        &self,
        event: PipelineEvent,
    ) -> Result<QueuedIngestOutcome, SitasRuntimeError> {
        event.validate().map_err(SitasRuntimeError::Ingest)?;
        let shard_id = self.shard_for_process_instance(event.process_instance_id());
        let sitas_shard = to_sitas_shard(shard_id);
        let mailboxes = self.mailboxes()?;
        let sender = mailboxes
            .sender_to(sitas_shard)
            .map_err(SitasRuntimeError::AddressMailbox)?;
        sender.try_send(event).map_err(|error| match error {
            ShardSendError::Full(_) => SitasRuntimeError::Runtime(RuntimeError::QueueFull {
                shard_id,
                capacity: mailboxes
                    .snapshot(sitas_shard)
                    .map(|snapshot| snapshot.capacity)
                    .unwrap_or_default(),
            }),
            ShardSendError::Closed(_) => SitasRuntimeError::MailboxClosed { shard_id },
        })?;
        let snapshot = mailboxes
            .snapshot(sitas_shard)
            .map_err(SitasRuntimeError::AddressMailbox)?;
        Ok(QueuedIngestOutcome {
            shard_id,
            queued_depth: snapshot.len,
            capacity: snapshot.capacity,
        })
    }

    /// Enqueues an owned event into the bounded Sitas mailbox, waiting for
    /// capacity if the owning shard's mailbox is full.
    ///
    /// This is the ingestion primitive for external sources that should apply
    /// backpressure instead of dropping or retry-spinning when Sitas workers are
    /// temporarily behind.
    pub fn submit(&self, event: PipelineEvent) -> Result<QueuedIngestOutcome, SitasRuntimeError> {
        event.validate().map_err(SitasRuntimeError::Ingest)?;
        let shard_id = self.shard_for_process_instance(event.process_instance_id());
        let sitas_shard = to_sitas_shard(shard_id);
        let mailboxes = self.mailboxes()?;
        let sender = mailboxes
            .sender_to(sitas_shard)
            .map_err(SitasRuntimeError::AddressMailbox)?;
        block_on(sender.send(event)).map_err(|error| match error {
            ShardSendError::Full(_) => SitasRuntimeError::Runtime(RuntimeError::QueueFull {
                shard_id,
                capacity: mailboxes
                    .snapshot(sitas_shard)
                    .map(|snapshot| snapshot.capacity)
                    .unwrap_or_default(),
            }),
            ShardSendError::Closed(_) => SitasRuntimeError::MailboxClosed { shard_id },
        })?;
        let snapshot = mailboxes
            .snapshot(sitas_shard)
            .map_err(SitasRuntimeError::AddressMailbox)?;
        Ok(QueuedIngestOutcome {
            shard_id,
            queued_depth: snapshot.len,
            capacity: snapshot.capacity,
        })
    }

    /// Drains up to `max_events` queued mailbox events on one Sitas shard into
    /// that shard's hot index.
    pub fn drain_shard(
        &self,
        shard_id: LogicalShardId,
        max_events: usize,
    ) -> Result<IngestDrainSummary, SitasRuntimeError> {
        if shard_id.0 >= self.shard_count() {
            return Err(SitasRuntimeError::Runtime(RuntimeError::InvalidShard {
                shard_id,
            }));
        }

        let sitas_shard = to_sitas_shard(shard_id);
        let state = self.state()?;
        let handle = state
            .with_on(sitas_shard, move |state| {
                let mut summary = IngestDrainSummary {
                    shard_id,
                    attempted: 0,
                    accepted: 0,
                    duplicates: 0,
                    failed: 0,
                    remaining_depth: 0,
                };
                let receiver = state
                    .receiver
                    .as_mut()
                    .ok_or(SitasRuntimeError::WorkersRunning)?;

                for _ in 0..max_events {
                    let event = match receiver.try_recv() {
                        Ok(event) => event,
                        Err(ShardRecvError::Empty | ShardRecvError::Closed) => break,
                    };
                    summary.attempted += 1;
                    match state.hot_index.ingest(event) {
                        Ok(IngestOutcome::Accepted) => summary.accepted += 1,
                        Ok(IngestOutcome::Duplicate) => summary.duplicates += 1,
                        Err(_) => summary.failed += 1,
                    }
                }
                Ok(summary)
            })
            .map_err(SitasRuntimeError::Spawn)?;
        let (_, summary) = block_on(handle.join()).map_err(SitasRuntimeError::Join)?;
        let mut summary = summary?;
        summary.remaining_depth = self
            .mailboxes()?
            .snapshot(sitas_shard)
            .map_err(SitasRuntimeError::AddressMailbox)?
            .len;
        Ok(summary)
    }

    /// Drains all Sitas shard mailboxes with the same per-shard limit.
    pub fn drain_all(
        &self,
        max_events_per_shard: usize,
    ) -> Result<Vec<IngestDrainSummary>, SitasRuntimeError> {
        (0..self.shard_count())
            .map(|idx| self.drain_shard(LogicalShardId(idx), max_events_per_shard))
            .collect()
    }

    /// Starts one long-running mailbox worker per Sitas shard.
    ///
    /// Workers continuously receive owned process events from their shard's
    /// mailbox and reduce them into the shard-local [`HotIndex`]. While workers
    /// are running, manual [`drain_shard`](Self::drain_shard) calls are rejected
    /// because each Sitas mailbox has a single receiver.
    pub fn start_mailbox_workers(&mut self) -> Result<(), SitasRuntimeError> {
        if self.workers.is_some() {
            return Err(SitasRuntimeError::WorkersAlreadyRunning);
        }
        let state = self.state()?;
        let workers = state
            .spawn_stoppable_workers(|shard_id, state, stop_token| async move {
                run_ingest_worker(shard_id, state, stop_token).await
            })
            .map_err(SitasRuntimeError::Spawn)?;
        self.workers = Some(workers);
        Ok(())
    }

    /// Requests cooperative stop for mailbox workers and waits for summaries.
    pub fn stop_mailbox_workers(
        &mut self,
    ) -> Result<Vec<(LogicalShardId, IngestWorkerSummary)>, SitasRuntimeError> {
        let Some(workers) = self.workers.take() else {
            return Ok(Vec::new());
        };
        block_on(workers.stop_and_join())
            .map_err(SitasRuntimeError::Join)
            .and_then(|summaries| {
                summaries
                    .into_iter()
                    .map(|(shard_id, summary)| {
                        summary.map(|summary| (to_logical_shard(shard_id), summary))
                    })
                    .collect()
            })
    }

    /// Runs a process-instance query on the owning Sitas shard.
    pub fn process_instance(
        &self,
        query: &ProcessInstanceQuery,
    ) -> Result<Option<crate::process::ProcessInstanceSnapshot>, SitasRuntimeError> {
        let shard_id = self.shard_for_process_instance(&query.process_instance_id);
        let query = query.clone();
        self.with_hot_index_on(shard_id, move |index| index.process_instance(&query))
    }

    /// Runs an event query on the owning Sitas shard.
    pub fn events(&self, query: &EventQuery) -> Result<QueryResult, SitasRuntimeError> {
        let shard_id = self.shard_for_process_instance(&query.process_instance_id);
        let query = query.clone();
        self.with_hot_index_on(shard_id, move |index| index.events(&query))
    }

    /// Fans out a pipeline query across Sitas shards and returns owned results.
    pub fn pipeline_instances(
        &self,
        query: &PipelineQuery,
    ) -> Result<QueryResult, SitasRuntimeError> {
        let mut instances = Vec::new();
        for result in self.map_hot_indexes({
            let query = query.clone();
            move |index| index.pipeline_instances(&query)
        })? {
            if let QueryResult::ProcessInstances(mut partial) = result {
                let remaining = query.limit.value().saturating_sub(instances.len());
                instances.extend(partial.drain(..remaining.min(partial.len())));
            }
            if query.limit.reached(instances.len()) {
                break;
            }
        }
        Ok(QueryResult::ProcessInstances(instances))
    }

    /// Fans out a metadata-impact query across Sitas shards.
    pub fn impact(&self, query: &ImpactQuery) -> Result<QueryResult, SitasRuntimeError> {
        let mut events = Vec::new();
        for result in self.map_hot_indexes({
            let query = query.clone();
            move |index| index.impact(&query)
        })? {
            if let QueryResult::Events(mut partial) = result {
                let remaining = query.limit.value().saturating_sub(events.len());
                events.extend(partial.drain(..remaining.min(partial.len())));
            }
            if query.limit.reached(events.len()) {
                break;
            }
        }
        Ok(QueryResult::Events(events))
    }

    /// Fans out a current-status query across Sitas shards.
    pub fn process_instances_by_status(
        &self,
        query: &ProcessStatusQuery,
    ) -> Result<QueryResult, SitasRuntimeError> {
        let mut instances = Vec::new();
        for result in self.map_hot_indexes({
            let query = query.clone();
            move |index| index.process_instances_by_status(&query)
        })? {
            if let QueryResult::ProcessInstances(mut partial) = result {
                let remaining = query.limit.value().saturating_sub(instances.len());
                instances.extend(partial.drain(..remaining.min(partial.len())));
            }
            if query.limit.reached(instances.len()) {
                break;
            }
        }
        Ok(QueryResult::ProcessInstances(instances))
    }

    /// Fans out a time-range query across Sitas shards.
    pub fn events_in_time_range(
        &self,
        query: &TimeRangeQuery,
    ) -> Result<QueryResult, SitasRuntimeError> {
        let mut events = Vec::new();
        for result in self.map_hot_indexes({
            let query = query.clone();
            move |index| index.events_in_time_range(&query)
        })? {
            if let QueryResult::Events(mut partial) = result {
                events.append(&mut partial);
            }
        }
        events.sort_by(|left, right| left.timestamp().cmp(right.timestamp()));
        events.truncate(query.limit.value());
        Ok(QueryResult::Events(events))
    }

    /// Fans out affected-pipeline collection across Sitas shards.
    pub fn affected_pipelines(
        &self,
        metadata_ref: &MetadataRef,
    ) -> Result<Vec<PipelineId>, SitasRuntimeError> {
        let mut pipelines = BTreeSet::new();
        for partial in self.map_hot_indexes({
            let metadata_ref = metadata_ref.clone();
            move |index| index.affected_pipelines(&metadata_ref)
        })? {
            pipelines.extend(partial);
        }
        Ok(pipelines.into_iter().collect())
    }

    /// Returns owned executor, mailbox, and Vannak hot-index snapshots.
    pub fn snapshot(&self) -> Result<SitasRuntimeSnapshot, SitasRuntimeError> {
        let executor = self
            .executor
            .as_ref()
            .ok_or(SitasRuntimeError::Stopped)?
            .snapshot();
        let mailboxes = self
            .mailboxes()?
            .snapshots()
            .into_iter()
            .map(SitasMailboxSnapshot::from)
            .collect();
        Ok(SitasRuntimeSnapshot {
            executor,
            runtime: self.runtime_snapshot()?,
            mailboxes,
        })
    }

    /// Stops Sitas executor shards after dropping submitter-owning state.
    pub fn stop(mut self) -> Result<(), SitasRuntimeError> {
        let _ = self.stop_mailbox_workers()?;
        self.state.take();
        self.mailboxes.take();
        if let Some(executor) = self.executor.take() {
            executor.stop().map_err(SitasRuntimeError::Start)?;
        }
        Ok(())
    }

    fn state(&self) -> Result<&ShardLocal<SitasShardState>, SitasRuntimeError> {
        self.state.as_ref().ok_or(SitasRuntimeError::Stopped)
    }

    fn mailboxes(&self) -> Result<&ShardMailboxSet<PipelineEvent>, SitasRuntimeError> {
        self.mailboxes.as_ref().ok_or(SitasRuntimeError::Stopped)
    }

    fn with_hot_index_on<R, F>(
        &self,
        shard_id: LogicalShardId,
        operation: F,
    ) -> Result<R, SitasRuntimeError>
    where
        R: Send + 'static,
        F: FnOnce(&mut HotIndex) -> R + Send + 'static,
    {
        let handle = self
            .state()?
            .with_on(to_sitas_shard(shard_id), move |state| {
                operation(&mut state.hot_index)
            })
            .map_err(SitasRuntimeError::Spawn)?;
        let (_, output) = block_on(handle.join()).map_err(SitasRuntimeError::Join)?;
        Ok(output)
    }

    fn map_hot_indexes<R, F>(&self, operation: F) -> Result<Vec<R>, SitasRuntimeError>
    where
        R: Send + 'static,
        F: Fn(&mut HotIndex) -> R + Send + Clone + 'static,
    {
        let outputs = block_on(self.state()?.map_all(move |_, state| {
            let operation = operation.clone();
            operation(&mut state.hot_index)
        }))
        .map_err(SitasRuntimeError::Operation)?;
        Ok(outputs.into_iter().map(|(_, output)| output).collect())
    }

    fn runtime_snapshot(&self) -> Result<ShardRuntimeSnapshot, SitasRuntimeError> {
        let snapshots = self.map_hot_indexes(|index| index.snapshot())?;
        let shards = snapshots
            .into_iter()
            .enumerate()
            .map(|(idx, hot_index)| ShardSnapshot {
                shard_id: LogicalShardId(idx),
                hot_index,
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
        Ok(ShardRuntimeSnapshot { shards, totals })
    }
}

impl Drop for SitasShardRuntime {
    fn drop(&mut self) {
        if let Some(workers) = self.workers.take() {
            let _ = block_on(workers.stop_and_join());
        }
        self.state.take();
        self.mailboxes.take();
        if let Some(executor) = self.executor.take() {
            let _ = executor.stop();
        }
    }
}

/// Summary returned by one Sitas mailbox worker.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IngestWorkerSummary {
    /// Events read from the shard mailbox.
    pub attempted: u64,
    /// New events accepted into the hot index.
    pub accepted: u64,
    /// Duplicate event ids observed by the hot index.
    pub duplicates: u64,
    /// Events rejected by validation/reduction.
    pub failed: u64,
}

/// Owned snapshot of the Sitas-backed Vannak runtime.
#[derive(Debug, Clone)]
pub struct SitasRuntimeSnapshot {
    /// Sitas executor snapshot.
    pub executor: sitas::ShardedExecutorSnapshot,
    /// Vannak shard-local hot-index snapshot.
    pub runtime: ShardRuntimeSnapshot,
    /// Sitas mailbox snapshots converted to Vannak logical shard ids.
    pub mailboxes: Vec<SitasMailboxSnapshot>,
}

/// Owned snapshot of one Sitas process-event mailbox.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SitasMailboxSnapshot {
    /// Vannak logical shard, equal to the Sitas executor shard for this adapter.
    pub shard_id: LogicalShardId,
    /// Maximum number of queued process events.
    pub capacity: usize,
    /// Current queued process-event count.
    pub len: usize,
    /// Number of live sender handles.
    pub sender_count: usize,
    /// Whether the receiver has been taken by the shard-local state.
    pub receiver_taken: bool,
    /// Whether the receiver has been closed.
    pub receiver_closed: bool,
    /// Total accepted sends.
    pub sent: u64,
    /// Total received events.
    pub received: u64,
    /// Total full-mailbox rejections.
    pub full_rejections: u64,
    /// Total closed-mailbox rejections.
    pub closed_rejections: u64,
    /// Number of async senders parked for capacity.
    pub send_waiter_count: usize,
}

impl From<sitas::ShardMailboxSnapshot> for SitasMailboxSnapshot {
    fn from(snapshot: sitas::ShardMailboxSnapshot) -> Self {
        Self {
            shard_id: to_logical_shard(snapshot.shard_id),
            capacity: snapshot.capacity,
            len: snapshot.len,
            sender_count: snapshot.sender_count,
            receiver_taken: snapshot.receiver_taken,
            receiver_closed: snapshot.receiver_closed,
            sent: snapshot.sent,
            received: snapshot.received,
            full_rejections: snapshot.full_rejections,
            closed_rejections: snapshot.closed_rejections,
            send_waiter_count: snapshot.send_waiter_count,
        }
    }
}

/// Errors returned by the Sitas adapter.
#[derive(Debug)]
pub enum SitasRuntimeError {
    /// Vannak runtime-level validation failed.
    Runtime(RuntimeError),
    /// Process-event validation or ingestion failed.
    Ingest(IngestError),
    /// Sitas executor startup or shutdown failed.
    Start(sitas::ShardError),
    /// Sitas rejected task placement.
    Spawn(sitas::ShardedSpawnError),
    /// Sitas shard task failed while being joined.
    Join(sitas::ShardedJoinError),
    /// Sitas fanout operation failed.
    Operation(sitas::ShardedOperationError),
    /// Sitas mailbox creation failed.
    CreateMailbox(sitas::ShardMailboxCreateError),
    /// Sitas mailbox addressing failed.
    AddressMailbox(sitas::ShardMailboxAddressError),
    /// The adapter has already stopped.
    Stopped,
    /// Mailbox workers are already running.
    WorkersAlreadyRunning,
    /// Manual mailbox drain was requested while workers own the receivers.
    WorkersRunning,
    /// A shard worker could not access its shard-local state.
    WorkerAccess(ShardLocalAccessError),
    /// The target process-event mailbox is closed.
    MailboxClosed {
        /// Vannak logical shard whose mailbox is closed.
        shard_id: LogicalShardId,
    },
}

impl fmt::Display for SitasRuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Runtime(error) => write!(f, "{error}"),
            Self::Ingest(error) => write!(f, "{error}"),
            Self::Start(error) => write!(f, "{error}"),
            Self::Spawn(error) => write!(f, "{error}"),
            Self::Join(error) => write!(f, "{error}"),
            Self::Operation(error) => write!(f, "{error}"),
            Self::CreateMailbox(error) => write!(f, "{error}"),
            Self::AddressMailbox(error) => write!(f, "{error}"),
            Self::Stopped => f.write_str("Sitas runtime has stopped"),
            Self::WorkersAlreadyRunning => f.write_str("Sitas mailbox workers are already running"),
            Self::WorkersRunning => {
                f.write_str("Sitas mailbox workers own the shard mailbox receivers")
            }
            Self::WorkerAccess(error) => write!(f, "{error}"),
            Self::MailboxClosed { shard_id } => {
                write!(
                    f,
                    "process-event mailbox for logical shard {} is closed",
                    shard_id.0
                )
            }
        }
    }
}

impl std::error::Error for SitasRuntimeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Runtime(error) => Some(error),
            Self::Ingest(error) => Some(error),
            Self::Start(error) => Some(error),
            Self::Spawn(error) => Some(error),
            Self::Join(error) => Some(error),
            Self::Operation(error) => Some(error),
            Self::CreateMailbox(error) => Some(error),
            Self::AddressMailbox(error) => Some(error),
            Self::WorkerAccess(error) => Some(error),
            Self::Stopped
            | Self::WorkersAlreadyRunning
            | Self::WorkersRunning
            | Self::MailboxClosed { .. } => None,
        }
    }
}

async fn run_ingest_worker(
    shard_id: ShardId,
    state: ShardLocal<SitasShardState>,
    stop_token: StopToken,
) -> Result<IngestWorkerSummary, SitasRuntimeError> {
    let mut receiver = state
        .with_current_result(|state| state.receiver.take())
        .map_err(SitasRuntimeError::WorkerAccess)?
        .ok_or(SitasRuntimeError::WorkersRunning)?;
    let mut summary = IngestWorkerSummary::default();

    loop {
        match race(receiver.recv(), stop_token.clone()).await {
            RaceOutput::First(Ok(event)) => {
                summary.attempted += 1;
                let outcome = state
                    .with_current_result(|state| state.hot_index.ingest(event))
                    .map_err(SitasRuntimeError::WorkerAccess)?;
                match outcome {
                    Ok(IngestOutcome::Accepted) => summary.accepted += 1,
                    Ok(IngestOutcome::Duplicate) => summary.duplicates += 1,
                    Err(_) => summary.failed += 1,
                }
            }
            RaceOutput::First(Err(ShardRecvError::Closed)) | RaceOutput::Second(()) => break,
            RaceOutput::First(Err(ShardRecvError::Empty)) => {}
        }
    }

    state
        .with_current_result(|state| {
            state.receiver = Some(receiver);
        })
        .map_err(SitasRuntimeError::WorkerAccess)?;
    debug_assert_eq!(sitas::current_executor_shard(), Some(shard_id));
    Ok(summary)
}

fn to_sitas_shard(shard_id: LogicalShardId) -> ShardId {
    ShardId(shard_id.0)
}

fn to_logical_shard(shard_id: ShardId) -> LogicalShardId {
    LogicalShardId(shard_id.0)
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
    use crate::query::QueryLimit;

    #[test]
    fn direct_ingest_runs_on_owning_sitas_shard() {
        let runtime = SitasShardRuntime::start(SitasRuntimeConfig::new(2, 4)).unwrap();
        let event = event("event-1", "instance-a", "pipeline-a");
        let expected = runtime.shard_for_process_instance(event.process_instance_id());

        let outcome = runtime.ingest(event).unwrap();

        assert_eq!(outcome.shard_id, expected);
        assert_eq!(outcome.outcome, IngestOutcome::Accepted);
        assert_eq!(runtime.snapshot().unwrap().runtime.totals.event_count, 1);
        runtime.stop().unwrap();
    }

    #[test]
    fn mailbox_submit_applies_backpressure_and_explicit_drain() {
        let runtime = SitasShardRuntime::start(SitasRuntimeConfig::new(1, 1)).unwrap();
        runtime
            .try_submit(event("event-1", "instance-a", "pipeline-a"))
            .unwrap();

        let full = runtime
            .try_submit(event("event-2", "instance-a", "pipeline-a"))
            .unwrap_err();
        assert!(matches!(
            full,
            SitasRuntimeError::Runtime(RuntimeError::QueueFull { .. })
        ));
        assert_eq!(runtime.snapshot().unwrap().runtime.totals.event_count, 0);

        let summary = runtime.drain_shard(LogicalShardId(0), 10).unwrap();

        assert_eq!(summary.accepted, 1);
        assert_eq!(summary.remaining_depth, 0);
        assert_eq!(runtime.snapshot().unwrap().runtime.totals.event_count, 1);
        runtime.stop().unwrap();
    }

    #[test]
    fn mailbox_workers_continuously_feed_shard_local_indexes() {
        let mut runtime = SitasShardRuntime::start(SitasRuntimeConfig::new(2, 8)).unwrap();
        runtime.start_mailbox_workers().unwrap();

        runtime
            .try_submit(event("event-1", "instance-a", "pipeline-a"))
            .unwrap();
        runtime
            .try_submit(event("event-2", "instance-b", "pipeline-a"))
            .unwrap();

        wait_for_event_count(&runtime, 2);
        assert!(matches!(
            runtime.drain_shard(LogicalShardId(0), 1).unwrap_err(),
            SitasRuntimeError::WorkersRunning
        ));
        let summaries = runtime.stop_mailbox_workers().unwrap();

        assert_eq!(summaries.iter().map(|(_, s)| s.accepted).sum::<u64>(), 2);
        assert_eq!(runtime.snapshot().unwrap().runtime.totals.event_count, 2);
        runtime.stop().unwrap();
    }

    #[test]
    fn fanout_queries_return_owned_results_from_sitas_shards() {
        let runtime = SitasShardRuntime::start(SitasRuntimeConfig::new(4, 8)).unwrap();
        let dataset = MetadataRef::Dataset(DatasetId::from("dataset-a"));
        runtime
            .ingest(
                event("event-1", "instance-a", "pipeline-a")
                    .with_metadata_refs(vec![dataset.clone()]),
            )
            .unwrap();
        runtime
            .ingest(event_with_kind(
                "event-2",
                "instance-b",
                "pipeline-a",
                EventKind::ProcessFailed,
            ))
            .unwrap();

        let QueryResult::ProcessInstances(instances) = runtime
            .pipeline_instances(&PipelineQuery {
                pipeline_id: PipelineId::from("pipeline-a"),
                limit: QueryLimit::new(10),
            })
            .unwrap()
        else {
            panic!("pipeline query should return process instances");
        };
        assert_eq!(instances.len(), 2);

        let QueryResult::ProcessInstances(failed) = runtime
            .process_instances_by_status(&ProcessStatusQuery {
                status: ProcessStatus::Failed,
                limit: QueryLimit::new(10),
            })
            .unwrap()
        else {
            panic!("status query should return process instances");
        };
        assert_eq!(failed.len(), 1);
        assert_eq!(
            runtime.affected_pipelines(&dataset).unwrap(),
            vec![PipelineId::from("pipeline-a")]
        );
        runtime.stop().unwrap();
    }

    fn wait_for_event_count(runtime: &SitasShardRuntime, expected: usize) {
        for _ in 0..100 {
            if runtime.snapshot().unwrap().runtime.totals.event_count == expected {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert_eq!(
            runtime.snapshot().unwrap().runtime.totals.event_count,
            expected
        );
    }

    fn event(event_id: &str, process_instance: &str, pipeline: &str) -> PipelineEvent {
        event_with_kind(
            event_id,
            process_instance,
            pipeline,
            EventKind::ProcessStarted,
        )
    }

    fn event_with_kind(
        event_id: &str,
        process_instance: &str,
        pipeline: &str,
        kind: EventKind,
    ) -> PipelineEvent {
        PipelineEvent::new(
            EventId::from(event_id),
            SourceId::from("durga"),
            SourceSequence(1),
            TenantId::from("tenant-a"),
            EnvironmentId::from("prod"),
            PipelineId::from(pipeline),
            ProcessDefinitionId::from("definition-a"),
            ProcessInstanceId::from(process_instance),
            EventTimestamp::from("2026-06-30T10:00:00Z"),
            kind,
        )
    }
}
