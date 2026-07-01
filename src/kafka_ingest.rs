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

//! Kafka-shaped process-event ingest boundary.
//!
//! This module intentionally does not depend on a Kafka client. It defines the
//! correctness boundary that a concrete Kafka consumer must satisfy: a decoded
//! process event with topic/partition/offset identity is durably journaled, fed
//! into the Sitas mailbox path, and only then becomes eligible for offset
//! commit.

use crate::ingest::{PipelineEvent, ProcessEventJournal, ProcessEventJournalError};
use crate::runtime::{QueuedIngestOutcome, RuntimeError};
use crate::sitas_runtime::{SitasRuntimeError, SitasShardRuntime};
use crate::storage::RecordOffset;
use std::fmt;

/// Kafka topic/partition identity for one consumed process-event record.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct KafkaTopicPartition {
    topic: String,
    partition: i32,
}

impl KafkaTopicPartition {
    /// Creates a topic/partition identity.
    pub fn new(topic: impl Into<String>, partition: i32) -> Self {
        Self {
            topic: topic.into(),
            partition,
        }
    }

    /// Returns the topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the partition number.
    pub fn partition(&self) -> i32 {
        self.partition
    }
}

/// Kafka record offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct KafkaOffset(pub i64);

impl KafkaOffset {
    /// Returns the next offset to commit after this record has been accepted.
    pub fn next_commit_offset(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

/// Decoded Kafka process-event record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaProcessRecord {
    /// Source topic and partition.
    pub topic_partition: KafkaTopicPartition,
    /// Source offset.
    pub offset: KafkaOffset,
    /// Decoded and validated Vannak process event.
    pub event: PipelineEvent,
}

impl KafkaProcessRecord {
    /// Creates a decoded process-event record.
    pub fn new(
        topic_partition: KafkaTopicPartition,
        offset: KafkaOffset,
        event: PipelineEvent,
    ) -> Self {
        Self {
            topic_partition,
            offset,
            event,
        }
    }
}

/// Kafka process-event record that may need to be retried after backpressure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaPendingRecord {
    record: KafkaProcessRecord,
    journal_offset: Option<RecordOffset>,
}

impl KafkaPendingRecord {
    /// Creates retry state for one decoded Kafka process-event record.
    pub fn new(record: KafkaProcessRecord) -> Self {
        Self {
            record,
            journal_offset: None,
        }
    }

    /// Returns the source topic/partition.
    pub fn topic_partition(&self) -> &KafkaTopicPartition {
        &self.record.topic_partition
    }

    /// Returns the source offset.
    pub fn offset(&self) -> KafkaOffset {
        self.record.offset
    }

    /// Returns the journal offset if this record has already been persisted.
    pub fn journal_offset(&self) -> Option<RecordOffset> {
        self.journal_offset
    }
}

/// Result returned after a Kafka record has crossed the durable/Sitas boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaSubmitOutcome {
    /// Source topic and partition.
    pub topic_partition: KafkaTopicPartition,
    /// Source offset that was accepted.
    pub offset: KafkaOffset,
    /// Next offset that a concrete Kafka consumer may commit.
    pub next_commit_offset: KafkaOffset,
    /// Optional process-event journal offset.
    pub journal_offset: Option<RecordOffset>,
    /// Sitas mailbox queue outcome.
    pub queued: QueuedIngestOutcome,
}

/// Submits one decoded Kafka process-event record to the Sitas runtime.
///
/// If `journal` is provided, the process event is appended before the Sitas
/// enqueue. The returned `next_commit_offset` is the earliest point at which an
/// external Kafka consumer may commit the partition offset.
pub fn submit_kafka_process_record(
    runtime: &SitasShardRuntime,
    journal: Option<&mut ProcessEventJournal>,
    record: KafkaProcessRecord,
) -> Result<KafkaSubmitOutcome, KafkaIngestError> {
    let journal_offset = if let Some(journal) = journal {
        Some(journal.append_durable(&record.event)?)
    } else {
        None
    };
    let queued = runtime.submit(record.event)?;
    Ok(KafkaSubmitOutcome {
        topic_partition: record.topic_partition,
        offset: record.offset,
        next_commit_offset: record.offset.next_commit_offset(),
        journal_offset,
        queued,
    })
}

/// Attempts to submit a pending Kafka record without waiting for mailbox
/// capacity.
///
/// If `journal` is provided, the process event is appended at most once and the
/// offset is retained in `pending` across backpressure retries. `Ok(None)` means
/// the owning Sitas mailbox is currently full, so a concrete Kafka consumer must
/// keep the source offset uncommitted and retry this same pending record later.
pub fn try_submit_kafka_pending_record(
    runtime: &SitasShardRuntime,
    journal: Option<&mut ProcessEventJournal>,
    pending: &mut KafkaPendingRecord,
) -> Result<Option<KafkaSubmitOutcome>, KafkaIngestError> {
    if pending.journal_offset.is_none()
        && let Some(journal) = journal
    {
        pending.journal_offset = Some(journal.append_durable(&pending.record.event)?);
    }

    let queued = match runtime.try_submit(pending.record.event.clone()) {
        Ok(queued) => queued,
        Err(error) if is_queue_full(&error) => return Ok(None),
        Err(error) => return Err(error.into()),
    };

    Ok(Some(KafkaSubmitOutcome {
        topic_partition: pending.record.topic_partition.clone(),
        offset: pending.record.offset,
        next_commit_offset: pending.record.offset.next_commit_offset(),
        journal_offset: pending.journal_offset,
        queued,
    }))
}

/// Errors returned by the Kafka-shaped ingest boundary.
#[derive(Debug)]
pub enum KafkaIngestError {
    /// Durable process-event journal append failed.
    Journal(ProcessEventJournalError),
    /// Sitas runtime admission failed.
    Runtime(SitasRuntimeError),
}

impl fmt::Display for KafkaIngestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Journal(error) => write!(f, "{error}"),
            Self::Runtime(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for KafkaIngestError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Journal(error) => Some(error),
            Self::Runtime(error) => Some(error),
        }
    }
}

impl From<ProcessEventJournalError> for KafkaIngestError {
    fn from(error: ProcessEventJournalError) -> Self {
        Self::Journal(error)
    }
}

impl From<SitasRuntimeError> for KafkaIngestError {
    fn from(error: SitasRuntimeError) -> Self {
        Self::Runtime(error)
    }
}

fn is_queue_full(error: &SitasRuntimeError) -> bool {
    matches!(
        error,
        SitasRuntimeError::Runtime(RuntimeError::QueueFull { .. })
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::{EventId, EventTimestamp, SourceId, SourceSequence};
    use crate::process::{
        EnvironmentId, EventKind, PipelineId, ProcessDefinitionId, ProcessInstanceId, TenantId,
    };
    use crate::runtime::LogicalShardId;
    use crate::sitas_runtime::SitasRuntimeConfig;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn kafka_record_is_journaled_then_fed_to_sitas_mailbox() {
        let mut runtime = SitasShardRuntime::start(SitasRuntimeConfig::new(2, 8)).unwrap();
        runtime.start_mailbox_workers().unwrap();
        let path = temp_path("vannak-kafka-journal");
        let mut journal = ProcessEventJournal::create(
            &path,
            crate::storage::SegmentId::from("segment-17"),
            crate::cluster::NodeId::from("node-a"),
        )
        .unwrap();
        let record = KafkaProcessRecord::new(
            KafkaTopicPartition::new("process-events-demo", 3),
            KafkaOffset(41),
            event("event-1", "instance-a"),
        );

        let outcome =
            submit_kafka_process_record(&runtime, Some(&mut journal), record.clone()).unwrap();

        assert_eq!(outcome.topic_partition, record.topic_partition);
        assert_eq!(outcome.offset, KafkaOffset(41));
        assert_eq!(outcome.next_commit_offset, KafkaOffset(42));
        assert!(outcome.journal_offset.is_some());
        assert!(outcome.queued.queued_depth <= outcome.queued.capacity);

        wait_for_event_count(&runtime, 1);
        let summaries = runtime.stop_mailbox_workers().unwrap();
        assert_eq!(summaries.iter().map(|(_, s)| s.accepted).sum::<u64>(), 1);
        runtime.stop().unwrap();
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn pending_kafka_record_reuses_journal_offset_after_backpressure() {
        let runtime = SitasShardRuntime::start(SitasRuntimeConfig::new(1, 1)).unwrap();
        let path = temp_path("vannak-kafka-pending-journal");
        let mut journal = ProcessEventJournal::create(
            &path,
            crate::storage::SegmentId::from("segment-18"),
            crate::cluster::NodeId::from("node-a"),
        )
        .unwrap();

        runtime
            .try_submit(event("event-1", "instance-a"))
            .expect("first event fills the mailbox");
        let mut pending = KafkaPendingRecord::new(KafkaProcessRecord::new(
            KafkaTopicPartition::new("process-events-demo", 3),
            KafkaOffset(42),
            event("event-2", "instance-a"),
        ));

        assert!(
            try_submit_kafka_pending_record(&runtime, Some(&mut journal), &mut pending)
                .unwrap()
                .is_none()
        );
        let first_journal_offset = pending.journal_offset().unwrap();
        assert!(
            try_submit_kafka_pending_record(&runtime, Some(&mut journal), &mut pending)
                .unwrap()
                .is_none()
        );
        assert_eq!(pending.journal_offset(), Some(first_journal_offset));

        let summary = runtime.drain_shard(LogicalShardId(0), 1).unwrap();
        assert_eq!(summary.accepted, 1);
        let outcome = try_submit_kafka_pending_record(&runtime, Some(&mut journal), &mut pending)
            .unwrap()
            .unwrap();

        assert_eq!(outcome.offset, KafkaOffset(42));
        assert_eq!(outcome.journal_offset, Some(first_journal_offset));
        runtime.stop().unwrap();
        let _ = std::fs::remove_file(path);
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

    fn temp_path(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("{label}-{}-{nanos}.seg", std::process::id()))
    }

    fn event(event_id: &str, process_instance: &str) -> PipelineEvent {
        PipelineEvent::new(
            EventId::from(event_id),
            SourceId::from("kafka"),
            SourceSequence(41),
            TenantId::from("tenant-a"),
            EnvironmentId::from("prod"),
            PipelineId::from("pipeline-a"),
            ProcessDefinitionId::from("definition-a"),
            ProcessInstanceId::from(process_instance),
            EventTimestamp::from("2026-06-30T10:00:00Z"),
            EventKind::ProcessStarted,
        )
    }
}
