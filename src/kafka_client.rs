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

//! Concrete Kafka consumer for process-event topics.
//!
//! This module is feature-gated behind `kafka-client`. It uses `rdkafka` only at
//! the edge, decodes Vannak binary process-event payloads or Durga JSON monitor
//! events, feeds the Sitas mailbox runtime, and commits Kafka offsets after
//! durable/Sitas admission succeeds. If Sitas reports mailbox backpressure, the
//! consumer keeps the decoded record pending, pauses the Kafka partition, and
//! resumes it after the record is accepted.

use crate::durga::{DurgaErrorInfo, DurgaEventType, DurgaProcessEvent, DurgaStatus};
use crate::ingest::{
    PipelineEvent, ProcessEventDecodeError, ProcessEventJournal, SourceId, SourceSequence,
    decode_pipeline_event,
};
use crate::kafka_ingest::{
    KafkaIngestError, KafkaOffset, KafkaPendingRecord, KafkaProcessRecord, KafkaSubmitOutcome,
    KafkaTopicPartition, try_submit_kafka_pending_record,
};
use crate::metadata::MetadataRef;
use crate::observability::DurgaCompatibilitySnapshot;
use crate::process::{EnvironmentId, TenantId};
use crate::sitas_runtime::SitasShardRuntime;
use rdkafka::ClientConfig;
use rdkafka::client::ClientContext;
use rdkafka::config::FromClientConfigAndContext;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaError;
use rdkafka::message::Message;
use rdkafka::{Offset, TopicPartitionList};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

/// Configuration for a process-event Kafka consumer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaProcessConsumerConfig {
    brokers: String,
    group_id: String,
    topics: Vec<String>,
    payload_format: KafkaPayloadFormat,
    durga_source_id: SourceId,
    durga_tenant_id: TenantId,
    durga_environment_id: EnvironmentId,
    poll_timeout: Duration,
    enable_partition_eof: bool,
    auto_offset_reset: String,
}

impl KafkaProcessConsumerConfig {
    /// Creates a Kafka consumer config.
    pub fn new(
        brokers: impl Into<String>,
        group_id: impl Into<String>,
        topics: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            brokers: brokers.into(),
            group_id: group_id.into(),
            topics: topics.into_iter().map(Into::into).collect(),
            payload_format: KafkaPayloadFormat::VannakBinary,
            durga_source_id: SourceId::from("kafka"),
            durga_tenant_id: TenantId::from("default"),
            durga_environment_id: EnvironmentId::from("default"),
            poll_timeout: Duration::from_millis(100),
            enable_partition_eof: false,
            auto_offset_reset: String::from("earliest"),
        }
    }

    /// Sets the poll timeout used by [`KafkaProcessConsumer::poll_once`].
    pub fn with_poll_timeout(mut self, poll_timeout: Duration) -> Self {
        self.poll_timeout = poll_timeout;
        self
    }

    /// Enables or disables partition EOF events.
    pub fn with_partition_eof(mut self, enable_partition_eof: bool) -> Self {
        self.enable_partition_eof = enable_partition_eof;
        self
    }

    /// Sets Kafka's `auto.offset.reset` policy for new consumer groups.
    pub fn with_auto_offset_reset(mut self, auto_offset_reset: impl Into<String>) -> Self {
        self.auto_offset_reset = auto_offset_reset.into();
        self
    }

    /// Sets the Kafka payload format.
    pub fn with_payload_format(mut self, payload_format: KafkaPayloadFormat) -> Self {
        self.payload_format = payload_format;
        self
    }

    /// Sets context used when decoding Durga JSON records.
    pub fn with_durga_context(
        mut self,
        source_id: SourceId,
        tenant_id: TenantId,
        environment_id: EnvironmentId,
    ) -> Self {
        self.durga_source_id = source_id;
        self.durga_tenant_id = tenant_id;
        self.durga_environment_id = environment_id;
        self
    }

    /// Returns the configured broker list.
    pub fn brokers(&self) -> &str {
        &self.brokers
    }

    /// Returns the configured consumer group id.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Returns the subscribed process-event topics.
    pub fn topics(&self) -> &[String] {
        &self.topics
    }

    /// Returns the poll timeout.
    pub fn poll_timeout(&self) -> Duration {
        self.poll_timeout
    }

    /// Returns the configured payload format.
    pub fn payload_format(&self) -> KafkaPayloadFormat {
        self.payload_format
    }

    fn validate(&self) -> Result<(), KafkaClientError> {
        if self.brokers.trim().is_empty() {
            return Err(KafkaClientError::InvalidConfig(
                "brokers must not be empty".to_string(),
            ));
        }
        if self.group_id.trim().is_empty() {
            return Err(KafkaClientError::InvalidConfig(
                "group id must not be empty".to_string(),
            ));
        }
        if self.topics.is_empty() || self.topics.iter().any(|topic| topic.trim().is_empty()) {
            return Err(KafkaClientError::InvalidConfig(
                "at least one non-empty topic is required".to_string(),
            ));
        }
        if self.auto_offset_reset.trim().is_empty() {
            return Err(KafkaClientError::InvalidConfig(
                "auto.offset.reset must not be empty".to_string(),
            ));
        }
        Ok(())
    }

    fn decode_event(
        &self,
        topic: &str,
        offset: i64,
        payload: &[u8],
        compat: &mut DurgaCompatibilityState,
    ) -> Result<PipelineEvent, KafkaClientError> {
        match self.payload_format {
            KafkaPayloadFormat::VannakBinary => Ok(decode_pipeline_event(payload)?),
            KafkaPayloadFormat::DurgaJson => {
                let event = decode_durga_json_process_event(payload, compat)?;
                Ok(event.into_pipeline_event(
                    SourceId::from(format!("{}:{}", self.durga_source_id.as_str(), topic)),
                    SourceSequence(offset.max(0) as u64),
                    self.durga_tenant_id.clone(),
                    self.durga_environment_id.clone(),
                ))
            }
        }
    }
}

/// Supported Kafka process-event payload encodings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KafkaPayloadFormat {
    /// Vannak's internal binary process-event codec.
    VannakBinary,
    /// Durga's JSON process-event shape.
    DurgaJson,
}

impl KafkaPayloadFormat {
    /// Parses a CLI payload format name.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "vannak-binary" => Some(Self::VannakBinary),
            "durga-json" => Some(Self::DurgaJson),
            _ => None,
        }
    }

    /// Returns the CLI payload format name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::VannakBinary => "vannak-binary",
            Self::DurgaJson => "durga-json",
        }
    }
}

/// Concrete Kafka process-event consumer.
pub struct KafkaProcessConsumer {
    consumer: BaseConsumer<KafkaConsumerContext>,
    config: KafkaProcessConsumerConfig,
    pending: VecDeque<KafkaPendingRecord>,
    paused: BTreeSet<KafkaTopicPartition>,
    rebalance_state: Arc<Mutex<KafkaRebalanceState>>,
    durga_compat: DurgaCompatibilityState,
    total_polled: u64,
    total_accepted: u64,
}

impl KafkaProcessConsumer {
    /// Creates and subscribes a Kafka process-event consumer.
    pub fn start(config: KafkaProcessConsumerConfig) -> Result<Self, KafkaClientError> {
        config.validate()?;
        let rebalance_state = Arc::new(Mutex::new(KafkaRebalanceState::default()));
        let context = KafkaConsumerContext {
            rebalance_state: Arc::clone(&rebalance_state),
        };
        let consumer = BaseConsumer::from_config_and_context(
            ClientConfig::new()
                .set("bootstrap.servers", config.brokers())
                .set("group.id", config.group_id())
                .set("enable.auto.commit", "false")
                .set("auto.offset.reset", &config.auto_offset_reset)
                .set(
                    "enable.partition.eof",
                    if config.enable_partition_eof {
                        "true"
                    } else {
                        "false"
                    },
                ),
            context,
        )?;
        let topics = config.topics.iter().map(String::as_str).collect::<Vec<_>>();
        consumer.subscribe(&topics)?;
        Ok(Self {
            consumer,
            config,
            pending: VecDeque::new(),
            paused: BTreeSet::new(),
            rebalance_state,
            durga_compat: DurgaCompatibilityState::default(),
            total_polled: 0,
            total_accepted: 0,
        })
    }

    /// Returns an owned snapshot of Kafka consumer-local state.
    pub fn snapshot(&self) -> KafkaProcessConsumerSnapshot {
        KafkaProcessConsumerSnapshot {
            pending_records: self.pending.len(),
            pending_partitions: self
                .pending
                .iter()
                .map(|pending| pending.topic_partition().clone())
                .collect(),
            paused_partitions: self.paused.iter().cloned().collect(),
            rebalance: lock_rebalance_state(&self.rebalance_state).snapshot(),
            durga_compat: self.durga_compat.snapshot(),
            total_polled: self.total_polled,
            total_accepted: self.total_accepted,
        }
    }

    /// Polls at most one Kafka record and submits it to Sitas.
    ///
    /// On success, the consumed message is committed synchronously after the
    /// record has crossed the optional journal and Sitas mailbox boundary.
    pub fn poll_once(
        &mut self,
        runtime: &SitasShardRuntime,
        journal: Option<&mut ProcessEventJournal>,
    ) -> Result<Option<KafkaSubmitOutcome>, KafkaClientError> {
        self.apply_rebalance_state()?;
        if !self.pending.is_empty() {
            return self.retry_pending(runtime, journal);
        }

        let Some(result) = self.consumer.poll(self.config.poll_timeout) else {
            return Ok(None);
        };
        self.total_polled += 1;
        let message = result?;
        let payload = message.payload().ok_or(KafkaClientError::MissingPayload)?;
        let event = self
            .config
            .decode_event(message.topic(), message.offset(), payload, &mut self.durga_compat)?;
        let record = KafkaProcessRecord::new(
            KafkaTopicPartition::new(message.topic(), message.partition()),
            KafkaOffset(message.offset()),
            event,
        );
        self.pending.push_back(KafkaPendingRecord::new(record));
        self.retry_pending(runtime, journal)
    }

    /// Polls records until `max_records` have been accepted or a poll returns no
    /// record.
    pub fn poll_batch(
        &mut self,
        runtime: &SitasShardRuntime,
        mut journal: Option<&mut ProcessEventJournal>,
        max_records: usize,
    ) -> Result<Vec<KafkaSubmitOutcome>, KafkaClientError> {
        let mut outcomes = Vec::new();
        for _ in 0..max_records {
            let outcome = match journal.as_deref_mut() {
                Some(journal) => self.poll_once(runtime, Some(journal))?,
                None => self.poll_once(runtime, None)?,
            };
            let Some(outcome) = outcome else {
                break;
            };
            outcomes.push(outcome);
        }
        Ok(outcomes)
    }

    fn retry_pending(
        &mut self,
        runtime: &SitasShardRuntime,
        journal: Option<&mut ProcessEventJournal>,
    ) -> Result<Option<KafkaSubmitOutcome>, KafkaClientError> {
        let Some(mut pending) = self.pending.pop_front() else {
            return Ok(None);
        };

        let outcome = try_submit_kafka_pending_record(runtime, journal, &mut pending)?;
        let Some(outcome) = outcome else {
            let topic_partition = pending.topic_partition().clone();
            self.pending.push_front(pending);
            self.pause_partition(&topic_partition)?;
            return Ok(None);
        };

        self.resume_partition(&outcome.topic_partition)?;
        self.commit_offset(&outcome.topic_partition, outcome.next_commit_offset)?;
        self.total_accepted += 1;
        Ok(Some(outcome))
    }

    fn apply_rebalance_state(&mut self) -> Result<(), KafkaClientError> {
        let revoked = {
            let mut state = lock_rebalance_state(&self.rebalance_state);
            state.take_unhandled_revoked()
        };
        if revoked.is_empty() {
            return Ok(());
        }

        self.paused
            .retain(|topic_partition| !revoked.contains(topic_partition));

        if let Some(pending) = self
            .pending
            .iter()
            .find(|pending| revoked.contains(pending.topic_partition()))
        {
            return Err(KafkaClientError::PendingPartitionRevoked {
                topic_partition: pending.topic_partition().clone(),
                offset: pending.offset(),
            });
        }
        Ok(())
    }

    fn pause_partition(
        &mut self,
        topic_partition: &KafkaTopicPartition,
    ) -> Result<(), KafkaClientError> {
        if !self.paused.insert(topic_partition.clone()) {
            return Ok(());
        }
        let list = topic_partition_list(topic_partition, None)?;
        self.consumer.pause(&list)?;
        Ok(())
    }

    fn resume_partition(
        &mut self,
        topic_partition: &KafkaTopicPartition,
    ) -> Result<(), KafkaClientError> {
        if !self.paused.remove(topic_partition) {
            return Ok(());
        }
        let list = topic_partition_list(topic_partition, None)?;
        self.consumer.resume(&list)?;
        Ok(())
    }

    fn commit_offset(
        &self,
        topic_partition: &KafkaTopicPartition,
        offset: KafkaOffset,
    ) -> Result<(), KafkaClientError> {
        let list = topic_partition_list(topic_partition, Some(offset))?;
        self.consumer
            .commit(&list, CommitMode::Sync)
            .map_err(KafkaClientError::Commit)
    }
}

#[derive(Debug, Clone, Default)]
struct KafkaConsumerContext {
    rebalance_state: Arc<Mutex<KafkaRebalanceState>>,
}

impl ClientContext for KafkaConsumerContext {}

impl ConsumerContext for KafkaConsumerContext {
    fn pre_rebalance(&self, rebalance: &Rebalance<'_>) {
        lock_rebalance_state(&self.rebalance_state).record_pre_rebalance(rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance<'_>) {
        lock_rebalance_state(&self.rebalance_state).record_post_rebalance(rebalance);
    }
}

#[derive(Debug, Clone, Default)]
struct KafkaRebalanceState {
    assigned_partitions: BTreeSet<KafkaTopicPartition>,
    revoked_partitions: Vec<KafkaTopicPartition>,
    pending_revoked_partitions: Vec<KafkaTopicPartition>,
    pre_rebalance_count: u64,
    post_rebalance_count: u64,
    assignment_count: u64,
    revocation_count: u64,
    error_count: u64,
    last_event: Option<KafkaRebalanceEvent>,
}

impl KafkaRebalanceState {
    fn record_pre_rebalance(&mut self, rebalance: &Rebalance<'_>) {
        self.pre_rebalance_count += 1;
        self.record_rebalance(rebalance);
    }

    fn record_post_rebalance(&mut self, rebalance: &Rebalance<'_>) {
        self.post_rebalance_count += 1;
        if let Rebalance::Assign(partitions) = rebalance {
            self.assigned_partitions = topic_partitions(partitions).into_iter().collect();
        }
    }

    fn record_rebalance(&mut self, rebalance: &Rebalance<'_>) {
        match rebalance {
            Rebalance::Assign(partitions) => {
                self.assignment_count += 1;
                let partitions = topic_partitions(partitions);
                self.last_event = Some(KafkaRebalanceEvent::Assigned(partitions));
            }
            Rebalance::Revoke(partitions) => {
                self.revocation_count += 1;
                let partitions = topic_partitions(partitions);
                for partition in &partitions {
                    self.assigned_partitions.remove(partition);
                }
                self.revoked_partitions.extend(partitions.iter().cloned());
                self.pending_revoked_partitions
                    .extend(partitions.iter().cloned());
                self.last_event = Some(KafkaRebalanceEvent::Revoked(partitions));
            }
            Rebalance::Error(error) => {
                self.error_count += 1;
                self.last_event = Some(KafkaRebalanceEvent::Error(error.to_string()));
            }
        }
    }

    fn take_unhandled_revoked(&mut self) -> Vec<KafkaTopicPartition> {
        std::mem::take(&mut self.pending_revoked_partitions)
    }

    fn snapshot(&self) -> KafkaRebalanceSnapshot {
        KafkaRebalanceSnapshot {
            assigned_partitions: self.assigned_partitions.iter().cloned().collect(),
            revoked_partitions: self.revoked_partitions.clone(),
            pre_rebalance_count: self.pre_rebalance_count,
            post_rebalance_count: self.post_rebalance_count,
            assignment_count: self.assignment_count,
            revocation_count: self.revocation_count,
            error_count: self.error_count,
            last_event: self.last_event.clone(),
        }
    }
}

/// Owned snapshot of Kafka process-consumer state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaProcessConsumerSnapshot {
    /// Number of decoded Kafka records waiting for Sitas admission.
    pub pending_records: usize,
    /// Topic/partition identities for pending records.
    pub pending_partitions: Vec<KafkaTopicPartition>,
    /// Kafka partitions currently paused by Vannak due to Sitas backpressure.
    pub paused_partitions: Vec<KafkaTopicPartition>,
    /// Rebalance callback state.
    pub rebalance: KafkaRebalanceSnapshot,
    /// Live Durga schema compatibility tracking.
    pub durga_compat: DurgaCompatibilitySnapshot,
    /// Total records polled from Kafka.
    pub total_polled: u64,
    /// Total records accepted after journal + Sitas admission.
    pub total_accepted: u64,
}

/// Owned snapshot of Kafka rebalance callback state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaRebalanceSnapshot {
    /// Partitions assigned by the latest assignment callback.
    pub assigned_partitions: Vec<KafkaTopicPartition>,
    /// Partitions seen in revocation callbacks.
    pub revoked_partitions: Vec<KafkaTopicPartition>,
    /// Number of pre-rebalance callbacks observed.
    pub pre_rebalance_count: u64,
    /// Number of post-rebalance callbacks observed.
    pub post_rebalance_count: u64,
    /// Number of assignment callbacks observed.
    pub assignment_count: u64,
    /// Number of revocation callbacks observed.
    pub revocation_count: u64,
    /// Number of rebalance error callbacks observed.
    pub error_count: u64,
    /// Last observed rebalance event.
    pub last_event: Option<KafkaRebalanceEvent>,
}

/// Last observed Kafka rebalance event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KafkaRebalanceEvent {
    /// Partitions assigned to this consumer.
    Assigned(Vec<KafkaTopicPartition>),
    /// Partitions revoked from this consumer.
    Revoked(Vec<KafkaTopicPartition>),
    /// Rebalance error text.
    Error(String),
}

fn topic_partitions(partitions: &TopicPartitionList) -> Vec<KafkaTopicPartition> {
    partitions
        .elements()
        .into_iter()
        .map(|partition| KafkaTopicPartition::new(partition.topic(), partition.partition()))
        .collect()
}

fn lock_rebalance_state(
    state: &Arc<Mutex<KafkaRebalanceState>>,
) -> MutexGuard<'_, KafkaRebalanceState> {
    state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn topic_partition_list(
    topic_partition: &KafkaTopicPartition,
    offset: Option<KafkaOffset>,
) -> Result<TopicPartitionList, KafkaClientError> {
    let mut list = TopicPartitionList::new();
    match offset {
        Some(offset) => list.add_partition_offset(
            topic_partition.topic(),
            topic_partition.partition(),
            Offset::Offset(offset.0),
        )?,
        None => {
            list.add_partition(topic_partition.topic(), topic_partition.partition());
        }
    }
    Ok(list)
}

/// Errors returned by the concrete Kafka consumer.
#[derive(Debug)]
pub enum KafkaClientError {
    /// Local consumer configuration is invalid.
    InvalidConfig(String),
    /// Kafka client operation failed.
    Kafka(KafkaError),
    /// Offset commit failed.
    Commit(KafkaError),
    /// Kafka record had no payload.
    MissingPayload,
    /// Kafka payload could not be decoded as a Vannak process event.
    Decode(ProcessEventDecodeError),
    /// Durga JSON payload could not be decoded.
    DurgaJson(serde_json::Error),
    /// Durable/Sitas admission failed.
    Ingest(KafkaIngestError),
    /// Kafka revoked a partition while a record from it was still uncommitted.
    PendingPartitionRevoked {
        /// Revoked topic/partition.
        topic_partition: KafkaTopicPartition,
        /// Uncommitted source offset.
        offset: KafkaOffset,
    },
}

impl fmt::Display for KafkaClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(message) => write!(f, "invalid Kafka consumer config: {message}"),
            Self::Kafka(error) => write!(f, "{error}"),
            Self::Commit(error) => write!(f, "Kafka offset commit failed: {error}"),
            Self::MissingPayload => f.write_str("Kafka process-event record has no payload"),
            Self::Decode(error) => write!(f, "{error}"),
            Self::DurgaJson(error) => write!(f, "Durga JSON payload decode failed: {error}"),
            Self::Ingest(error) => write!(f, "{error}"),
            Self::PendingPartitionRevoked {
                topic_partition,
                offset,
            } => write!(
                f,
                "Kafka partition {}:{} was revoked with uncommitted pending offset {}",
                topic_partition.topic(),
                topic_partition.partition(),
                offset.0
            ),
        }
    }
}

impl std::error::Error for KafkaClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Kafka(error) | Self::Commit(error) => Some(error),
            Self::Decode(error) => Some(error),
            Self::DurgaJson(error) => Some(error),
            Self::Ingest(error) => Some(error),
            Self::InvalidConfig(_)
            | Self::MissingPayload
            | Self::PendingPartitionRevoked { .. } => None,
        }
    }
}

impl From<KafkaError> for KafkaClientError {
    fn from(error: KafkaError) -> Self {
        Self::Kafka(error)
    }
}

impl From<ProcessEventDecodeError> for KafkaClientError {
    fn from(error: ProcessEventDecodeError) -> Self {
        Self::Decode(error)
    }
}

impl From<serde_json::Error> for KafkaClientError {
    fn from(error: serde_json::Error) -> Self {
        Self::DurgaJson(error)
    }
}

impl From<KafkaIngestError> for KafkaClientError {
    fn from(error: KafkaIngestError) -> Self {
        Self::Ingest(error)
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DurgaProcessEventJson {
    process_instance_id: String,
    process_id: String,
    activity_id: Option<String>,
    token_id: Option<String>,
    correlation_id: Option<String>,
    payload: Option<serde_json::Value>,
    status: String,
    error: Option<DurgaErrorInfoJson>,
    event_type: String,
    process_version: Option<String>,
    business_key: Option<String>,
    timestamp: String,
    #[serde(default)]
    schema_version: Option<String>,
    #[serde(default)]
    metadata_refs: Vec<MetadataRefJson>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MetadataRefJson {
    kind: String,
    id: String,
    version: Option<String>,
}

impl MetadataRefJson {
    fn into_metadata_ref(self) -> Option<MetadataRef> {
        match self.kind.as_str() {
            "dataset" => Some(MetadataRef::Dataset(crate::metadata::DatasetId::from(
                self.id,
            ))),
            "schema" => Some(MetadataRef::Schema {
                id: crate::metadata::SchemaId::from(self.id),
                version: self.version.map(crate::metadata::MetadataVersion::from),
            }),
            "field" => Some(MetadataRef::Field(crate::metadata::FieldId::from(self.id))),
            "pipelineDefinition" => Some(MetadataRef::PipelineDefinition {
                id: crate::metadata::PipelineDefinitionId::from(self.id),
                version: self.version.map(crate::metadata::MetadataVersion::from),
            }),
            "object" => Some(MetadataRef::Object(
                crate::metadata::MetadataObjectId::from(self.id),
            )),
            "lineageEdge" => Some(MetadataRef::LineageEdge(
                crate::metadata::LineageEdgeId::from(self.id),
            )),
            "dataContract" => Some(MetadataRef::DataContract(
                crate::metadata::DataContractId::from(self.id),
            )),
            "owner" => Some(MetadataRef::Owner(crate::metadata::OwnerId::from(
                self.id,
            ))),
            "classification" => Some(MetadataRef::Classification(
                crate::metadata::ClassificationId::from(self.id),
            )),
            _ => None,
        }
    }
}

#[derive(Debug, Deserialize)]
struct DurgaErrorInfoJson {
    message: String,
    code: Option<String>,
}

fn decode_durga_json_process_event(
    payload: &[u8],
    compat: &mut DurgaCompatibilityState,
) -> Result<DurgaProcessEvent, KafkaClientError> {
    let value: DurgaProcessEventJson = serde_json::from_slice(payload)?;
    let metadata_refs: Vec<MetadataRef> = value
        .metadata_refs
        .into_iter()
        .filter_map(MetadataRefJson::into_metadata_ref)
        .collect();
    Ok(DurgaProcessEvent {
        process_instance_id: value.process_instance_id,
        process_id: value.process_id,
        activity_id: value.activity_id,
        token_id: value.token_id,
        correlation_id: value.correlation_id,
        payload: value.payload.map(|payload| payload.to_string()),
        status: parse_durga_status(&value.status, compat),
        error: value.error.map(|error| DurgaErrorInfo {
            message: error.message,
            code: error.code,
        }),
        event_type: parse_durga_event_type(&value.event_type, compat),
        process_version: value.process_version,
        business_key: value.business_key,
        timestamp: value.timestamp,
        metadata_refs,
        schema_version: value.schema_version,
    })
}

fn parse_durga_status(value: &str, compat: &mut DurgaCompatibilityState) -> DurgaStatus {
    match value {
        "Started" | "STARTED" => DurgaStatus::Started,
        "Completed" | "COMPLETED" => DurgaStatus::Completed,
        "Failed" | "FAILED" => DurgaStatus::Failed,
        "Escalated" | "ESCALATED" => DurgaStatus::Escalated,
        "Cancelled" | "CANCELLED" | "Canceled" | "CANCELED" => DurgaStatus::Cancelled,
        _ => {
            compat.record_unknown_status(value);
            DurgaStatus::Unknown(value.to_string())
        }
    }
}

fn parse_durga_event_type(value: &str, compat: &mut DurgaCompatibilityState) -> DurgaEventType {
    match value {
        "ProcessStarted" | "PROCESS_STARTED" => DurgaEventType::ProcessStarted,
        "ActivityEntered" | "ACTIVITY_ENTERED" => DurgaEventType::ActivityEntered,
        "ActivityCompleted" | "ACTIVITY_COMPLETED" => DurgaEventType::ActivityCompleted,
        "ActivityEscalated" | "ACTIVITY_ESCALATED" => DurgaEventType::ActivityEscalated,
        "ActivityCancelled" | "ACTIVITY_CANCELLED" | "ActivityCanceled" | "ACTIVITY_CANCELED" => {
            DurgaEventType::ActivityCancelled
        }
        "GatewayTaken" | "GATEWAY_TAKEN" => DurgaEventType::GatewayTaken,
        "ProcessCompleted" | "PROCESS_COMPLETED" => DurgaEventType::ProcessCompleted,
        "ProcessFailed" | "PROCESS_FAILED" => DurgaEventType::ProcessFailed,
        _ => {
            compat.record_unknown_event_type(value);
            DurgaEventType::Unknown(value.to_string())
        }
    }
}

/// Accumulates live Durga schema compatibility counters over one consumer session.
#[derive(Debug, Clone, Default)]
struct DurgaCompatibilityState {
    unknown_status_counts: BTreeMap<String, u64>,
    unknown_event_type_counts: BTreeMap<String, u64>,
}

impl DurgaCompatibilityState {
    fn record_unknown_status(&mut self, value: &str) {
        *self
            .unknown_status_counts
            .entry(value.to_string())
            .or_default() += 1;
    }

    fn record_unknown_event_type(&mut self, value: &str) {
        *self
            .unknown_event_type_counts
            .entry(value.to_string())
            .or_default() += 1;
    }

    fn snapshot(&self) -> DurgaCompatibilitySnapshot {
        fn sorted(counts: &BTreeMap<String, u64>) -> Vec<(String, u64)> {
            counts.iter().map(|(k, v)| (k.clone(), *v)).collect()
        }
        DurgaCompatibilitySnapshot {
            unknown_status_values: sorted(&self.unknown_status_counts),
            unknown_event_type_values: sorted(&self.unknown_event_type_counts),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consumer_config_rejects_missing_required_fields() {
        assert!(matches!(
            KafkaProcessConsumerConfig::new("", "group", ["topic"]).validate(),
            Err(KafkaClientError::InvalidConfig(_))
        ));
        assert!(matches!(
            KafkaProcessConsumerConfig::new("localhost:9092", "", ["topic"]).validate(),
            Err(KafkaClientError::InvalidConfig(_))
        ));
        assert!(matches!(
            KafkaProcessConsumerConfig::new("localhost:9092", "group", Vec::<String>::new())
                .validate(),
            Err(KafkaClientError::InvalidConfig(_))
        ));
    }

    #[test]
    fn durga_json_payload_decodes_to_pipeline_event() {
        let config = KafkaProcessConsumerConfig::new("localhost:9092", "group", ["topic-a"])
            .with_payload_format(KafkaPayloadFormat::DurgaJson)
            .with_durga_context(
                SourceId::from("durga-kafka"),
                TenantId::from("tenant-a"),
                EnvironmentId::from("prod"),
            );
        let mut compat = DurgaCompatibilityState::default();
        let event = config
            .decode_event(
                "process-events-demo",
                42,
                br#"{
                    "processInstanceId": "instance-a",
                    "processId": "pipeline-a",
                    "activityId": "extract",
                    "tokenId": "token-1",
                    "correlationId": "corr-1",
                    "payload": {"data": "value"},
                    "status": "STARTED",
                    "eventType": "ACTIVITY_ENTERED",
                    "processVersion": "v1",
                    "businessKey": "business-1",
                    "timestamp": "2026-06-30T10:00:00Z"
                }"#,
                &mut compat,
            )
            .unwrap();

        assert_eq!(
            event.source_id().as_str(),
            "durga-kafka:process-events-demo"
        );
        assert_eq!(event.source_sequence().0, 42);
        assert_eq!(event.pipeline_id().as_str(), "pipeline-a");
        assert_eq!(event.process_instance_id().as_str(), "instance-a");
        assert_eq!(event.activity_id().unwrap().as_str(), "extract");
        assert_eq!(event.payload().unwrap(), r#"{"data":"value"}"#);
    }

    #[test]
    fn rebalance_state_records_assignment_and_revocation() {
        let mut state = KafkaRebalanceState::default();
        let mut assigned = TopicPartitionList::new();
        assigned.add_partition("topic-a", 0);
        assigned.add_partition("topic-a", 1);

        state.record_pre_rebalance(&Rebalance::Assign(&assigned));
        state.record_post_rebalance(&Rebalance::Assign(&assigned));

        let snapshot = state.snapshot();
        assert_eq!(snapshot.pre_rebalance_count, 1);
        assert_eq!(snapshot.post_rebalance_count, 1);
        assert_eq!(snapshot.assignment_count, 1);
        assert_eq!(
            snapshot.assigned_partitions,
            vec![
                KafkaTopicPartition::new("topic-a", 0),
                KafkaTopicPartition::new("topic-a", 1)
            ]
        );
        assert_eq!(
            snapshot.last_event,
            Some(KafkaRebalanceEvent::Assigned(vec![
                KafkaTopicPartition::new("topic-a", 0),
                KafkaTopicPartition::new("topic-a", 1)
            ]))
        );

        let mut revoked = TopicPartitionList::new();
        revoked.add_partition("topic-a", 1);
        state.record_pre_rebalance(&Rebalance::Revoke(&revoked));

        let snapshot = state.snapshot();
        assert_eq!(snapshot.revocation_count, 1);
        assert_eq!(
            snapshot.assigned_partitions,
            vec![KafkaTopicPartition::new("topic-a", 0)]
        );
        assert_eq!(
            state.take_unhandled_revoked(),
            vec![KafkaTopicPartition::new("topic-a", 1)]
        );
        assert!(state.take_unhandled_revoked().is_empty());
    }

    #[test]
    fn durga_json_unknown_status_parsed_tolerantly_and_tracked() {
        let config = KafkaProcessConsumerConfig::new("localhost:9092", "group", ["topic-a"])
            .with_payload_format(KafkaPayloadFormat::DurgaJson)
            .with_durga_context(
                SourceId::from("durga-kafka"),
                TenantId::from("tenant-a"),
                EnvironmentId::from("prod"),
            );
        let mut compat = DurgaCompatibilityState::default();
        let _event = config
            .decode_event(
                "topic-a",
                1,
                br#"{
                    "processInstanceId": "instance-a",
                    "processId": "pipeline-a",
                    "status": "PENDING",
                    "eventType": "ACTIVITY_ENTERED",
                    "timestamp": "2026-06-30T10:00:00Z"
                }"#,
                &mut compat,
            )
            .unwrap();

        let snapshot = compat.snapshot();
        assert!(
            snapshot
                .unknown_status_values
                .iter()
                .any(|(val, _)| val == "PENDING"),
            "PENDING should be tracked as unknown status"
        );
    }

    #[test]
    fn durga_json_unknown_event_type_parsed_tolerantly_and_tracked() {
        let config = KafkaProcessConsumerConfig::new("localhost:9092", "group", ["topic-a"])
            .with_payload_format(KafkaPayloadFormat::DurgaJson)
            .with_durga_context(
                SourceId::from("durga-kafka"),
                TenantId::from("tenant-a"),
                EnvironmentId::from("prod"),
            );
        let mut compat = DurgaCompatibilityState::default();
        let _event = config
            .decode_event(
                "topic-a",
                1,
                br#"{
                    "processInstanceId": "instance-a",
                    "processId": "pipeline-a",
                    "status": "STARTED",
                    "eventType": "CUSTOM_TASK_STARTED",
                    "timestamp": "2026-06-30T10:00:00Z"
                }"#,
                &mut compat,
            )
            .unwrap();

        let snapshot = compat.snapshot();
        assert!(
            snapshot
                .unknown_event_type_values
                .iter()
                .any(|(val, _)| val == "CUSTOM_TASK_STARTED"),
            "CUSTOM_TASK_STARTED should be tracked as unknown event type"
        );
    }

    #[test]
    fn durga_json_unknown_values_accumulate_counts() {
        let config = KafkaProcessConsumerConfig::new("localhost:9092", "group", ["topic-a"])
            .with_payload_format(KafkaPayloadFormat::DurgaJson)
            .with_durga_context(
                SourceId::from("durga-kafka"),
                TenantId::from("tenant-a"),
                EnvironmentId::from("prod"),
            );
        let mut compat = DurgaCompatibilityState::default();

        config
            .decode_event(
                "topic-a",
                1,
                br#"{
                    "processInstanceId": "instance-a",
                    "processId": "pipeline-a",
                    "status": "PENDING",
                    "eventType": "ACTIVITY_ENTERED",
                    "timestamp": "2026-06-30T10:00:00Z"
                }"#,
                &mut compat,
            )
            .unwrap();
        config
            .decode_event(
                "topic-a",
                2,
                br#"{
                    "processInstanceId": "instance-a",
                    "processId": "pipeline-a",
                    "status": "PENDING",
                    "eventType": "ACTIVITY_ENTERED",
                    "timestamp": "2026-06-30T10:00:01Z"
                }"#,
                &mut compat,
            )
            .unwrap();

        let snapshot = compat.snapshot();
        let (_, count) = snapshot
            .unknown_status_values
            .iter()
            .find(|(val, _)| val == "PENDING")
            .unwrap();
        assert_eq!(*count, 2, "PENDING should be counted twice");
    }

    #[test]
    fn durga_json_metadata_refs_deserialized() {
        let config = KafkaProcessConsumerConfig::new("localhost:9092", "group", ["topic-a"])
            .with_payload_format(KafkaPayloadFormat::DurgaJson)
            .with_durga_context(
                SourceId::from("durga-kafka"),
                TenantId::from("tenant-a"),
                EnvironmentId::from("prod"),
            );
        let mut compat = DurgaCompatibilityState::default();
        let event = config
            .decode_event(
                "topic-a",
                1,
                br#"{
                    "processInstanceId": "instance-a",
                    "processId": "pipeline-a",
                    "status": "STARTED",
                    "eventType": "ACTIVITY_ENTERED",
                    "timestamp": "2026-06-30T10:00:00Z",
                    "metadataRefs": [
                        {"kind": "dataset", "id": "dataset-a"},
                        {"kind": "schema", "id": "schema-a", "version": "v1"},
                        {"kind": "classification", "id": "pii"}
                    ]
                }"#,
                &mut compat,
            )
            .unwrap();

        let refs = event.metadata_refs();
        assert_eq!(
            refs.len(),
            3,
            "all three metadata refs should be parsed"
        );
    }

    #[test]
    fn durga_json_schema_version_deserialized() {
        let config = KafkaProcessConsumerConfig::new("localhost:9092", "group", ["topic-a"])
            .with_payload_format(KafkaPayloadFormat::DurgaJson)
            .with_durga_context(
                SourceId::from("durga-kafka"),
                TenantId::from("tenant-a"),
                EnvironmentId::from("prod"),
            );
        let mut compat = DurgaCompatibilityState::default();
        let event = config
            .decode_event(
                "topic-a",
                1,
                br#"{
                    "processInstanceId": "instance-a",
                    "processId": "pipeline-a",
                    "status": "STARTED",
                    "eventType": "ACTIVITY_ENTERED",
                    "timestamp": "2026-06-30T10:00:00Z",
                    "schemaVersion": "2.1.0"
                }"#,
                &mut compat,
            )
            .unwrap();

        // Verify the event decoded successfully (schema version tracked internally)
        assert_eq!(event.pipeline_id().as_str(), "pipeline-a");
    }

    #[test]
    fn empty_compatibility_state_produces_empty_snapshot() {
        let compat = DurgaCompatibilityState::default();
        let snapshot = compat.snapshot();
        assert!(snapshot.unknown_status_values.is_empty());
        assert!(snapshot.unknown_event_type_values.is_empty());
    }
}
