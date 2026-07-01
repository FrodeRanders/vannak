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

//! Ipto placement, mapping, and durable outbox primitives.
//!
//! This module is still dependency-free. A later adapter can translate
//! `IptoWritePayload` into calls against the Rust Ipto repository API or direct
//! PostgreSQL-backed repositories.

use crate::NodeId;
use crate::cluster::{CheckpointEpoch, MetadataOutboxCheckpoint};
use crate::data::{
    DataIndividualMetadataEvent, DataIndividualShardId, IdempotencyKey, MetadataFieldName,
    MetadataValue,
};
use crate::ingest::EventTimestamp;
use crate::storage::{
    RecordOffset, SegmentError, SegmentId, SegmentManifest, SegmentReader, SegmentWriter,
};
use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IptoInstanceId(String);

impl IptoInstanceId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for IptoInstanceId {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for IptoInstanceId {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl fmt::Display for IptoInstanceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IptoAttributeName(String);

impl IptoAttributeName {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for IptoAttributeName {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for IptoAttributeName {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

/// Versioned mapping from Vannak metadata field names to Ipto attributes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IptoMapping {
    version: String,
    fields: BTreeMap<MetadataFieldName, IptoAttributeName>,
    relations_enabled: bool,
}

impl IptoMapping {
    pub fn new(version: impl Into<String>) -> Self {
        Self {
            version: version.into(),
            fields: BTreeMap::new(),
            relations_enabled: true,
        }
    }

    pub fn map_field(
        mut self,
        field: impl Into<MetadataFieldName>,
        attribute: impl Into<IptoAttributeName>,
    ) -> Self {
        self.fields.insert(field.into(), attribute.into());
        self
    }

    pub fn without_relations(mut self) -> Self {
        self.relations_enabled = false;
        self
    }

    pub fn version(&self) -> &str {
        &self.version
    }

    pub fn attribute_for(&self, field: &MetadataFieldName) -> Option<&IptoAttributeName> {
        self.fields.get(field)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IptoWritePayload {
    pub target: IptoInstanceId,
    pub shard_id: DataIndividualShardId,
    pub idempotency_key: IdempotencyKey,
    pub mapping_version: String,
    pub attributes: BTreeMap<IptoAttributeName, MetadataValue>,
}

impl IptoWritePayload {
    pub fn from_event(
        event: &DataIndividualMetadataEvent,
        target: &IptoInstanceId,
        mapping: &IptoMapping,
    ) -> Self {
        let mut attributes = BTreeMap::new();
        for (field, value) in event.passive_metadata().fields() {
            if let Some(attribute) = mapping.attribute_for(field) {
                attributes.insert(attribute.clone(), value.clone());
            }
        }
        for (field, value) in event.active_metadata().fields() {
            if let Some(attribute) = mapping.attribute_for(field) {
                attributes.insert(attribute.clone(), value.clone());
            }
        }

        // Sentinel PROV-O relation attributes from event identity fields.
        if mapping.relations_enabled {
            if let Some(ref activity_id) = event.activity_id() {
                attributes.insert(
                    IptoAttributeName::from("vannak:relation:wasGeneratedBy"),
                    MetadataValue::string(activity_id.as_str()),
                );
            }
        }

        Self {
            target: target.clone(),
            shard_id: event.data_individual_shard_id(),
            idempotency_key: event.idempotency_key().clone(),
            mapping_version: mapping.version().to_string(),
            attributes,
        }
    }

    /// Encodes this payload for append-only outbox segment storage.
    ///
    /// This is a small stable binary format, not a general serialization
    /// framework. It exists so outbox replay can be implemented before choosing
    /// JSON, serde, or a direct Ipto wire format.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        write_string(&mut out, self.target.as_str());
        write_u64(&mut out, self.shard_id.0);
        write_string(&mut out, self.idempotency_key.as_str());
        write_string(&mut out, &self.mapping_version);
        write_u32(&mut out, self.attributes.len() as u32);
        for (attribute, value) in &self.attributes {
            write_string(&mut out, attribute.as_str());
            write_metadata_value(&mut out, value);
        }
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, IptoPayloadDecodeError> {
        let mut cursor = DecodeCursor::new(bytes);
        let target = IptoInstanceId::from(cursor.read_string()?);
        let shard_id = DataIndividualShardId(cursor.read_u64()?);
        let idempotency_key = IdempotencyKey::from(cursor.read_string()?);
        let mapping_version = cursor.read_string()?;
        let attribute_count = cursor.read_u32()? as usize;
        let mut attributes = BTreeMap::new();
        for _ in 0..attribute_count {
            let attribute = IptoAttributeName::from(cursor.read_string()?);
            let value = cursor.read_metadata_value()?;
            attributes.insert(attribute, value);
        }
        cursor.finish()?;

        Ok(Self {
            target,
            shard_id,
            idempotency_key,
            mapping_version,
            attributes,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IptoPayloadDecodeError {
    UnexpectedEof,
    InvalidUtf8,
    InvalidValueTag(u8),
    TrailingBytes(usize),
}

impl fmt::Display for IptoPayloadDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedEof => f.write_str("unexpected end of Ipto payload bytes"),
            Self::InvalidUtf8 => f.write_str("Ipto payload contains invalid UTF-8"),
            Self::InvalidValueTag(tag) => write!(f, "invalid Ipto metadata value tag {tag}"),
            Self::TrailingBytes(count) => {
                write!(f, "Ipto payload has {count} trailing undecoded bytes")
            }
        }
    }
}

impl std::error::Error for IptoPayloadDecodeError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutboxStatus {
    Pending,
    Acknowledged,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataOutboxEntry {
    payload: IptoWritePayload,
    status: OutboxStatus,
    record_offset: Option<RecordOffset>,
    retry_count: u32,
    last_error: Option<String>,
}

impl MetadataOutboxEntry {
    pub fn payload(&self) -> &IptoWritePayload {
        &self.payload
    }

    pub fn status(&self) -> OutboxStatus {
        self.status
    }

    pub fn record_offset(&self) -> Option<RecordOffset> {
        self.record_offset
    }

    pub fn retry_count(&self) -> u32 {
        self.retry_count
    }

    pub fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }
}

#[derive(Debug, Default)]
pub struct MetadataOutbox {
    entries: BTreeMap<IdempotencyKey, MetadataOutboxEntry>,
    pending: VecDeque<IdempotencyKey>,
}

impl MetadataOutbox {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn enqueue(&mut self, payload: IptoWritePayload) -> OutboxEnqueueResult {
        self.enqueue_with_offset(payload, None)
    }

    fn enqueue_with_offset(
        &mut self,
        payload: IptoWritePayload,
        record_offset: Option<RecordOffset>,
    ) -> OutboxEnqueueResult {
        let key = payload.idempotency_key.clone();
        if self.entries.contains_key(&key) {
            return OutboxEnqueueResult::Duplicate;
        }

        self.entries.insert(
            key.clone(),
            MetadataOutboxEntry {
                payload,
                status: OutboxStatus::Pending,
                record_offset,
                retry_count: 0,
                last_error: None,
            },
        );
        self.pending.push_back(key);
        OutboxEnqueueResult::Enqueued
    }

    pub fn next_pending(&self) -> Option<&MetadataOutboxEntry> {
        self.pending
            .iter()
            .filter_map(|key| self.entries.get(key))
            .find(|entry| entry.status == OutboxStatus::Pending)
    }

    pub fn next_pending_for_target(&self, target: &IptoInstanceId) -> Option<&MetadataOutboxEntry> {
        self.pending
            .iter()
            .filter_map(|key| self.entries.get(key))
            .find(|entry| entry.status == OutboxStatus::Pending && entry.payload.target == *target)
    }

    pub fn acknowledge(&mut self, key: &IdempotencyKey) -> bool {
        let Some(entry) = self.entries.get_mut(key) else {
            return false;
        };
        entry.status = OutboxStatus::Acknowledged;
        self.pending.retain(|pending_key| pending_key != key);
        true
    }

    pub fn fail(&mut self, key: &IdempotencyKey, error: impl Into<String>) -> bool {
        let Some(entry) = self.entries.get_mut(key) else {
            return false;
        };
        entry.status = OutboxStatus::Failed;
        entry.retry_count += 1;
        entry.last_error = Some(error.into());
        self.pending.retain(|pending_key| pending_key != key);
        true
    }

    pub fn retry_failed(&mut self, key: &IdempotencyKey) -> bool {
        let Some(entry) = self.entries.get_mut(key) else {
            return false;
        };
        if entry.status != OutboxStatus::Failed {
            return false;
        }
        entry.status = OutboxStatus::Pending;
        if !self.pending.iter().any(|pending_key| pending_key == key) {
            self.pending.push_back(key.clone());
        }
        true
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn snapshot(&self) -> MetadataOutboxSnapshot {
        let mut snapshot = MetadataOutboxSnapshot {
            total: self.entries.len(),
            pending: 0,
            acknowledged: 0,
            failed: 0,
            queued_pending: self.pending.len(),
        };

        for entry in self.entries.values() {
            match entry.status {
                OutboxStatus::Pending => snapshot.pending += 1,
                OutboxStatus::Acknowledged => snapshot.acknowledged += 1,
                OutboxStatus::Failed => snapshot.failed += 1,
            }
        }

        snapshot
    }

    fn acknowledged_checkpoint(
        &self,
        data_individual_shard_id: DataIndividualShardId,
        target: &IptoInstanceId,
        segment_id: SegmentId,
        epoch: CheckpointEpoch,
    ) -> Option<MetadataOutboxCheckpoint> {
        self.entries
            .values()
            .filter(|entry| {
                entry.status == OutboxStatus::Acknowledged && entry.payload.target == *target
            })
            .filter_map(|entry| Some((entry.record_offset?, entry.payload.mapping_version.clone())))
            .max_by_key(|(offset, _)| *offset)
            .map(
                |(last_acknowledged_offset, mapping_version)| MetadataOutboxCheckpoint {
                    data_individual_shard_id,
                    target: target.clone(),
                    segment_id,
                    last_acknowledged_offset,
                    mapping_version,
                    epoch,
                },
            )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutboxEnqueueResult {
    Enqueued,
    Duplicate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataOutboxSnapshot {
    pub total: usize,
    pub pending: usize,
    pub acknowledged: usize,
    pub failed: usize,
    pub queued_pending: usize,
}

pub trait IptoWriter {
    fn write(&mut self, payload: &IptoWritePayload) -> Result<(), IptoWriteError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IptoWriteError {
    message: String,
    retryable: bool,
}

impl IptoWriteError {
    pub fn retryable(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: true,
        }
    }

    pub fn permanent(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: false,
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn is_retryable(&self) -> bool {
        self.retryable
    }
}

impl fmt::Display for IptoWriteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.retryable {
            write!(f, "retryable Ipto write error: {}", self.message)
        } else {
            write!(f, "permanent Ipto write error: {}", self.message)
        }
    }
}

impl std::error::Error for IptoWriteError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataOutboxDeliveryResult {
    NoPending,
    Acknowledged {
        idempotency_key: IdempotencyKey,
    },
    Failed {
        idempotency_key: IdempotencyKey,
        retryable: bool,
        message: String,
    },
}

pub fn deliver_next_pending(
    outbox: &mut MetadataOutbox,
    writer: &mut (impl IptoWriter + ?Sized),
) -> MetadataOutboxDeliveryResult {
    let Some(payload) = outbox.next_pending().map(|entry| entry.payload().clone()) else {
        return MetadataOutboxDeliveryResult::NoPending;
    };
    let key = payload.idempotency_key.clone();

    match writer.write(&payload) {
        Ok(()) => {
            outbox.acknowledge(&key);
            MetadataOutboxDeliveryResult::Acknowledged {
                idempotency_key: key,
            }
        }
        Err(error) => {
            let retryable = error.is_retryable();
            let message = error.message().to_string();
            outbox.fail(&key, message.clone());
            MetadataOutboxDeliveryResult::Failed {
                idempotency_key: key,
                retryable,
                message,
            }
        }
    }
}

pub fn deliver_next_pending_for_target(
    outbox: &mut MetadataOutbox,
    target: &IptoInstanceId,
    writer: &mut (impl IptoWriter + ?Sized),
) -> MetadataOutboxDeliveryResult {
    let Some(payload) = outbox
        .next_pending_for_target(target)
        .map(|entry| entry.payload().clone())
    else {
        return MetadataOutboxDeliveryResult::NoPending;
    };
    let key = payload.idempotency_key.clone();

    match writer.write(&payload) {
        Ok(()) => {
            outbox.acknowledge(&key);
            MetadataOutboxDeliveryResult::Acknowledged {
                idempotency_key: key,
            }
        }
        Err(error) => {
            let retryable = error.is_retryable();
            let message = error.message().to_string();
            outbox.fail(&key, message.clone());
            MetadataOutboxDeliveryResult::Failed {
                idempotency_key: key,
                retryable,
                message,
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataOutboxDrainSummary {
    pub attempted: usize,
    pub acknowledged: usize,
    pub failed: usize,
    pub stopped_after_failure: bool,
}

impl MetadataOutboxDrainSummary {
    pub fn is_empty(&self) -> bool {
        self.attempted == 0
    }
}

pub fn drain_pending_outbox(
    outbox: &mut MetadataOutbox,
    writer: &mut (impl IptoWriter + ?Sized),
    max_attempts: usize,
) -> MetadataOutboxDrainSummary {
    let mut summary = MetadataOutboxDrainSummary {
        attempted: 0,
        acknowledged: 0,
        failed: 0,
        stopped_after_failure: false,
    };

    for _ in 0..max_attempts {
        match deliver_next_pending(outbox, writer) {
            MetadataOutboxDeliveryResult::NoPending => break,
            MetadataOutboxDeliveryResult::Acknowledged { .. } => {
                summary.attempted += 1;
                summary.acknowledged += 1;
            }
            MetadataOutboxDeliveryResult::Failed { retryable, .. } => {
                summary.attempted += 1;
                summary.failed += 1;
                summary.stopped_after_failure = retryable;
                if retryable {
                    break;
                }
            }
        }
    }

    summary
}

pub fn drain_pending_outbox_for_target(
    outbox: &mut MetadataOutbox,
    target: &IptoInstanceId,
    writer: &mut (impl IptoWriter + ?Sized),
    max_attempts: usize,
) -> MetadataOutboxDrainSummary {
    let mut summary = MetadataOutboxDrainSummary {
        attempted: 0,
        acknowledged: 0,
        failed: 0,
        stopped_after_failure: false,
    };

    for _ in 0..max_attempts {
        match deliver_next_pending_for_target(outbox, target, writer) {
            MetadataOutboxDeliveryResult::NoPending => break,
            MetadataOutboxDeliveryResult::Acknowledged { .. } => {
                summary.attempted += 1;
                summary.acknowledged += 1;
            }
            MetadataOutboxDeliveryResult::Failed { retryable, .. } => {
                summary.attempted += 1;
                summary.failed += 1;
                summary.stopped_after_failure = retryable;
                if retryable {
                    break;
                }
            }
        }
    }

    summary
}

#[derive(Debug)]
pub struct SegmentBackedMetadataOutbox {
    outbox: MetadataOutbox,
    writer: SegmentWriter,
}

impl SegmentBackedMetadataOutbox {
    pub fn create(
        path: impl AsRef<Path>,
        segment_id: SegmentId,
        node_id: NodeId,
    ) -> Result<Self, MetadataOutboxStorageError> {
        Ok(Self {
            outbox: MetadataOutbox::new(),
            writer: SegmentWriter::create(path, segment_id, node_id)?,
        })
    }

    pub fn recover_after(
        path: impl AsRef<Path>,
        segment_id: SegmentId,
        node_id: NodeId,
        checkpoint_offset: Option<RecordOffset>,
    ) -> Result<SegmentBackedMetadataOutboxRecovery, MetadataOutboxStorageError> {
        let path = path.as_ref();
        let replay = replay_metadata_outbox_segment_after(path, checkpoint_offset)?;
        let writer = SegmentWriter::open_append(path, segment_id, node_id)?;
        Ok(SegmentBackedMetadataOutboxRecovery {
            outbox: Self {
                outbox: replay.outbox,
                writer,
            },
            summary: replay.summary,
        })
    }

    pub fn enqueue_durable(
        &mut self,
        payload: IptoWritePayload,
    ) -> Result<DurableOutboxEnqueueResult, MetadataOutboxStorageError> {
        if self.outbox.entries.contains_key(&payload.idempotency_key) {
            return Ok(DurableOutboxEnqueueResult::Duplicate);
        }

        let offset = self.writer.append_record(&payload.encode())?;
        self.writer.sync()?;
        debug_assert_eq!(
            self.outbox.enqueue_with_offset(payload, Some(offset)),
            OutboxEnqueueResult::Enqueued
        );
        Ok(DurableOutboxEnqueueResult::Enqueued { offset })
    }

    pub fn flush(&mut self) -> Result<(), MetadataOutboxStorageError> {
        self.writer.flush()?;
        Ok(())
    }

    pub fn sync(&mut self) -> Result<(), MetadataOutboxStorageError> {
        self.writer.sync()?;
        Ok(())
    }

    pub fn manifest(&self) -> SegmentManifest {
        self.writer.manifest()
    }

    pub fn snapshot(&self) -> SegmentBackedMetadataOutboxSnapshot {
        SegmentBackedMetadataOutboxSnapshot {
            outbox: self.outbox.snapshot(),
            segment: self.writer.manifest(),
        }
    }

    pub fn acknowledged_checkpoint(
        &self,
        data_individual_shard_id: DataIndividualShardId,
        target: &IptoInstanceId,
        epoch: CheckpointEpoch,
    ) -> Option<MetadataOutboxCheckpoint> {
        self.outbox.acknowledged_checkpoint(
            data_individual_shard_id,
            target,
            self.writer.manifest().segment_id,
            epoch,
        )
    }

    pub fn seal(self) -> Result<SegmentManifest, MetadataOutboxStorageError> {
        Ok(self.writer.seal()?)
    }

    pub fn outbox(&self) -> &MetadataOutbox {
        &self.outbox
    }

    pub fn outbox_mut(&mut self) -> &mut MetadataOutbox {
        &mut self.outbox
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurableOutboxEnqueueResult {
    Enqueued { offset: RecordOffset },
    Duplicate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentBackedMetadataOutboxSnapshot {
    pub outbox: MetadataOutboxSnapshot,
    pub segment: SegmentManifest,
}

#[derive(Debug)]
pub struct SegmentBackedMetadataOutboxRecovery {
    pub outbox: SegmentBackedMetadataOutbox,
    pub summary: MetadataOutboxReplaySummary,
}

#[derive(Debug)]
pub enum MetadataOutboxStorageError {
    Segment(SegmentError),
    PayloadDecode(IptoPayloadDecodeError),
}

impl fmt::Display for MetadataOutboxStorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Segment(error) => write!(f, "metadata outbox segment error: {error}"),
            Self::PayloadDecode(error) => {
                write!(f, "metadata outbox payload decode error: {error}")
            }
        }
    }
}

impl std::error::Error for MetadataOutboxStorageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Segment(error) => Some(error),
            Self::PayloadDecode(error) => Some(error),
        }
    }
}

impl From<SegmentError> for MetadataOutboxStorageError {
    fn from(value: SegmentError) -> Self {
        Self::Segment(value)
    }
}

impl From<IptoPayloadDecodeError> for MetadataOutboxStorageError {
    fn from(value: IptoPayloadDecodeError) -> Self {
        Self::PayloadDecode(value)
    }
}

pub fn replay_metadata_outbox_segment(
    path: impl AsRef<Path>,
) -> Result<MetadataOutbox, MetadataOutboxStorageError> {
    Ok(replay_metadata_outbox_segment_after(path, None)?.outbox)
}

pub fn replay_metadata_outbox_segment_after(
    path: impl AsRef<Path>,
    checkpoint_offset: Option<RecordOffset>,
) -> Result<MetadataOutboxReplay, MetadataOutboxStorageError> {
    let mut reader = SegmentReader::open(path)?;
    let mut outbox = MetadataOutbox::new();
    let mut summary = MetadataOutboxReplaySummary {
        checkpoint_offset,
        scanned_records: 0,
        skipped_records: 0,
        replayed_records: 0,
    };

    while let Some(record) = reader.read_next()? {
        summary.scanned_records += 1;
        if checkpoint_offset.is_some_and(|offset| record.offset <= offset) {
            summary.skipped_records += 1;
            continue;
        }
        let payload = IptoWritePayload::decode(&record.payload)?;
        let _ = outbox.enqueue_with_offset(payload, Some(record.offset));
        summary.replayed_records += 1;
    }

    Ok(MetadataOutboxReplay { outbox, summary })
}

/// Replay outbox segment entries whose `shard_id` falls within the given
/// range, returning an outbox containing only the matching pending entries.
///
/// This enables rebalancing: when a placement map change moves a shard range
/// to a different Ipto instance, this function extracts the relevant payloads
/// so they can be re-delivered to the new target.
pub fn replay_metadata_outbox_segment_for_shard_range(
    path: impl AsRef<Path>,
    start: DataIndividualShardId,
    end: DataIndividualShardId,
) -> Result<MetadataOutbox, MetadataOutboxStorageError> {
    let mut reader = SegmentReader::open(path)?;
    let mut outbox = MetadataOutbox::new();
    let mut scanned = 0usize;
    let mut matched = 0usize;

    while let Some(record) = reader.read_next()? {
        scanned += 1;
        let payload = IptoWritePayload::decode(&record.payload)?;
        if payload.shard_id >= start && payload.shard_id <= end {
            let _ = outbox.enqueue_with_offset(payload, Some(record.offset));
            matched += 1;
        }
    }

    let _ = (scanned, matched);
    Ok(outbox)
}

/// Rebalance a shard range from one or more outbox segments to a new Ipto
/// target.
///
/// Replays the segment, extracting entries whose `shard_id` falls within
/// `[start, end]`, then drains the resulting pending entries through the
/// given writer. The writer must be connected to the new Ipto instance.
///
/// Idempotent: if some entries already exist on the target (e.g. from a
/// previous partial rebalance), they are skipped via correlation-id lookup.
///
/// Returns a summary of how many entries were attempted and acknowledged.
pub fn rebalance_shard_range_to(
    segment_path: impl AsRef<Path>,
    start: DataIndividualShardId,
    end: DataIndividualShardId,
    writer: &mut (impl IptoWriter + ?Sized),
    max_attempts: usize,
) -> Result<MetadataOutboxRebalanceSummary, MetadataOutboxStorageError> {
    let mut pending = replay_metadata_outbox_segment_for_shard_range(segment_path, start, end)?;
    let drain = drain_pending_outbox(&mut pending, writer, max_attempts);
    Ok(MetadataOutboxRebalanceSummary {
        entries_found: pending.len(),
        attempted: drain.attempted,
        acknowledged: drain.acknowledged,
        failed: drain.failed,
    })
}

/// Summary of a shard-range rebalancing operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataOutboxRebalanceSummary {
    pub entries_found: usize,
    pub attempted: usize,
    pub acknowledged: usize,
    pub failed: usize,
}

#[derive(Debug)]
pub struct MetadataOutboxReplay {
    pub outbox: MetadataOutbox,
    pub summary: MetadataOutboxReplaySummary,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataOutboxReplaySummary {
    pub checkpoint_offset: Option<RecordOffset>,
    pub scanned_records: usize,
    pub skipped_records: usize,
    pub replayed_records: usize,
}

fn write_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn write_i64(out: &mut Vec<u8>, value: i64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn write_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn write_string(out: &mut Vec<u8>, value: &str) {
    write_u32(out, value.len() as u32);
    out.extend_from_slice(value.as_bytes());
}

fn write_metadata_value(out: &mut Vec<u8>, value: &MetadataValue) {
    match value {
        MetadataValue::String(value) => {
            out.push(0);
            write_string(out, value);
        }
        MetadataValue::Integer(value) => {
            out.push(1);
            write_i64(out, *value);
        }
        MetadataValue::Boolean(value) => {
            out.push(2);
            out.push(u8::from(*value));
        }
        MetadataValue::Timestamp(value) => {
            out.push(3);
            write_string(out, value.as_str());
        }
        MetadataValue::StringList(values) => {
            out.push(4);
            write_u32(out, values.len() as u32);
            for value in values {
                write_string(out, value);
            }
        }
    }
}

struct DecodeCursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> DecodeCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn finish(&self) -> Result<(), IptoPayloadDecodeError> {
        if self.pos == self.bytes.len() {
            Ok(())
        } else {
            Err(IptoPayloadDecodeError::TrailingBytes(
                self.bytes.len() - self.pos,
            ))
        }
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8], IptoPayloadDecodeError> {
        let end = self
            .pos
            .checked_add(len)
            .ok_or(IptoPayloadDecodeError::UnexpectedEof)?;
        let Some(slice) = self.bytes.get(self.pos..end) else {
            return Err(IptoPayloadDecodeError::UnexpectedEof);
        };
        self.pos = end;
        Ok(slice)
    }

    fn read_u8(&mut self) -> Result<u8, IptoPayloadDecodeError> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u32(&mut self) -> Result<u32, IptoPayloadDecodeError> {
        let bytes: [u8; 4] = self
            .read_exact(4)?
            .try_into()
            .map_err(|_| IptoPayloadDecodeError::UnexpectedEof)?;
        Ok(u32::from_le_bytes(bytes))
    }

    fn read_i64(&mut self) -> Result<i64, IptoPayloadDecodeError> {
        let bytes: [u8; 8] = self
            .read_exact(8)?
            .try_into()
            .map_err(|_| IptoPayloadDecodeError::UnexpectedEof)?;
        Ok(i64::from_le_bytes(bytes))
    }

    fn read_u64(&mut self) -> Result<u64, IptoPayloadDecodeError> {
        let bytes: [u8; 8] = self
            .read_exact(8)?
            .try_into()
            .map_err(|_| IptoPayloadDecodeError::UnexpectedEof)?;
        Ok(u64::from_le_bytes(bytes))
    }

    fn read_string(&mut self) -> Result<String, IptoPayloadDecodeError> {
        let len = self.read_u32()? as usize;
        let bytes = self.read_exact(len)?;
        String::from_utf8(bytes.to_vec()).map_err(|_| IptoPayloadDecodeError::InvalidUtf8)
    }

    fn read_metadata_value(&mut self) -> Result<MetadataValue, IptoPayloadDecodeError> {
        let tag = self.read_u8()?;
        match tag {
            0 => Ok(MetadataValue::String(self.read_string()?)),
            1 => Ok(MetadataValue::Integer(self.read_i64()?)),
            2 => Ok(MetadataValue::Boolean(self.read_u8()? != 0)),
            3 => Ok(MetadataValue::Timestamp(EventTimestamp::from(
                self.read_string()?,
            ))),
            4 => {
                let count = self.read_u32()? as usize;
                let mut values = Vec::with_capacity(count);
                for _ in 0..count {
                    values.push(self.read_string()?);
                }
                Ok(MetadataValue::StringList(values))
            }
            other => Err(IptoPayloadDecodeError::InvalidValueTag(other)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NodeId;
    use crate::cluster::{IptoPlacementMap, IptoPlacementSlot, PlacementEpoch};
    use crate::data::{
        ActiveMetadata, DataIndividualId, MetadataEventId, MetadataOperation, MetadataValue,
        PassiveMetadata,
    };
    use crate::ingest::EventTimestamp;
    use crate::process::{EnvironmentId, PipelineId, ProcessInstanceId, TenantId};
    use crate::storage::{SegmentId, SegmentReader, SegmentWriter};
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn placement_maps_data_individual_shards_to_ipto_instances() {
        let placement = IptoPlacementMap::new(
            PlacementEpoch(1),
            vec![
                IptoPlacementSlot::new(IptoInstanceId::from("ipto-a"), 64).unwrap(),
                IptoPlacementSlot::new(IptoInstanceId::from("ipto-b"), 64).unwrap(),
            ],
            vec![],
        )
        .unwrap();

        let target = placement.resolve(DataIndividualShardId(0)).unwrap();
        assert!(target.as_str() == "ipto-a" || target.as_str() == "ipto-b");

        let target = placement.resolve(DataIndividualShardId(3)).unwrap();
        assert!(target.as_str() == "ipto-a" || target.as_str() == "ipto-b");
    }

    #[test]
    fn metadata_event_maps_to_ipto_payload_and_outbox_is_idempotent() {
        let event = sample_event()
            .with_passive_metadata(
                PassiveMetadata::new()
                    .insert("metadata.source_system", MetadataValue::string("orders"))
                    .insert("metadata.size_bytes", MetadataValue::Integer(128)),
            )
            .with_active_metadata(
                ActiveMetadata::new().insert("mask.customer.email", MetadataValue::Boolean(true)),
            );
        let mapping = IptoMapping::new("v1")
            .map_field("metadata.source_system", "attr:provenance.source_system")
            .map_field("metadata.size_bytes", "attr:provenance.size_bytes")
            .map_field("mask.customer.email", "attr:provenance.masked_field");

        let target = IptoInstanceId::from("ipto-a");
        let payload = IptoWritePayload::from_event(&event, &target, &mapping);
        assert_eq!(payload.target, IptoInstanceId::from("ipto-a"));
        assert_eq!(payload.attributes.len(), 3);

        let mut outbox = MetadataOutbox::new();
        assert_eq!(
            outbox.enqueue(payload.clone()),
            OutboxEnqueueResult::Enqueued
        );
        assert_eq!(
            outbox.enqueue(payload.clone()),
            OutboxEnqueueResult::Duplicate
        );
        assert!(outbox.next_pending().is_some());
        assert!(outbox.acknowledge(&payload.idempotency_key));
        assert!(outbox.next_pending().is_none());
        assert_eq!(
            outbox.snapshot(),
            MetadataOutboxSnapshot {
                total: 1,
                pending: 0,
                acknowledged: 1,
                failed: 0,
                queued_pending: 0,
            }
        );
    }

    #[test]
    fn ipto_write_payload_round_trips_through_codec() {
        let payload = sample_payload();

        let decoded = IptoWritePayload::decode(&payload.encode()).unwrap();

        assert_eq!(decoded, payload);
    }

    #[test]
    fn ipto_write_payload_round_trips_through_segment_record() {
        let path = temp_segment_path("ipto-payload");
        let payload = sample_payload();
        let mut writer =
            SegmentWriter::create(&path, SegmentId::from("segment-a"), NodeId::from("node-a"))
                .unwrap();
        writer.append_record(&payload.encode()).unwrap();
        writer.seal().unwrap();

        let mut reader = SegmentReader::open(&path).unwrap();
        let record = reader.read_next().unwrap().unwrap();
        let decoded = IptoWritePayload::decode(&record.payload).unwrap();

        assert_eq!(decoded, payload);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn segment_backed_outbox_persists_before_pending_delivery() {
        let path = temp_segment_path("durable-outbox");
        let payload = sample_payload();
        let mut outbox = SegmentBackedMetadataOutbox::create(
            &path,
            SegmentId::from("outbox-segment-a"),
            NodeId::from("node-a"),
        )
        .unwrap();

        let result = outbox.enqueue_durable(payload.clone()).unwrap();
        assert!(matches!(
            result,
            DurableOutboxEnqueueResult::Enqueued { .. }
        ));
        let DurableOutboxEnqueueResult::Enqueued { offset } = result else {
            unreachable!("duplicate checked above")
        };
        assert_eq!(
            outbox.enqueue_durable(payload.clone()).unwrap(),
            DurableOutboxEnqueueResult::Duplicate
        );
        assert_eq!(outbox.outbox().len(), 1);
        assert_eq!(
            outbox.outbox().next_pending().unwrap().record_offset(),
            Some(offset)
        );
        let snapshot = outbox.snapshot();
        assert_eq!(snapshot.outbox.pending, 1);
        assert_eq!(snapshot.segment.record_count, 1);
        assert!(outbox.outbox_mut().acknowledge(&payload.idempotency_key));
        let checkpoint = outbox
            .acknowledged_checkpoint(
                DataIndividualShardId(42),
                &payload.target,
                CheckpointEpoch(1),
            )
            .unwrap();
        assert_eq!(checkpoint.segment_id, SegmentId::from("outbox-segment-a"));
        assert_eq!(checkpoint.last_acknowledged_offset, offset);
        assert_eq!(checkpoint.mapping_version, "v1");
        outbox.seal().unwrap();

        let replayed = replay_metadata_outbox_segment(&path).unwrap();
        let pending = replayed.next_pending().unwrap();
        assert_eq!(pending.payload(), &payload);
        assert_eq!(pending.record_offset(), Some(offset));

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn replay_after_checkpoint_skips_acknowledged_offsets() {
        let path = temp_segment_path("checkpoint-replay");
        let first = sample_payload();
        let mut second = sample_payload();
        second.idempotency_key = IdempotencyKey::from("data-2:metadata-event-2");
        let mut outbox = SegmentBackedMetadataOutbox::create(
            &path,
            SegmentId::from("outbox-segment-b"),
            NodeId::from("node-a"),
        )
        .unwrap();

        let DurableOutboxEnqueueResult::Enqueued {
            offset: first_offset,
        } = outbox.enqueue_durable(first.clone()).unwrap()
        else {
            unreachable!("first payload should be new")
        };
        let DurableOutboxEnqueueResult::Enqueued {
            offset: second_offset,
        } = outbox.enqueue_durable(second.clone()).unwrap()
        else {
            unreachable!("second payload should be new")
        };
        outbox.seal().unwrap();

        let replayed = replay_metadata_outbox_segment_after(&path, Some(first_offset)).unwrap();
        let pending = replayed.outbox.next_pending().unwrap();

        assert_eq!(pending.payload(), &second);
        assert_eq!(pending.record_offset(), Some(second_offset));
        assert_eq!(replayed.outbox.snapshot().total, 1);
        assert_eq!(
            replayed.summary,
            MetadataOutboxReplaySummary {
                checkpoint_offset: Some(first_offset),
                scanned_records: 2,
                skipped_records: 1,
                replayed_records: 1,
            }
        );

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn segment_backed_outbox_recovers_pending_entries_and_appends() {
        let path = temp_segment_path("recover-outbox");
        let first = sample_payload();
        let mut second = sample_payload();
        second.idempotency_key = IdempotencyKey::from("data-2:metadata-event-2");

        let first_offset = {
            let mut outbox = SegmentBackedMetadataOutbox::create(
                &path,
                SegmentId::from("recover-segment"),
                NodeId::from("node-a"),
            )
            .unwrap();
            let DurableOutboxEnqueueResult::Enqueued {
                offset: first_offset,
            } = outbox.enqueue_durable(first.clone()).unwrap()
            else {
                unreachable!("first payload should be new")
            };
            outbox.enqueue_durable(second.clone()).unwrap();
            assert!(outbox.outbox_mut().acknowledge(&first.idempotency_key));
            first_offset
        };

        let recovery = SegmentBackedMetadataOutbox::recover_after(
            &path,
            SegmentId::from("recover-segment"),
            NodeId::from("node-a"),
            Some(first_offset),
        )
        .unwrap();
        assert_eq!(
            recovery.summary,
            MetadataOutboxReplaySummary {
                checkpoint_offset: Some(first_offset),
                scanned_records: 2,
                skipped_records: 1,
                replayed_records: 1,
            }
        );
        assert_eq!(recovery.outbox.outbox().snapshot().pending, 1);
        assert_eq!(recovery.outbox.manifest().record_count, 2);

        let mut recovered = recovery.outbox;
        let mut third = sample_payload();
        third.idempotency_key = IdempotencyKey::from("data-3:metadata-event-3");
        recovered.enqueue_durable(third).unwrap();
        assert_eq!(recovered.manifest().record_count, 3);
        recovered.seal().unwrap();

        let replayed = replay_metadata_outbox_segment(&path).unwrap();
        assert_eq!(replayed.snapshot().total, 3);

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn outbox_delivery_acknowledges_successful_ipto_write() {
        let payload = sample_payload();
        let mut outbox = MetadataOutbox::new();
        assert_eq!(
            outbox.enqueue(payload.clone()),
            OutboxEnqueueResult::Enqueued
        );
        let mut writer = RecordingWriter::default();

        let result = deliver_next_pending(&mut outbox, &mut writer);

        assert_eq!(
            result,
            MetadataOutboxDeliveryResult::Acknowledged {
                idempotency_key: payload.idempotency_key.clone()
            }
        );
        assert_eq!(writer.written, vec![payload]);
        assert!(outbox.next_pending().is_none());
    }

    #[test]
    fn outbox_delivery_marks_failed_write_and_continues_to_later_pending() {
        let first = sample_payload();
        let mut second = sample_payload();
        second.idempotency_key = IdempotencyKey::from("data-2:metadata-event-2");
        let mut outbox = MetadataOutbox::new();
        assert_eq!(outbox.enqueue(first.clone()), OutboxEnqueueResult::Enqueued);
        assert_eq!(
            outbox.enqueue(second.clone()),
            OutboxEnqueueResult::Enqueued
        );
        let mut failing_writer = RecordingWriter::retryable_failure("ipto unavailable");

        let result = deliver_next_pending(&mut outbox, &mut failing_writer);

        assert_eq!(
            result,
            MetadataOutboxDeliveryResult::Failed {
                idempotency_key: first.idempotency_key.clone(),
                retryable: true,
                message: String::from("ipto unavailable"),
            }
        );
        assert_eq!(outbox.next_pending().unwrap().payload(), &second);
        assert_eq!(
            outbox.snapshot(),
            MetadataOutboxSnapshot {
                total: 2,
                pending: 1,
                acknowledged: 0,
                failed: 1,
                queued_pending: 1,
            }
        );

        assert!(outbox.retry_failed(&first.idempotency_key));
        assert_eq!(outbox.next_pending().unwrap().payload(), &second);
    }

    #[test]
    fn outbox_drain_respects_max_attempts() {
        let first = sample_payload();
        let mut second = sample_payload();
        second.idempotency_key = IdempotencyKey::from("data-2:metadata-event-2");
        let mut outbox = MetadataOutbox::new();
        assert_eq!(outbox.enqueue(first.clone()), OutboxEnqueueResult::Enqueued);
        assert_eq!(
            outbox.enqueue(second.clone()),
            OutboxEnqueueResult::Enqueued
        );
        let mut writer = RecordingWriter::default();

        let summary = drain_pending_outbox(&mut outbox, &mut writer, 1);

        assert_eq!(
            summary,
            MetadataOutboxDrainSummary {
                attempted: 1,
                acknowledged: 1,
                failed: 0,
                stopped_after_failure: false,
            }
        );
        assert_eq!(writer.written, vec![first]);
        assert_eq!(outbox.next_pending().unwrap().payload(), &second);
    }

    #[test]
    fn outbox_drain_stops_after_retryable_failure() {
        let first = sample_payload();
        let mut second = sample_payload();
        second.idempotency_key = IdempotencyKey::from("data-2:metadata-event-2");
        let mut outbox = MetadataOutbox::new();
        assert_eq!(outbox.enqueue(first.clone()), OutboxEnqueueResult::Enqueued);
        assert_eq!(
            outbox.enqueue(second.clone()),
            OutboxEnqueueResult::Enqueued
        );
        let mut writer =
            RecordingWriter::with_failures(vec![IptoWriteError::retryable("ipto unavailable")]);

        let summary = drain_pending_outbox(&mut outbox, &mut writer, 10);

        assert_eq!(
            summary,
            MetadataOutboxDrainSummary {
                attempted: 1,
                acknowledged: 0,
                failed: 1,
                stopped_after_failure: true,
            }
        );
        assert!(writer.written.is_empty());
        assert_eq!(outbox.next_pending().unwrap().payload(), &second);
    }

    #[test]
    fn replay_for_shard_range_filters_by_shard_id() {
        use crate::storage::{SegmentId, SegmentWriter};

        let path = temp_segment_path("shard-replay");
        let mut writer =
            SegmentWriter::create(&path, SegmentId::from("shard-seg"), NodeId::from("node-a"))
                .unwrap();

        let in_range = sample_payload();
        let mut out_of_range = sample_payload();
        out_of_range.idempotency_key = IdempotencyKey::from("other:event");
        out_of_range.shard_id = DataIndividualShardId(999);

        writer.append_record(&in_range.encode()).unwrap();
        writer.append_record(&out_of_range.encode()).unwrap();
        writer.seal().unwrap();

        let replayed = replay_metadata_outbox_segment_for_shard_range(
            &path,
            DataIndividualShardId(0),
            DataIndividualShardId(100),
        )
        .unwrap();

        assert_eq!(replayed.len(), 1);
        assert_eq!(
            replayed.next_pending().unwrap().payload().shard_id,
            DataIndividualShardId(42)
        );

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn rebalance_shard_range_to_delivers_only_matching_entries() {
        use crate::storage::{SegmentId, SegmentWriter};

        let path = temp_segment_path("rebalance");
        let mut seg_writer =
            SegmentWriter::create(&path, SegmentId::from("rebal-seg"), NodeId::from("node-a"))
                .unwrap();

        let in_range = sample_payload();
        let mut out_of_range = sample_payload();
        out_of_range.idempotency_key = IdempotencyKey::from("other:event");
        out_of_range.shard_id = DataIndividualShardId(999);

        seg_writer.append_record(&in_range.encode()).unwrap();
        seg_writer.append_record(&out_of_range.encode()).unwrap();
        seg_writer.seal().unwrap();

        let mut writer = RecordingWriter::default();

        let summary = rebalance_shard_range_to(
            &path,
            DataIndividualShardId(0),
            DataIndividualShardId(100),
            &mut writer,
            10,
        )
        .unwrap();

        assert_eq!(summary.entries_found, 1);
        assert_eq!(summary.acknowledged, 1);
        assert_eq!(summary.failed, 0);
        assert_eq!(writer.written.len(), 1);
        assert_eq!(writer.written[0].shard_id, DataIndividualShardId(42));

        fs::remove_file(path).unwrap();
    }

    #[derive(Default)]
    struct RecordingWriter {
        written: Vec<IptoWritePayload>,
        failures: VecDeque<IptoWriteError>,
    }

    impl RecordingWriter {
        fn retryable_failure(message: impl Into<String>) -> Self {
            Self::with_failures(vec![IptoWriteError::retryable(message)])
        }

        fn with_failures(failures: Vec<IptoWriteError>) -> Self {
            Self {
                written: Vec::new(),
                failures: failures.into(),
            }
        }
    }

    impl IptoWriter for RecordingWriter {
        fn write(&mut self, payload: &IptoWritePayload) -> Result<(), IptoWriteError> {
            if let Some(error) = self.failures.pop_front() {
                return Err(error);
            }
            self.written.push(payload.clone());
            Ok(())
        }
    }

    fn sample_event() -> DataIndividualMetadataEvent {
        DataIndividualMetadataEvent::new(
            MetadataEventId::from("metadata-event-1"),
            DataIndividualId::from("data-1"),
            DataIndividualShardId(42),
            TenantId::from("tenant-a"),
            EnvironmentId::from("prod"),
            PipelineId::from("pipeline-a"),
            ProcessInstanceId::from("instance-a"),
            EventTimestamp::from("2026-06-30T12:00:00Z"),
            MetadataOperation::Received,
        )
    }

    fn sample_payload() -> IptoWritePayload {
        let event = sample_event()
            .with_passive_metadata(
                PassiveMetadata::new()
                    .insert("metadata.source_system", MetadataValue::string("orders"))
                    .insert("metadata.size_bytes", MetadataValue::Integer(128))
                    .insert(
                        "metadata.received_at",
                        MetadataValue::Timestamp(EventTimestamp::from("2026-06-30T12:00:00Z")),
                    ),
            )
            .with_active_metadata(ActiveMetadata::new().insert(
                "mask.customer.email",
                MetadataValue::StringList(vec![String::from("customer.email")]),
            ));
        let mapping = IptoMapping::new("v1")
            .map_field("metadata.source_system", "attr:provenance.source_system")
            .map_field("metadata.size_bytes", "attr:provenance.size_bytes")
            .map_field("metadata.received_at", "attr:provenance.received_at")
            .map_field("mask.customer.email", "attr:provenance.masked_field");

        IptoWritePayload::from_event(&event, &IptoInstanceId::from("ipto-a"), &mapping)
    }

    fn temp_segment_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("vannak-{name}-{nanos}.seg"))
    }
}
