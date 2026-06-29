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

//! IpTo placement, mapping, and durable outbox primitives.
//!
//! This module is still dependency-free. A later adapter can translate
//! `IpToWritePayload` into calls against the Rust IpTo repository API or direct
//! PostgreSQL-backed repositories.

use crate::NodeId;
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
pub struct IpToInstanceId(String);

impl IpToInstanceId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for IpToInstanceId {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for IpToInstanceId {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl fmt::Display for IpToInstanceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IpToPlacement {
    buckets: Vec<IpToInstanceId>,
}

impl IpToPlacement {
    pub fn new(buckets: Vec<IpToInstanceId>) -> Result<Self, IpToPlacementError> {
        if buckets.is_empty() {
            return Err(IpToPlacementError::NoInstances);
        }
        Ok(Self { buckets })
    }

    pub fn resolve(
        &self,
        shard_id: DataIndividualShardId,
    ) -> Result<IpToInstanceId, IpToPlacementError> {
        let idx = shard_id.0 as usize % self.buckets.len();
        self.buckets
            .get(idx)
            .cloned()
            .ok_or(IpToPlacementError::NoInstances)
    }

    pub fn instances(&self) -> &[IpToInstanceId] {
        &self.buckets
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IpToPlacementError {
    NoInstances,
}

impl fmt::Display for IpToPlacementError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoInstances => f.write_str("IpTo placement requires at least one instance"),
        }
    }
}

impl std::error::Error for IpToPlacementError {}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IpToAttributeName(String);

impl IpToAttributeName {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for IpToAttributeName {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for IpToAttributeName {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

/// Versioned mapping from Vannak metadata field names to IpTo attributes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IpToMapping {
    version: String,
    fields: BTreeMap<MetadataFieldName, IpToAttributeName>,
}

impl IpToMapping {
    pub fn new(version: impl Into<String>) -> Self {
        Self {
            version: version.into(),
            fields: BTreeMap::new(),
        }
    }

    pub fn map_field(
        mut self,
        field: impl Into<MetadataFieldName>,
        attribute: impl Into<IpToAttributeName>,
    ) -> Self {
        self.fields.insert(field.into(), attribute.into());
        self
    }

    pub fn version(&self) -> &str {
        &self.version
    }

    pub fn attribute_for(&self, field: &MetadataFieldName) -> Option<&IpToAttributeName> {
        self.fields.get(field)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IpToWritePayload {
    pub target: IpToInstanceId,
    pub idempotency_key: IdempotencyKey,
    pub mapping_version: String,
    pub attributes: BTreeMap<IpToAttributeName, MetadataValue>,
}

impl IpToWritePayload {
    pub fn from_event(
        event: &DataIndividualMetadataEvent,
        placement: &IpToPlacement,
        mapping: &IpToMapping,
    ) -> Result<Self, IpToPlacementError> {
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

        Ok(Self {
            target: placement.resolve(event.data_individual_shard_id())?,
            idempotency_key: event.idempotency_key().clone(),
            mapping_version: mapping.version().to_string(),
            attributes,
        })
    }

    /// Encodes this payload for append-only outbox segment storage.
    ///
    /// This is a small stable binary format, not a general serialization
    /// framework. It exists so outbox replay can be implemented before choosing
    /// JSON, serde, or a direct IpTo wire format.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        write_string(&mut out, self.target.as_str());
        write_string(&mut out, self.idempotency_key.as_str());
        write_string(&mut out, &self.mapping_version);
        write_u32(&mut out, self.attributes.len() as u32);
        for (attribute, value) in &self.attributes {
            write_string(&mut out, attribute.as_str());
            write_metadata_value(&mut out, value);
        }
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, IpToPayloadDecodeError> {
        let mut cursor = DecodeCursor::new(bytes);
        let target = IpToInstanceId::from(cursor.read_string()?);
        let idempotency_key = IdempotencyKey::from(cursor.read_string()?);
        let mapping_version = cursor.read_string()?;
        let attribute_count = cursor.read_u32()? as usize;
        let mut attributes = BTreeMap::new();
        for _ in 0..attribute_count {
            let attribute = IpToAttributeName::from(cursor.read_string()?);
            let value = cursor.read_metadata_value()?;
            attributes.insert(attribute, value);
        }
        cursor.finish()?;

        Ok(Self {
            target,
            idempotency_key,
            mapping_version,
            attributes,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IpToPayloadDecodeError {
    UnexpectedEof,
    InvalidUtf8,
    InvalidValueTag(u8),
    TrailingBytes(usize),
}

impl fmt::Display for IpToPayloadDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedEof => f.write_str("unexpected end of IpTo payload bytes"),
            Self::InvalidUtf8 => f.write_str("IpTo payload contains invalid UTF-8"),
            Self::InvalidValueTag(tag) => write!(f, "invalid IpTo metadata value tag {tag}"),
            Self::TrailingBytes(count) => {
                write!(f, "IpTo payload has {count} trailing undecoded bytes")
            }
        }
    }
}

impl std::error::Error for IpToPayloadDecodeError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutboxStatus {
    Pending,
    Acknowledged,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataOutboxEntry {
    payload: IpToWritePayload,
    status: OutboxStatus,
    retry_count: u32,
    last_error: Option<String>,
}

impl MetadataOutboxEntry {
    pub fn payload(&self) -> &IpToWritePayload {
        &self.payload
    }

    pub fn status(&self) -> OutboxStatus {
        self.status
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

    pub fn enqueue(&mut self, payload: IpToWritePayload) -> OutboxEnqueueResult {
        let key = payload.idempotency_key.clone();
        if self.entries.contains_key(&key) {
            return OutboxEnqueueResult::Duplicate;
        }

        self.entries.insert(
            key.clone(),
            MetadataOutboxEntry {
                payload,
                status: OutboxStatus::Pending,
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutboxEnqueueResult {
    Enqueued,
    Duplicate,
}

pub trait IpToWriter {
    fn write(&mut self, payload: &IpToWritePayload) -> Result<(), IpToWriteError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IpToWriteError {
    message: String,
    retryable: bool,
}

impl IpToWriteError {
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

impl fmt::Display for IpToWriteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.retryable {
            write!(f, "retryable IpTo write error: {}", self.message)
        } else {
            write!(f, "permanent IpTo write error: {}", self.message)
        }
    }
}

impl std::error::Error for IpToWriteError {}

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
    writer: &mut impl IpToWriter,
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

    pub fn enqueue_durable(
        &mut self,
        payload: IpToWritePayload,
    ) -> Result<DurableOutboxEnqueueResult, MetadataOutboxStorageError> {
        if self.outbox.entries.contains_key(&payload.idempotency_key) {
            return Ok(DurableOutboxEnqueueResult::Duplicate);
        }

        let offset = self.writer.append_record(&payload.encode())?;
        self.writer.sync()?;
        debug_assert_eq!(self.outbox.enqueue(payload), OutboxEnqueueResult::Enqueued);
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

#[derive(Debug)]
pub enum MetadataOutboxStorageError {
    Segment(SegmentError),
    PayloadDecode(IpToPayloadDecodeError),
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

impl From<IpToPayloadDecodeError> for MetadataOutboxStorageError {
    fn from(value: IpToPayloadDecodeError) -> Self {
        Self::PayloadDecode(value)
    }
}

pub fn replay_metadata_outbox_segment(
    path: impl AsRef<Path>,
) -> Result<MetadataOutbox, MetadataOutboxStorageError> {
    let mut reader = SegmentReader::open(path)?;
    let mut outbox = MetadataOutbox::new();

    while let Some(record) = reader.read_next()? {
        let payload = IpToWritePayload::decode(&record.payload)?;
        let _ = outbox.enqueue(payload);
    }

    Ok(outbox)
}

fn write_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn write_i64(out: &mut Vec<u8>, value: i64) {
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

    fn finish(&self) -> Result<(), IpToPayloadDecodeError> {
        if self.pos == self.bytes.len() {
            Ok(())
        } else {
            Err(IpToPayloadDecodeError::TrailingBytes(
                self.bytes.len() - self.pos,
            ))
        }
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8], IpToPayloadDecodeError> {
        let end = self
            .pos
            .checked_add(len)
            .ok_or(IpToPayloadDecodeError::UnexpectedEof)?;
        let Some(slice) = self.bytes.get(self.pos..end) else {
            return Err(IpToPayloadDecodeError::UnexpectedEof);
        };
        self.pos = end;
        Ok(slice)
    }

    fn read_u8(&mut self) -> Result<u8, IpToPayloadDecodeError> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u32(&mut self) -> Result<u32, IpToPayloadDecodeError> {
        let bytes: [u8; 4] = self
            .read_exact(4)?
            .try_into()
            .map_err(|_| IpToPayloadDecodeError::UnexpectedEof)?;
        Ok(u32::from_le_bytes(bytes))
    }

    fn read_i64(&mut self) -> Result<i64, IpToPayloadDecodeError> {
        let bytes: [u8; 8] = self
            .read_exact(8)?
            .try_into()
            .map_err(|_| IpToPayloadDecodeError::UnexpectedEof)?;
        Ok(i64::from_le_bytes(bytes))
    }

    fn read_string(&mut self) -> Result<String, IpToPayloadDecodeError> {
        let len = self.read_u32()? as usize;
        let bytes = self.read_exact(len)?;
        String::from_utf8(bytes.to_vec()).map_err(|_| IpToPayloadDecodeError::InvalidUtf8)
    }

    fn read_metadata_value(&mut self) -> Result<MetadataValue, IpToPayloadDecodeError> {
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
            other => Err(IpToPayloadDecodeError::InvalidValueTag(other)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NodeId;
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
        let placement = IpToPlacement::new(vec![
            IpToInstanceId::from("ipto-a"),
            IpToInstanceId::from("ipto-b"),
        ])
        .unwrap();

        assert_eq!(
            placement.resolve(DataIndividualShardId(0)).unwrap(),
            IpToInstanceId::from("ipto-a")
        );
        assert_eq!(
            placement.resolve(DataIndividualShardId(3)).unwrap(),
            IpToInstanceId::from("ipto-b")
        );
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
        let placement = IpToPlacement::new(vec![IpToInstanceId::from("ipto-a")]).unwrap();
        let mapping = IpToMapping::new("v1")
            .map_field("metadata.source_system", "attr:provenance.source_system")
            .map_field("metadata.size_bytes", "attr:provenance.size_bytes")
            .map_field("mask.customer.email", "attr:provenance.masked_field");

        let payload = IpToWritePayload::from_event(&event, &placement, &mapping).unwrap();
        assert_eq!(payload.target, IpToInstanceId::from("ipto-a"));
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
    }

    #[test]
    fn ipto_write_payload_round_trips_through_codec() {
        let payload = sample_payload();

        let decoded = IpToWritePayload::decode(&payload.encode()).unwrap();

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
        let decoded = IpToWritePayload::decode(&record.payload).unwrap();

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
        assert_eq!(
            outbox.enqueue_durable(payload.clone()).unwrap(),
            DurableOutboxEnqueueResult::Duplicate
        );
        assert_eq!(outbox.outbox().len(), 1);
        outbox.seal().unwrap();

        let replayed = replay_metadata_outbox_segment(&path).unwrap();
        let pending = replayed.next_pending().unwrap();
        assert_eq!(pending.payload(), &payload);

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

        assert!(outbox.retry_failed(&first.idempotency_key));
        assert_eq!(outbox.next_pending().unwrap().payload(), &second);
    }

    #[derive(Default)]
    struct RecordingWriter {
        written: Vec<IpToWritePayload>,
        failure: Option<IpToWriteError>,
    }

    impl RecordingWriter {
        fn retryable_failure(message: impl Into<String>) -> Self {
            Self {
                written: Vec::new(),
                failure: Some(IpToWriteError::retryable(message)),
            }
        }
    }

    impl IpToWriter for RecordingWriter {
        fn write(&mut self, payload: &IpToWritePayload) -> Result<(), IpToWriteError> {
            if let Some(error) = self.failure.clone() {
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

    fn sample_payload() -> IpToWritePayload {
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
        let placement = IpToPlacement::new(vec![IpToInstanceId::from("ipto-a")]).unwrap();
        let mapping = IpToMapping::new("v1")
            .map_field("metadata.source_system", "attr:provenance.source_system")
            .map_field("metadata.size_bytes", "attr:provenance.size_bytes")
            .map_field("metadata.received_at", "attr:provenance.received_at")
            .map_field("mask.customer.email", "attr:provenance.masked_field");

        IpToWritePayload::from_event(&event, &placement, &mapping).unwrap()
    }

    fn temp_segment_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("vannak-{name}-{nanos}.seg"))
    }
}
