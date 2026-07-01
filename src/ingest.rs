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

use crate::metadata::{MetadataRef, MetadataVersion};
use crate::process::{
    ActivityId, BusinessKey, CorrelationId, EnvironmentId, ErrorInfo, EventKind, EventStatus,
    PipelineId, ProcessDefinitionId, ProcessInstanceId, ProcessVersion, TenantId, TokenId,
};
use crate::storage::{
    RecordOffset, SegmentError, SegmentId, SegmentManifest, SegmentReader, SegmentWriter,
};
use std::fmt;
use std::path::Path;

macro_rules! string_id {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self::new(value)
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self::new(value)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

string_id!(EventId);
string_id!(SourceId);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SourceSequence(pub u64);

/// Durga-compatible event timestamp.
///
/// Durga publishes ISO-8601 instants as strings. Vannak keeps that source shape
/// at the domain boundary and parses only the small subset it needs for local
/// latency projections.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventTimestamp(String);

impl EventTimestamp {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn saturating_duration_until(&self, later: &Self) -> u64 {
        match (
            parse_epoch_millis(self.as_str()),
            parse_epoch_millis(later.as_str()),
        ) {
            (Some(start), Some(end)) => end.saturating_sub(start),
            _ => 0,
        }
    }
}

impl From<&str> for EventTimestamp {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for EventTimestamp {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl fmt::Display for EventTimestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Validated process event accepted by Vannak's hot path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineEvent {
    event_id: EventId,
    source_id: SourceId,
    source_sequence: SourceSequence,
    tenant_id: TenantId,
    environment_id: EnvironmentId,
    pipeline_id: PipelineId,
    process_definition_id: ProcessDefinitionId,
    process_instance_id: ProcessInstanceId,
    process_version: Option<ProcessVersion>,
    activity_id: Option<ActivityId>,
    token_id: Option<TokenId>,
    correlation_id: Option<CorrelationId>,
    business_key: Option<BusinessKey>,
    timestamp: EventTimestamp,
    status: EventStatus,
    kind: EventKind,
    error: Option<ErrorInfo>,
    metadata_refs: Vec<MetadataRef>,
    metadata_version: Option<MetadataVersion>,
    causal_parent: Option<EventId>,
    payload: Option<String>,
}

impl PipelineEvent {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        event_id: EventId,
        source_id: SourceId,
        source_sequence: SourceSequence,
        tenant_id: TenantId,
        environment_id: EnvironmentId,
        pipeline_id: PipelineId,
        process_definition_id: ProcessDefinitionId,
        process_instance_id: ProcessInstanceId,
        timestamp: EventTimestamp,
        kind: EventKind,
    ) -> Self {
        let status = kind.clone().inferred_status();
        Self {
            event_id,
            source_id,
            source_sequence,
            tenant_id,
            environment_id,
            pipeline_id,
            process_definition_id,
            process_instance_id,
            process_version: None,
            activity_id: None,
            token_id: None,
            correlation_id: None,
            business_key: None,
            timestamp,
            status,
            kind,
            error: None,
            metadata_refs: Vec::new(),
            metadata_version: None,
            causal_parent: None,
            payload: None,
        }
    }

    pub fn with_status(mut self, status: EventStatus) -> Self {
        self.status = status;
        self
    }

    pub fn with_process_version(mut self, version: ProcessVersion) -> Self {
        self.process_version = Some(version);
        self
    }

    pub fn with_activity_id(mut self, activity_id: ActivityId) -> Self {
        self.activity_id = Some(activity_id);
        self
    }

    pub fn with_token_id(mut self, token_id: TokenId) -> Self {
        self.token_id = Some(token_id);
        self
    }

    pub fn with_correlation_id(mut self, correlation_id: CorrelationId) -> Self {
        self.correlation_id = Some(correlation_id);
        self
    }

    pub fn with_business_key(mut self, business_key: BusinessKey) -> Self {
        self.business_key = Some(business_key);
        self
    }

    pub fn with_error(mut self, error: ErrorInfo) -> Self {
        self.error = Some(error);
        self
    }

    pub fn with_metadata_refs(mut self, metadata_refs: Vec<MetadataRef>) -> Self {
        self.metadata_refs = metadata_refs;
        self
    }

    pub fn with_metadata_version(mut self, metadata_version: MetadataVersion) -> Self {
        self.metadata_version = Some(metadata_version);
        self
    }

    pub fn with_causal_parent(mut self, causal_parent: EventId) -> Self {
        self.causal_parent = Some(causal_parent);
        self
    }

    pub fn with_payload(mut self, payload: impl Into<String>) -> Self {
        self.payload = Some(payload.into());
        self
    }

    pub fn validate(&self) -> Result<(), IngestError> {
        validate_non_empty("event_id", self.event_id.as_str())?;
        validate_non_empty("source_id", self.source_id.as_str())?;
        validate_non_empty("tenant_id", self.tenant_id.as_str())?;
        validate_non_empty("environment_id", self.environment_id.as_str())?;
        validate_non_empty("pipeline_id", self.pipeline_id.as_str())?;
        validate_non_empty("process_definition_id", self.process_definition_id.as_str())?;
        validate_non_empty("process_instance_id", self.process_instance_id.as_str())?;
        validate_non_empty("timestamp", self.timestamp.as_str())?;
        Ok(())
    }

    pub fn event_id(&self) -> &EventId {
        &self.event_id
    }

    pub fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    pub fn source_sequence(&self) -> SourceSequence {
        self.source_sequence
    }

    pub fn tenant_id(&self) -> &TenantId {
        &self.tenant_id
    }

    pub fn environment_id(&self) -> &EnvironmentId {
        &self.environment_id
    }

    pub fn pipeline_id(&self) -> &PipelineId {
        &self.pipeline_id
    }

    pub fn process_definition_id(&self) -> &ProcessDefinitionId {
        &self.process_definition_id
    }

    pub fn process_instance_id(&self) -> &ProcessInstanceId {
        &self.process_instance_id
    }

    pub fn process_version(&self) -> Option<&ProcessVersion> {
        self.process_version.as_ref()
    }

    pub fn activity_id(&self) -> Option<&ActivityId> {
        self.activity_id.as_ref()
    }

    pub fn token_id(&self) -> Option<&TokenId> {
        self.token_id.as_ref()
    }

    pub fn correlation_id(&self) -> Option<&CorrelationId> {
        self.correlation_id.as_ref()
    }

    pub fn business_key(&self) -> Option<&BusinessKey> {
        self.business_key.as_ref()
    }

    pub fn timestamp(&self) -> &EventTimestamp {
        &self.timestamp
    }

    pub fn status(&self) -> EventStatus {
        self.status
    }

    pub fn kind(&self) -> EventKind {
        self.kind.clone()
    }

    pub fn error(&self) -> Option<&ErrorInfo> {
        self.error.as_ref()
    }

    pub fn metadata_refs(&self) -> &[MetadataRef] {
        &self.metadata_refs
    }

    pub fn metadata_version(&self) -> Option<&MetadataVersion> {
        self.metadata_version.as_ref()
    }

    pub fn causal_parent(&self) -> Option<&EventId> {
        self.causal_parent.as_ref()
    }

    pub fn payload(&self) -> Option<&str> {
        self.payload.as_deref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngestError {
    EmptyField { field: &'static str },
}

impl fmt::Display for IngestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyField { field } => write!(f, "event field {field} must not be empty"),
        }
    }
}

impl std::error::Error for IngestError {}

fn validate_non_empty(field: &'static str, value: &str) -> Result<(), IngestError> {
    if value.is_empty() {
        Err(IngestError::EmptyField { field })
    } else {
        Ok(())
    }
}

const PIPELINE_EVENT_CODEC_MAGIC: &[u8; 4] = b"VPE1";

#[derive(Debug)]
pub struct ProcessEventJournal {
    writer: SegmentWriter,
}

impl ProcessEventJournal {
    pub fn create(
        path: impl AsRef<Path>,
        segment_id: SegmentId,
        node_id: crate::cluster::NodeId,
    ) -> Result<Self, ProcessEventJournalError> {
        Ok(Self {
            writer: SegmentWriter::create(path, segment_id, node_id)?,
        })
    }

    pub fn recover(
        path: impl AsRef<Path>,
        segment_id: SegmentId,
        node_id: crate::cluster::NodeId,
    ) -> Result<ProcessEventJournalRecovery, ProcessEventJournalError> {
        let replay = replay_process_event_segment(&path)?;
        let writer = SegmentWriter::open_append(path, segment_id, node_id)?;
        Ok(ProcessEventJournalRecovery {
            journal: Self { writer },
            replay,
        })
    }

    pub fn append_durable(
        &mut self,
        event: &PipelineEvent,
    ) -> Result<RecordOffset, ProcessEventJournalError> {
        event.validate()?;
        let offset = self.writer.append_record(&encode_pipeline_event(event))?;
        self.writer.sync()?;
        Ok(offset)
    }

    pub fn manifest(&self) -> SegmentManifest {
        self.writer.manifest()
    }

    pub fn seal(self) -> Result<SegmentManifest, ProcessEventJournalError> {
        Ok(self.writer.seal()?)
    }
}

#[derive(Debug)]
pub struct ProcessEventJournalRecovery {
    pub journal: ProcessEventJournal,
    pub replay: ProcessEventReplay,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessEventReplay {
    pub events: Vec<JournaledPipelineEvent>,
    pub summary: ProcessEventReplaySummary,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournaledPipelineEvent {
    pub offset: RecordOffset,
    pub event: PipelineEvent,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessEventReplaySummary {
    pub scanned_records: u64,
    pub replayed_records: u64,
}

pub fn replay_process_event_segment(
    path: impl AsRef<Path>,
) -> Result<ProcessEventReplay, ProcessEventJournalError> {
    let mut reader = SegmentReader::open(path)?;
    let mut events = Vec::new();
    let mut scanned_records = 0;

    while let Some(record) = reader.read_next()? {
        scanned_records += 1;
        let event = decode_pipeline_event(&record.payload)?;
        event.validate()?;
        events.push(JournaledPipelineEvent {
            offset: record.offset,
            event,
        });
    }

    Ok(ProcessEventReplay {
        summary: ProcessEventReplaySummary {
            scanned_records,
            replayed_records: events.len() as u64,
        },
        events,
    })
}

#[derive(Debug)]
pub enum ProcessEventJournalError {
    Segment(SegmentError),
    Ingest(IngestError),
    Decode(ProcessEventDecodeError),
}

impl fmt::Display for ProcessEventJournalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Segment(error) => write!(f, "{error}"),
            Self::Ingest(error) => write!(f, "{error}"),
            Self::Decode(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for ProcessEventJournalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Segment(error) => Some(error),
            Self::Ingest(error) => Some(error),
            Self::Decode(error) => Some(error),
        }
    }
}

impl From<SegmentError> for ProcessEventJournalError {
    fn from(value: SegmentError) -> Self {
        Self::Segment(value)
    }
}

impl From<IngestError> for ProcessEventJournalError {
    fn from(value: IngestError) -> Self {
        Self::Ingest(value)
    }
}

impl From<ProcessEventDecodeError> for ProcessEventJournalError {
    fn from(value: ProcessEventDecodeError) -> Self {
        Self::Decode(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessEventDecodeError {
    InvalidMagic,
    UnexpectedEof,
    TrailingBytes { remaining: usize },
    InvalidUtf8,
    InvalidBool { value: u8 },
    InvalidEventStatus { value: u8 },
    InvalidEventKind { value: u8 },
    InvalidMetadataRefKind { value: u8 },
}

impl fmt::Display for ProcessEventDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidMagic => f.write_str("invalid pipeline event payload magic"),
            Self::UnexpectedEof => f.write_str("truncated pipeline event payload"),
            Self::TrailingBytes { remaining } => {
                write!(f, "pipeline event payload has {remaining} trailing bytes")
            }
            Self::InvalidUtf8 => f.write_str("pipeline event payload contains invalid UTF-8"),
            Self::InvalidBool { value } => write!(f, "invalid boolean tag {value}"),
            Self::InvalidEventStatus { value } => write!(f, "invalid event status tag {value}"),
            Self::InvalidEventKind { value } => write!(f, "invalid event kind tag {value}"),
            Self::InvalidMetadataRefKind { value } => {
                write!(f, "invalid metadata reference kind tag {value}")
            }
        }
    }
}

impl std::error::Error for ProcessEventDecodeError {}

pub(crate) fn encode_pipeline_event(event: &PipelineEvent) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(PIPELINE_EVENT_CODEC_MAGIC);
    put_string(&mut out, event.event_id().as_str());
    put_string(&mut out, event.source_id().as_str());
    put_u64(&mut out, event.source_sequence().0);
    put_string(&mut out, event.tenant_id().as_str());
    put_string(&mut out, event.environment_id().as_str());
    put_string(&mut out, event.pipeline_id().as_str());
    put_string(&mut out, event.process_definition_id().as_str());
    put_string(&mut out, event.process_instance_id().as_str());
    put_option_string(
        &mut out,
        event.process_version().map(ProcessVersion::as_str),
    );
    put_option_string(&mut out, event.activity_id().map(ActivityId::as_str));
    put_option_string(&mut out, event.token_id().map(TokenId::as_str));
    put_option_string(&mut out, event.correlation_id().map(CorrelationId::as_str));
    put_option_string(&mut out, event.business_key().map(BusinessKey::as_str));
    put_string(&mut out, event.timestamp().as_str());
    out.push(encode_event_status(event.status()));
    encode_event_kind(&mut out, &event.kind());
    put_option_error(&mut out, event.error());
    put_metadata_refs(&mut out, event.metadata_refs());
    put_option_string(
        &mut out,
        event.metadata_version().map(MetadataVersion::as_str),
    );
    put_option_string(&mut out, event.causal_parent().map(EventId::as_str));
    put_option_string(&mut out, event.payload());
    out
}

pub(crate) fn decode_pipeline_event(
    payload: &[u8],
) -> Result<PipelineEvent, ProcessEventDecodeError> {
    let mut cursor = PayloadCursor::new(payload);
    let magic = cursor.take(PIPELINE_EVENT_CODEC_MAGIC.len())?;
    if magic != PIPELINE_EVENT_CODEC_MAGIC {
        return Err(ProcessEventDecodeError::InvalidMagic);
    }

    let event_id = EventId::from(cursor.string()?);
    let source_id = SourceId::from(cursor.string()?);
    let source_sequence = SourceSequence(cursor.u64()?);
    let tenant_id = TenantId::from(cursor.string()?);
    let environment_id = EnvironmentId::from(cursor.string()?);
    let pipeline_id = PipelineId::from(cursor.string()?);
    let process_definition_id = ProcessDefinitionId::from(cursor.string()?);
    let process_instance_id = ProcessInstanceId::from(cursor.string()?);
    let process_version = cursor.option_string()?.map(ProcessVersion::from);
    let activity_id = cursor.option_string()?.map(ActivityId::from);
    let token_id = cursor.option_string()?.map(TokenId::from);
    let correlation_id = cursor.option_string()?.map(CorrelationId::from);
    let business_key = cursor.option_string()?.map(BusinessKey::from);
    let timestamp = EventTimestamp::from(cursor.string()?);
    let status = decode_event_status(cursor.u8()?)?;
    let kind_tag = cursor.u8()?;
    let kind = decode_event_kind(kind_tag, &mut cursor)?;
    let error = cursor.option_error()?;
    let metadata_refs = cursor.metadata_refs()?;
    let metadata_version = cursor.option_string()?.map(MetadataVersion::from);
    let causal_parent = cursor.option_string()?.map(EventId::from);
    let event_payload = cursor.option_string()?;
    cursor.finish()?;

    let mut event = PipelineEvent::new(
        event_id,
        source_id,
        source_sequence,
        tenant_id,
        environment_id,
        pipeline_id,
        process_definition_id,
        process_instance_id,
        timestamp,
        kind,
    )
    .with_status(status)
    .with_metadata_refs(metadata_refs);

    if let Some(value) = process_version {
        event = event.with_process_version(value);
    }
    if let Some(value) = activity_id {
        event = event.with_activity_id(value);
    }
    if let Some(value) = token_id {
        event = event.with_token_id(value);
    }
    if let Some(value) = correlation_id {
        event = event.with_correlation_id(value);
    }
    if let Some(value) = business_key {
        event = event.with_business_key(value);
    }
    if let Some(value) = error {
        event = event.with_error(value);
    }
    if let Some(value) = metadata_version {
        event = event.with_metadata_version(value);
    }
    if let Some(value) = causal_parent {
        event = event.with_causal_parent(value);
    }
    if let Some(value) = event_payload {
        event = event.with_payload(value);
    }

    Ok(event)
}

fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_string(out: &mut Vec<u8>, value: &str) {
    put_u32(out, value.len() as u32);
    out.extend_from_slice(value.as_bytes());
}

fn put_option_string(out: &mut Vec<u8>, value: Option<&str>) {
    match value {
        Some(value) => {
            out.push(1);
            put_string(out, value);
        }
        None => out.push(0),
    }
}

fn put_option_error(out: &mut Vec<u8>, value: Option<&ErrorInfo>) {
    match value {
        Some(error) => {
            out.push(1);
            put_string(out, &error.message);
            put_option_string(out, error.code.as_deref());
        }
        None => out.push(0),
    }
}

fn put_metadata_refs(out: &mut Vec<u8>, refs: &[MetadataRef]) {
    put_u32(out, refs.len() as u32);
    for metadata_ref in refs {
        match metadata_ref {
            MetadataRef::Dataset(id) => {
                out.push(0);
                put_string(out, id.as_str());
            }
            MetadataRef::Schema { id, version } => {
                out.push(1);
                put_string(out, id.as_str());
                put_option_string(out, version.as_ref().map(MetadataVersion::as_str));
            }
            MetadataRef::Field(id) => {
                out.push(2);
                put_string(out, id.as_str());
            }
            MetadataRef::PipelineDefinition { id, version } => {
                out.push(3);
                put_string(out, id.as_str());
                put_option_string(out, version.as_ref().map(MetadataVersion::as_str));
            }
            MetadataRef::Object(id) => {
                out.push(4);
                put_string(out, id.as_str());
            }
            MetadataRef::LineageEdge(id) => {
                out.push(5);
                put_string(out, id.as_str());
            }
            MetadataRef::DataContract(id) => {
                out.push(6);
                put_string(out, id.as_str());
            }
            MetadataRef::Owner(id) => {
                out.push(7);
                put_string(out, id.as_str());
            }
            MetadataRef::Classification(id) => {
                out.push(8);
                put_string(out, id.as_str());
            }
        }
    }
}

fn encode_event_status(value: EventStatus) -> u8 {
    match value {
        EventStatus::Started => 0,
        EventStatus::Completed => 1,
        EventStatus::Failed => 2,
        EventStatus::Escalated => 3,
        EventStatus::Cancelled => 4,
    }
}

fn decode_event_status(value: u8) -> Result<EventStatus, ProcessEventDecodeError> {
    match value {
        0 => Ok(EventStatus::Started),
        1 => Ok(EventStatus::Completed),
        2 => Ok(EventStatus::Failed),
        3 => Ok(EventStatus::Escalated),
        4 => Ok(EventStatus::Cancelled),
        _ => Err(ProcessEventDecodeError::InvalidEventStatus { value }),
    }
}

fn encode_event_kind(out: &mut Vec<u8>, value: &EventKind) {
    match value {
        EventKind::ProcessStarted => out.push(0),
        EventKind::ActivityEntered => out.push(1),
        EventKind::ActivityCompleted => out.push(2),
        EventKind::ActivityEscalated => out.push(3),
        EventKind::ActivityCancelled => out.push(4),
        EventKind::GatewayTaken => out.push(5),
        EventKind::ProcessCompleted => out.push(6),
        EventKind::ProcessFailed => out.push(7),
        EventKind::Unknown(s) => {
            out.push(8);
            put_string(out, s);
        }
    }
}

fn decode_event_kind(
    value: u8,
    cursor: &mut PayloadCursor<'_>,
) -> Result<EventKind, ProcessEventDecodeError> {
    match value {
        0 => Ok(EventKind::ProcessStarted),
        1 => Ok(EventKind::ActivityEntered),
        2 => Ok(EventKind::ActivityCompleted),
        3 => Ok(EventKind::ActivityEscalated),
        4 => Ok(EventKind::ActivityCancelled),
        5 => Ok(EventKind::GatewayTaken),
        6 => Ok(EventKind::ProcessCompleted),
        7 => Ok(EventKind::ProcessFailed),
        8 => Ok(EventKind::Unknown(cursor.string()?)),
        _ => Err(ProcessEventDecodeError::InvalidEventKind { value }),
    }
}

struct PayloadCursor<'a> {
    payload: &'a [u8],
    position: usize,
}

impl<'a> PayloadCursor<'a> {
    fn new(payload: &'a [u8]) -> Self {
        Self {
            payload,
            position: 0,
        }
    }

    fn finish(&self) -> Result<(), ProcessEventDecodeError> {
        let remaining = self.payload.len() - self.position;
        if remaining == 0 {
            Ok(())
        } else {
            Err(ProcessEventDecodeError::TrailingBytes { remaining })
        }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], ProcessEventDecodeError> {
        let end = self
            .position
            .checked_add(len)
            .ok_or(ProcessEventDecodeError::UnexpectedEof)?;
        if end > self.payload.len() {
            return Err(ProcessEventDecodeError::UnexpectedEof);
        }
        let bytes = &self.payload[self.position..end];
        self.position = end;
        Ok(bytes)
    }

    fn u8(&mut self) -> Result<u8, ProcessEventDecodeError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, ProcessEventDecodeError> {
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(self.take(4)?);
        Ok(u32::from_le_bytes(bytes))
    }

    fn u64(&mut self) -> Result<u64, ProcessEventDecodeError> {
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(self.take(8)?);
        Ok(u64::from_le_bytes(bytes))
    }

    fn string(&mut self) -> Result<String, ProcessEventDecodeError> {
        let len = self.u32()? as usize;
        let bytes = self.take(len)?;
        std::str::from_utf8(bytes)
            .map(str::to_owned)
            .map_err(|_| ProcessEventDecodeError::InvalidUtf8)
    }

    fn option_string(&mut self) -> Result<Option<String>, ProcessEventDecodeError> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.string()?)),
            value => Err(ProcessEventDecodeError::InvalidBool { value }),
        }
    }

    fn option_error(&mut self) -> Result<Option<ErrorInfo>, ProcessEventDecodeError> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(ErrorInfo::new(self.string()?, self.option_string()?))),
            value => Err(ProcessEventDecodeError::InvalidBool { value }),
        }
    }

    fn metadata_refs(&mut self) -> Result<Vec<MetadataRef>, ProcessEventDecodeError> {
        let count = self.u32()? as usize;
        let mut refs = Vec::with_capacity(count);
        for _ in 0..count {
            refs.push(match self.u8()? {
                0 => MetadataRef::Dataset(crate::metadata::DatasetId::from(self.string()?)),
                1 => MetadataRef::Schema {
                    id: crate::metadata::SchemaId::from(self.string()?),
                    version: self.option_string()?.map(MetadataVersion::from),
                },
                2 => MetadataRef::Field(crate::metadata::FieldId::from(self.string()?)),
                3 => MetadataRef::PipelineDefinition {
                    id: crate::metadata::PipelineDefinitionId::from(self.string()?),
                    version: self.option_string()?.map(MetadataVersion::from),
                },
                4 => MetadataRef::Object(crate::metadata::MetadataObjectId::from(self.string()?)),
                5 => MetadataRef::LineageEdge(crate::metadata::LineageEdgeId::from(self.string()?)),
                6 => {
                    MetadataRef::DataContract(crate::metadata::DataContractId::from(self.string()?))
                }
                7 => MetadataRef::Owner(crate::metadata::OwnerId::from(self.string()?)),
                8 => MetadataRef::Classification(crate::metadata::ClassificationId::from(
                    self.string()?,
                )),
                value => return Err(ProcessEventDecodeError::InvalidMetadataRefKind { value }),
            });
        }
        Ok(refs)
    }
}

fn parse_epoch_millis(timestamp: &str) -> Option<u64> {
    let rest = timestamp.strip_suffix('Z')?;
    let (date, time) = rest.split_once('T')?;
    let mut date_parts = date.split('-');
    let year = date_parts.next()?.parse::<i32>().ok()?;
    let month = date_parts.next()?.parse::<u32>().ok()?;
    let day = date_parts.next()?.parse::<u32>().ok()?;

    let mut time_parts = time.split(':');
    let hour = time_parts.next()?.parse::<u32>().ok()?;
    let minute = time_parts.next()?.parse::<u32>().ok()?;
    let second_part = time_parts.next()?;
    let (second, millis) = parse_second_millis(second_part)?;

    let days = days_from_civil(year, month, day)?;
    let seconds = days
        .checked_mul(86_400)?
        .checked_add(hour as i64 * 3_600 + minute as i64 * 60 + second as i64)?;
    u64::try_from(seconds)
        .ok()?
        .checked_mul(1_000)?
        .checked_add(millis)
}

fn parse_second_millis(value: &str) -> Option<(u32, u64)> {
    if let Some((second, fraction)) = value.split_once('.') {
        let second = second.parse::<u32>().ok()?;
        let millis = fraction
            .chars()
            .take(3)
            .collect::<String>()
            .parse::<u64>()
            .ok()?;
        Some((second, millis))
    } else {
        Some((value.parse::<u32>().ok()?, 0))
    }
}

fn days_from_civil(year: i32, month: u32, day: u32) -> Option<i64> {
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let year = year - i32::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let month = month as i32;
    let day = day as i32;
    let doy = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    Some((era * 146_097 + doe - 719_468) as i64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::NodeId;
    use crate::metadata::{DatasetId, FieldId, MetadataRef, MetadataVersion, PipelineDefinitionId};
    use crate::process::{
        ActivityId, BusinessKey, CorrelationId, EnvironmentId, ErrorInfo, EventKind, EventStatus,
        PipelineId, ProcessDefinitionId, ProcessInstanceId, ProcessVersion, TenantId, TokenId,
    };
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn process_event_codec_round_trips_full_event() {
        let event = pipeline_event("event-1", EventKind::ActivityCompleted)
            .with_status(EventStatus::Completed)
            .with_process_version(ProcessVersion::from("v3"))
            .with_activity_id(ActivityId::from("load"))
            .with_token_id(TokenId::from("token-1"))
            .with_correlation_id(CorrelationId::from("corr-1"))
            .with_business_key(BusinessKey::from("customer-42"))
            .with_error(ErrorInfo::new("late input", Some("E_LATE".to_string())))
            .with_metadata_refs(vec![
                MetadataRef::Dataset(DatasetId::from("customer-data")),
                MetadataRef::PipelineDefinition {
                    id: PipelineDefinitionId::from("pipe-def"),
                    version: Some(MetadataVersion::from("2026-06")),
                },
                MetadataRef::Field(FieldId::from("email")),
            ])
            .with_metadata_version(MetadataVersion::from("mv-7"))
            .with_causal_parent(EventId::from("event-0"))
            .with_payload("{\"records\":10}");

        let decoded = decode_pipeline_event(&encode_pipeline_event(&event)).unwrap();

        assert_eq!(decoded, event);
    }

    #[test]
    fn process_event_journal_replays_events_and_reopens_for_append() {
        let path = temp_segment_path("process-journal");
        let first = pipeline_event("event-1", EventKind::ProcessStarted);
        let second = pipeline_event("event-2", EventKind::ProcessCompleted);
        let third = pipeline_event("event-3", EventKind::ProcessFailed);

        let mut journal = ProcessEventJournal::create(
            &path,
            SegmentId::from("process-segment-a"),
            NodeId::from("node-a"),
        )
        .unwrap();
        let first_offset = journal.append_durable(&first).unwrap();
        journal.append_durable(&second).unwrap();
        drop(journal);

        let recovery = ProcessEventJournal::recover(
            &path,
            SegmentId::from("process-segment-a"),
            NodeId::from("node-a"),
        )
        .unwrap();
        assert_eq!(recovery.replay.summary.scanned_records, 2);
        assert_eq!(recovery.replay.events[0].offset, first_offset);
        assert_eq!(recovery.replay.events[0].event, first);
        assert_eq!(recovery.replay.events[1].event, second);

        let mut journal = recovery.journal;
        journal.append_durable(&third).unwrap();
        assert_eq!(journal.manifest().record_count, 3);
        drop(journal);

        let replay = replay_process_event_segment(&path).unwrap();
        assert_eq!(replay.summary.replayed_records, 3);
        assert_eq!(replay.events[2].event, third);

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn process_event_replay_rejects_invalid_payload() {
        let path = temp_segment_path("process-journal-invalid");
        let mut writer =
            SegmentWriter::create(&path, SegmentId::from("segment-a"), NodeId::from("node-a"))
                .unwrap();
        writer.append_record(b"not-a-process-event").unwrap();
        writer.sync().unwrap();
        drop(writer);

        let error = replay_process_event_segment(&path).unwrap_err();

        assert!(matches!(
            error,
            ProcessEventJournalError::Decode(ProcessEventDecodeError::InvalidMagic)
        ));

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn process_event_codec_round_trips_unknown_event_kind() {
        let event = PipelineEvent::new(
            EventId::from("event-unknown"),
            SourceId::from("durga"),
            SourceSequence(1),
            TenantId::from("tenant-a"),
            EnvironmentId::from("prod"),
            PipelineId::from("pipeline-a"),
            ProcessDefinitionId::from("definition-a"),
            ProcessInstanceId::from("instance-a"),
            EventTimestamp::from("2026-06-30T10:00:00Z"),
            EventKind::Unknown("CUSTOM_ACTION".to_string()),
        );
        let encoded = encode_pipeline_event(&event);
        let decoded = decode_pipeline_event(&encoded).unwrap();
        assert_eq!(decoded.event_id(), event.event_id());
        assert_eq!(
            decoded.kind(),
            EventKind::Unknown("CUSTOM_ACTION".to_string())
        );
    }

    fn pipeline_event(event_id: &str, kind: EventKind) -> PipelineEvent {
        PipelineEvent::new(
            EventId::from(event_id),
            SourceId::from("durga"),
            SourceSequence(11),
            TenantId::from("tenant-a"),
            EnvironmentId::from("prod"),
            PipelineId::from("pipeline-a"),
            ProcessDefinitionId::from("definition-a"),
            ProcessInstanceId::from("instance-a"),
            EventTimestamp::from("2026-06-30T10:00:00Z"),
            kind,
        )
    }

    fn temp_segment_path(name: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("vannak-{name}-{nanos}.seg"))
    }
}
