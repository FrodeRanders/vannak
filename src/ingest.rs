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
use std::fmt;

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
            status: kind.inferred_status(),
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
        self.kind
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
