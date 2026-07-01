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

//! Durga monitor compatibility types.
//!
//! These mirror `org.gautelis.durga.ProcessEvent` closely enough for adapters
//! and tests to stay anchored to Durga's canonical monitoring contract. JSON
//! transport is deliberately outside this dependency-free core for now.

use crate::ingest::{EventId, EventTimestamp, PipelineEvent, SourceId, SourceSequence};
use crate::metadata::MetadataRef;
use crate::process::{
    ActivityId, BusinessKey, CorrelationId, EnvironmentId, ErrorInfo, EventKind, EventStatus,
    PipelineId, ProcessDefinitionId, ProcessInstanceId, ProcessVersion, TenantId, TokenId,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DurgaStatus {
    Started,
    Completed,
    Failed,
    Escalated,
    Cancelled,
    Unknown(String),
}

impl From<DurgaStatus> for EventStatus {
    fn from(value: DurgaStatus) -> Self {
        match value {
            DurgaStatus::Started => EventStatus::Started,
            DurgaStatus::Completed => EventStatus::Completed,
            DurgaStatus::Failed => EventStatus::Failed,
            DurgaStatus::Escalated => EventStatus::Escalated,
            DurgaStatus::Cancelled => EventStatus::Cancelled,
            DurgaStatus::Unknown(_) => EventStatus::Started,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DurgaEventType {
    ProcessStarted,
    ActivityEntered,
    ActivityCompleted,
    ActivityEscalated,
    ActivityCancelled,
    GatewayTaken,
    ProcessCompleted,
    ProcessFailed,
    Unknown(String),
}

impl From<DurgaEventType> for EventKind {
    fn from(value: DurgaEventType) -> Self {
        match value {
            DurgaEventType::ProcessStarted => EventKind::ProcessStarted,
            DurgaEventType::ActivityEntered => EventKind::ActivityEntered,
            DurgaEventType::ActivityCompleted => EventKind::ActivityCompleted,
            DurgaEventType::ActivityEscalated => EventKind::ActivityEscalated,
            DurgaEventType::ActivityCancelled => EventKind::ActivityCancelled,
            DurgaEventType::GatewayTaken => EventKind::GatewayTaken,
            DurgaEventType::ProcessCompleted => EventKind::ProcessCompleted,
            DurgaEventType::ProcessFailed => EventKind::ProcessFailed,
            DurgaEventType::Unknown(s) => EventKind::Unknown(s),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurgaErrorInfo {
    pub message: String,
    pub code: Option<String>,
}

impl From<DurgaErrorInfo> for ErrorInfo {
    fn from(value: DurgaErrorInfo) -> Self {
        ErrorInfo::new(value.message, value.code)
    }
}

/// Rust mirror of Durga's canonical monitoring event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurgaProcessEvent {
    pub process_instance_id: String,
    pub process_id: String,
    pub activity_id: Option<String>,
    pub token_id: Option<String>,
    pub correlation_id: Option<String>,
    pub payload: Option<String>,
    pub status: DurgaStatus,
    pub error: Option<DurgaErrorInfo>,
    pub event_type: DurgaEventType,
    pub process_version: Option<String>,
    pub business_key: Option<String>,
    pub timestamp: String,
    pub metadata_refs: Vec<MetadataRef>,
    pub schema_version: Option<String>,
}

impl DurgaProcessEvent {
    pub fn into_pipeline_event(
        self,
        source_id: SourceId,
        source_sequence: SourceSequence,
        tenant_id: TenantId,
        environment_id: EnvironmentId,
    ) -> PipelineEvent {
        let event_id = EventId::from(format!(
            "{}:{}:{}",
            source_id.as_str(),
            self.process_instance_id,
            source_sequence.0
        ));
        let kind = EventKind::from(self.event_type);
        let mut event = PipelineEvent::new(
            event_id,
            source_id,
            source_sequence,
            tenant_id,
            environment_id,
            PipelineId::from(self.process_id.clone()),
            ProcessDefinitionId::from(self.process_id),
            ProcessInstanceId::from(self.process_instance_id),
            EventTimestamp::from(self.timestamp),
            kind,
        )
        .with_status(EventStatus::from(self.status));

        let metadata_refs = self.metadata_refs;
        if !metadata_refs.is_empty() {
            event = event.with_metadata_refs(metadata_refs);
        }

        if let Some(activity_id) = self.activity_id {
            event = event.with_activity_id(ActivityId::from(activity_id));
        }
        if let Some(token_id) = self.token_id {
            event = event.with_token_id(TokenId::from(token_id));
        }
        if let Some(correlation_id) = self.correlation_id {
            event = event.with_correlation_id(CorrelationId::from(correlation_id));
        }
        if let Some(process_version) = self.process_version {
            event = event.with_process_version(ProcessVersion::from(process_version));
        }
        if let Some(business_key) = self.business_key {
            event = event.with_business_key(BusinessKey::from(business_key));
        }
        if let Some(error) = self.error {
            event = event.with_error(ErrorInfo::from(error));
        }
        if let Some(payload) = self.payload {
            event = event.with_payload(payload);
        }

        event
    }
}
