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

use crate::ingest::EventTimestamp;
use crate::metadata::MetadataRef;
use std::collections::BTreeMap;
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

string_id!(TenantId);
string_id!(EnvironmentId);
string_id!(PipelineId);
string_id!(ProcessDefinitionId);
string_id!(ProcessInstanceId);
string_id!(ActivityId);
string_id!(TokenId);
string_id!(CorrelationId);
string_id!(BusinessKey);
string_id!(ProcessVersion);

/// Coarse Durga lifecycle status attached to a process event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventStatus {
    Started,
    Completed,
    Failed,
    Escalated,
    Cancelled,
}

/// Durga monitor event type. Keep this aligned with
/// `org.gautelis.durga.ProcessEvent.EventType`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventKind {
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

impl EventKind {
    pub fn inferred_status(self) -> EventStatus {
        match self {
            Self::ProcessStarted | Self::ActivityEntered => EventStatus::Started,
            Self::ActivityCompleted | Self::GatewayTaken | Self::ProcessCompleted => {
                EventStatus::Completed
            }
            Self::ActivityEscalated => EventStatus::Escalated,
            Self::ActivityCancelled => EventStatus::Cancelled,
            Self::ProcessFailed => EventStatus::Failed,
            Self::Unknown(_) => EventStatus::Started,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessStatus {
    Unknown,
    Active,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivityState {
    Entered,
    Completed,
    Escalated,
    Cancelled,
    GatewayTaken,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorInfo {
    pub message: String,
    pub code: Option<String>,
}

impl ErrorInfo {
    pub fn new(message: impl Into<String>, code: Option<String>) -> Self {
        Self {
            message: message.into(),
            code,
        }
    }
}

/// Mutable shard-local reducer state for one process instance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessInstanceState {
    tenant_id: TenantId,
    environment_id: EnvironmentId,
    pipeline_id: PipelineId,
    process_definition_id: ProcessDefinitionId,
    process_instance_id: ProcessInstanceId,
    process_version: Option<ProcessVersion>,
    current_activity_id: Option<ActivityId>,
    status: ProcessStatus,
    started_at: Option<EventTimestamp>,
    last_updated_at: EventTimestamp,
    completed_at: Option<EventTimestamp>,
    correlation_id: Option<CorrelationId>,
    business_key: Option<BusinessKey>,
    token_id: Option<TokenId>,
    activities: BTreeMap<ActivityId, ActivityState>,
    activity_entered_at: BTreeMap<ActivityId, EventTimestamp>,
    activity_durations: BTreeMap<ActivityId, u64>,
    metadata_refs: Vec<MetadataRef>,
    retry_count: u64,
    last_error: Option<ErrorInfo>,
}

impl ProcessInstanceState {
    pub fn new(event: &crate::ingest::PipelineEvent) -> Self {
        Self {
            tenant_id: event.tenant_id().clone(),
            environment_id: event.environment_id().clone(),
            pipeline_id: event.pipeline_id().clone(),
            process_definition_id: event.process_definition_id().clone(),
            process_instance_id: event.process_instance_id().clone(),
            process_version: event.process_version().cloned(),
            current_activity_id: None,
            status: ProcessStatus::Unknown,
            started_at: None,
            last_updated_at: event.timestamp().clone(),
            completed_at: None,
            correlation_id: event.correlation_id().cloned(),
            business_key: event.business_key().cloned(),
            token_id: event.token_id().cloned(),
            activities: BTreeMap::new(),
            activity_entered_at: BTreeMap::new(),
            activity_durations: BTreeMap::new(),
            metadata_refs: event.metadata_refs().to_vec(),
            retry_count: 0,
            last_error: None,
        }
    }

    pub fn apply(&mut self, event: &crate::ingest::PipelineEvent) {
        self.last_updated_at = event.timestamp().clone();
        self.process_version = event
            .process_version()
            .cloned()
            .or_else(|| self.process_version.clone());
        self.correlation_id = event
            .correlation_id()
            .cloned()
            .or_else(|| self.correlation_id.clone());
        self.business_key = event
            .business_key()
            .cloned()
            .or_else(|| self.business_key.clone());
        self.token_id = event.token_id().cloned().or_else(|| self.token_id.clone());
        self.current_activity_id = event
            .activity_id()
            .cloned()
            .or_else(|| self.current_activity_id.clone());
        self.last_error = event.error().cloned().or_else(|| self.last_error.clone());

        for metadata_ref in event.metadata_refs() {
            if !self.metadata_refs.contains(metadata_ref) {
                self.metadata_refs.push(metadata_ref.clone());
            }
        }

        match event.kind() {
            EventKind::ProcessStarted => {
                self.status = ProcessStatus::Active;
                if self.started_at.is_none() {
                    self.started_at = Some(event.timestamp().clone());
                }
            }
            EventKind::ActivityEntered => {
                self.status = ProcessStatus::Active;
                if let Some(activity_id) = event.activity_id() {
                    self.activities
                        .insert(activity_id.clone(), ActivityState::Entered);
                    self.activity_entered_at
                        .insert(activity_id.clone(), event.timestamp().clone());
                }
            }
            EventKind::ActivityCompleted => {
                self.status = ProcessStatus::Active;
                self.record_activity_terminal(event, ActivityState::Completed);
            }
            EventKind::ActivityEscalated => {
                self.status = ProcessStatus::Active;
                self.record_activity_terminal(event, ActivityState::Escalated);
            }
            EventKind::ActivityCancelled => {
                self.status = ProcessStatus::Cancelled;
                self.record_activity_terminal(event, ActivityState::Cancelled);
            }
            EventKind::GatewayTaken => {
                self.status = ProcessStatus::Active;
                self.record_activity_terminal(event, ActivityState::GatewayTaken);
            }
            EventKind::ProcessCompleted => {
                self.status = ProcessStatus::Completed;
                self.completed_at = Some(event.timestamp().clone());
                self.current_activity_id = Some(ActivityId::from("completed"));
                self.record_activity_terminal(event, ActivityState::Completed);
            }
            EventKind::ProcessFailed => {
                self.status = ProcessStatus::Failed;
                self.retry_count += 1;
                self.record_activity_terminal(event, ActivityState::Failed);
            }
            EventKind::Unknown(_) => {}
        }
    }

    pub fn snapshot(&self) -> ProcessInstanceSnapshot {
        ProcessInstanceSnapshot {
            tenant_id: self.tenant_id.clone(),
            environment_id: self.environment_id.clone(),
            pipeline_id: self.pipeline_id.clone(),
            process_definition_id: self.process_definition_id.clone(),
            process_instance_id: self.process_instance_id.clone(),
            process_version: self.process_version.clone(),
            current_activity_id: self.current_activity_id.clone(),
            status: self.status,
            started_at: self.started_at.clone(),
            last_updated_at: self.last_updated_at.clone(),
            completed_at: self.completed_at.clone(),
            correlation_id: self.correlation_id.clone(),
            business_key: self.business_key.clone(),
            token_id: self.token_id.clone(),
            activities: self.activities.clone(),
            activity_entered_at: self.activity_entered_at.clone(),
            activity_durations: self.activity_durations.clone(),
            metadata_refs: self.metadata_refs.clone(),
            retry_count: self.retry_count,
            last_error: self.last_error.clone(),
        }
    }

    fn record_activity_terminal(
        &mut self,
        event: &crate::ingest::PipelineEvent,
        state: ActivityState,
    ) {
        let Some(activity_id) = event.activity_id() else {
            return;
        };
        self.activities.insert(activity_id.clone(), state);
        if let Some(entered_at) = self.activity_entered_at.get(activity_id) {
            let duration = entered_at.saturating_duration_until(event.timestamp());
            self.activity_durations
                .insert(activity_id.clone(), duration);
        }
    }
}

/// Owned process-instance view returned by queries and snapshots.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessInstanceSnapshot {
    pub tenant_id: TenantId,
    pub environment_id: EnvironmentId,
    pub pipeline_id: PipelineId,
    pub process_definition_id: ProcessDefinitionId,
    pub process_instance_id: ProcessInstanceId,
    pub process_version: Option<ProcessVersion>,
    pub current_activity_id: Option<ActivityId>,
    pub status: ProcessStatus,
    pub started_at: Option<EventTimestamp>,
    pub last_updated_at: EventTimestamp,
    pub completed_at: Option<EventTimestamp>,
    pub correlation_id: Option<CorrelationId>,
    pub business_key: Option<BusinessKey>,
    pub token_id: Option<TokenId>,
    pub activities: BTreeMap<ActivityId, ActivityState>,
    pub activity_entered_at: BTreeMap<ActivityId, EventTimestamp>,
    pub activity_durations: BTreeMap<ActivityId, u64>,
    pub metadata_refs: Vec<MetadataRef>,
    pub retry_count: u64,
    pub last_error: Option<ErrorInfo>,
}
