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

//! Data-individual provenance model.
//!
//! These types describe metadata about a flowing data item. They are separate
//! from Durga process-monitoring events: process events say an activity ran;
//! data-individual metadata events say what happened to a specific data item.

use crate::ingest::EventTimestamp;
use crate::process::{ActivityId, EnvironmentId, PipelineId, ProcessInstanceId, TenantId};
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

string_id!(DataIndividualId);
string_id!(MetadataEventId);
string_id!(PayloadRef);
string_id!(PluginName);
string_id!(PluginVersion);
string_id!(IdempotencyKey);

/// Domain placement key for durable metadata ownership.
///
/// This is not a Sitas executor shard id. It selects the Ipto repository
/// instance that owns this data individual's durable metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DataIndividualShardId(pub u64);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MetadataFieldName(String);

impl MetadataFieldName {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for MetadataFieldName {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for MetadataFieldName {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

/// Small dependency-free metadata value representation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataValue {
    String(String),
    Integer(i64),
    Boolean(bool),
    Timestamp(EventTimestamp),
    StringList(Vec<String>),
}

impl MetadataValue {
    pub fn string(value: impl Into<String>) -> Self {
        Self::String(value.into())
    }
}

/// Passive metadata observed at receive/create boundaries.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PassiveMetadata {
    fields: BTreeMap<MetadataFieldName, MetadataValue>,
}

impl PassiveMetadata {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(mut self, name: impl Into<MetadataFieldName>, value: MetadataValue) -> Self {
        self.fields.insert(name.into(), value);
        self
    }

    pub fn fields(&self) -> &BTreeMap<MetadataFieldName, MetadataValue> {
        &self.fields
    }
}

/// Active metadata produced by transformations, masking, validation, and
/// enrichment.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ActiveMetadata {
    fields: BTreeMap<MetadataFieldName, MetadataValue>,
}

impl ActiveMetadata {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(mut self, name: impl Into<MetadataFieldName>, value: MetadataValue) -> Self {
        self.fields.insert(name.into(), value);
        self
    }

    pub fn fields(&self) -> &BTreeMap<MetadataFieldName, MetadataValue> {
        &self.fields
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataOperation {
    Created,
    Received,
    Transformed {
        plugin_name: Option<PluginName>,
        plugin_version: Option<PluginVersion>,
    },
    Masked {
        fields: Vec<String>,
    },
    Validated {
        passed: bool,
    },
    Enriched {
        source: Option<String>,
    },
    Routed,
    Persisted,
}

/// Provenance event for one flowing data item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataIndividualMetadataEvent {
    metadata_event_id: MetadataEventId,
    data_individual_id: DataIndividualId,
    data_individual_shard_id: DataIndividualShardId,
    tenant_id: TenantId,
    environment_id: EnvironmentId,
    pipeline_id: PipelineId,
    process_instance_id: ProcessInstanceId,
    activity_id: Option<ActivityId>,
    timestamp: EventTimestamp,
    operation: MetadataOperation,
    passive_metadata: PassiveMetadata,
    active_metadata: ActiveMetadata,
    source_payload_ref: Option<PayloadRef>,
    idempotency_key: IdempotencyKey,
}

impl DataIndividualMetadataEvent {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        metadata_event_id: MetadataEventId,
        data_individual_id: DataIndividualId,
        data_individual_shard_id: DataIndividualShardId,
        tenant_id: TenantId,
        environment_id: EnvironmentId,
        pipeline_id: PipelineId,
        process_instance_id: ProcessInstanceId,
        timestamp: EventTimestamp,
        operation: MetadataOperation,
    ) -> Self {
        let idempotency_key = IdempotencyKey::from(format!(
            "{}:{}",
            data_individual_id.as_str(),
            metadata_event_id.as_str()
        ));
        Self {
            metadata_event_id,
            data_individual_id,
            data_individual_shard_id,
            tenant_id,
            environment_id,
            pipeline_id,
            process_instance_id,
            activity_id: None,
            timestamp,
            operation,
            passive_metadata: PassiveMetadata::new(),
            active_metadata: ActiveMetadata::new(),
            source_payload_ref: None,
            idempotency_key,
        }
    }

    pub fn with_activity_id(mut self, activity_id: ActivityId) -> Self {
        self.activity_id = Some(activity_id);
        self
    }

    pub fn with_passive_metadata(mut self, passive_metadata: PassiveMetadata) -> Self {
        self.passive_metadata = passive_metadata;
        self
    }

    pub fn with_active_metadata(mut self, active_metadata: ActiveMetadata) -> Self {
        self.active_metadata = active_metadata;
        self
    }

    pub fn with_source_payload_ref(mut self, source_payload_ref: PayloadRef) -> Self {
        self.source_payload_ref = Some(source_payload_ref);
        self
    }

    pub fn metadata_event_id(&self) -> &MetadataEventId {
        &self.metadata_event_id
    }

    pub fn data_individual_id(&self) -> &DataIndividualId {
        &self.data_individual_id
    }

    pub fn data_individual_shard_id(&self) -> DataIndividualShardId {
        self.data_individual_shard_id
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

    pub fn process_instance_id(&self) -> &ProcessInstanceId {
        &self.process_instance_id
    }

    pub fn activity_id(&self) -> Option<&ActivityId> {
        self.activity_id.as_ref()
    }

    pub fn timestamp(&self) -> &EventTimestamp {
        &self.timestamp
    }

    pub fn operation(&self) -> &MetadataOperation {
        &self.operation
    }

    pub fn passive_metadata(&self) -> &PassiveMetadata {
        &self.passive_metadata
    }

    pub fn active_metadata(&self) -> &ActiveMetadata {
        &self.active_metadata
    }

    pub fn source_payload_ref(&self) -> Option<&PayloadRef> {
        self.source_payload_ref.as_ref()
    }

    pub fn idempotency_key(&self) -> &IdempotencyKey {
        &self.idempotency_key
    }
}
