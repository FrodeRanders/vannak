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
use crate::query::{
    ActivityMetadataQuery, DataIndividualMetadataQuery, ProcessMetadataQuery, QueryResult,
};
use std::collections::{BTreeMap, BTreeSet};
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

impl DataIndividualShardId {
    /// Derive a shard ID from a stable data-individual identity.
    ///
    /// Uses a deterministic 64-bit hash of the identity string so shard IDs
    /// are uniformly distributed across the u64 space. This works well with
    /// consistent-hash ring placement.
    ///
    /// Callers may still assign an explicit `DataIndividualShardId` when
    /// the domain placement key carries business meaning (e.g. scoping all
    /// data for a tenant to a dedicated shard range).
    pub fn from_data_individual(data_individual_id: &DataIndividualId) -> Self {
        let mut state = 0xcbf2_9ce4_8422_2325u64;
        for byte in data_individual_id.as_str().as_bytes() {
            state ^= u64::from(*byte);
            state = state.wrapping_mul(0x0000_0100_0000_01b3);
        }
        Self(state)
    }
}

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

/// Shard-local recent provenance lookup index.
///
/// This is not the durable store of record; Ipto remains the durable metadata
/// owner. The index gives the local service owned, recent answers for common
/// operator questions while metadata writes are still delivered through the
/// durable outbox.
#[derive(Debug, Default)]
pub struct DataProvenanceIndex {
    events_by_idempotency: BTreeMap<IdempotencyKey, DataIndividualMetadataEvent>,
    events_by_data_individual: BTreeMap<DataIndividualId, Vec<IdempotencyKey>>,
    events_by_process: BTreeMap<ProcessInstanceId, Vec<IdempotencyKey>>,
    events_by_activity: BTreeMap<(ProcessInstanceId, ActivityId), Vec<IdempotencyKey>>,
    data_individuals_by_process: BTreeMap<ProcessInstanceId, BTreeSet<DataIndividualId>>,
    data_individuals_by_activity:
        BTreeMap<(ProcessInstanceId, ActivityId), BTreeSet<DataIndividualId>>,
}

impl DataProvenanceIndex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn ingest(&mut self, event: DataIndividualMetadataEvent) -> DataProvenanceIngestOutcome {
        if self
            .events_by_idempotency
            .contains_key(event.idempotency_key())
        {
            return DataProvenanceIngestOutcome::Duplicate;
        }

        let key = event.idempotency_key().clone();
        let data_individual_id = event.data_individual_id().clone();
        let process_instance_id = event.process_instance_id().clone();
        let activity_key = event
            .activity_id()
            .cloned()
            .map(|activity_id| (process_instance_id.clone(), activity_id));

        self.events_by_data_individual
            .entry(data_individual_id.clone())
            .or_default()
            .push(key.clone());
        self.events_by_process
            .entry(process_instance_id.clone())
            .or_default()
            .push(key.clone());
        self.data_individuals_by_process
            .entry(process_instance_id)
            .or_default()
            .insert(data_individual_id.clone());

        if let Some(activity_key) = activity_key {
            self.events_by_activity
                .entry(activity_key.clone())
                .or_default()
                .push(key.clone());
            self.data_individuals_by_activity
                .entry(activity_key)
                .or_default()
                .insert(data_individual_id);
        }

        self.events_by_idempotency.insert(key, event);
        DataProvenanceIngestOutcome::Accepted
    }

    pub fn metadata_for_data_individual(&self, query: &DataIndividualMetadataQuery) -> QueryResult {
        QueryResult::MetadataEvents(
            self.events_for_keys(
                self.events_by_data_individual
                    .get(&query.data_individual_id)
                    .into_iter()
                    .flatten()
                    .take(query.limit.value()),
            ),
        )
    }

    pub fn metadata_for_process(&self, query: &ProcessMetadataQuery) -> QueryResult {
        QueryResult::MetadataEvents(
            self.events_for_keys(
                self.events_by_process
                    .get(&query.process_instance_id)
                    .into_iter()
                    .flatten()
                    .take(query.limit.value()),
            ),
        )
    }

    pub fn metadata_for_activity(&self, query: &ActivityMetadataQuery) -> QueryResult {
        QueryResult::MetadataEvents(
            self.events_for_keys(
                self.events_by_activity
                    .get(&(query.process_instance_id.clone(), query.activity_id.clone()))
                    .into_iter()
                    .flatten()
                    .take(query.limit.value()),
            ),
        )
    }

    pub fn data_individuals_for_process(
        &self,
        process_instance_id: &ProcessInstanceId,
    ) -> Vec<DataIndividualId> {
        self.data_individuals_by_process
            .get(process_instance_id)
            .into_iter()
            .flatten()
            .cloned()
            .collect()
    }

    pub fn data_individuals_for_activity(
        &self,
        process_instance_id: &ProcessInstanceId,
        activity_id: &ActivityId,
    ) -> Vec<DataIndividualId> {
        self.data_individuals_by_activity
            .get(&(process_instance_id.clone(), activity_id.clone()))
            .into_iter()
            .flatten()
            .cloned()
            .collect()
    }

    pub fn snapshot(&self) -> DataProvenanceIndexSnapshot {
        DataProvenanceIndexSnapshot {
            metadata_event_count: self.events_by_idempotency.len(),
            data_individual_count: self.events_by_data_individual.len(),
            process_instance_count: self.events_by_process.len(),
            activity_count: self.events_by_activity.len(),
        }
    }

    fn events_for_keys<'a>(
        &self,
        keys: impl Iterator<Item = &'a IdempotencyKey>,
    ) -> Vec<DataIndividualMetadataEvent> {
        keys.filter_map(|key| self.events_by_idempotency.get(key).cloned())
            .collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataProvenanceIngestOutcome {
    Accepted,
    Duplicate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DataProvenanceIndexSnapshot {
    pub metadata_event_count: usize,
    pub data_individual_count: usize,
    pub process_instance_count: usize,
    pub activity_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::process::{EnvironmentId, PipelineId, TenantId};

    #[test]
    fn provenance_index_queries_by_data_process_and_activity() {
        let mut index = DataProvenanceIndex::new();
        let first = metadata_event("data-1", "meta-1", "instance-a", Some("extract"));
        let second = metadata_event("data-1", "meta-2", "instance-a", Some("load"));
        let third = metadata_event("data-2", "meta-3", "instance-a", Some("extract"));

        assert_eq!(
            index.ingest(first.clone()),
            DataProvenanceIngestOutcome::Accepted
        );
        assert_eq!(
            index.ingest(second.clone()),
            DataProvenanceIngestOutcome::Accepted
        );
        assert_eq!(
            index.ingest(third.clone()),
            DataProvenanceIngestOutcome::Accepted
        );

        let QueryResult::MetadataEvents(data_events) =
            index.metadata_for_data_individual(&DataIndividualMetadataQuery {
                data_individual_id: DataIndividualId::from("data-1"),
                limit: crate::query::QueryLimit::new(10),
            })
        else {
            panic!("data query returns metadata events");
        };
        assert_eq!(data_events, vec![first.clone(), second.clone()]);

        let QueryResult::MetadataEvents(activity_events) =
            index.metadata_for_activity(&ActivityMetadataQuery {
                process_instance_id: ProcessInstanceId::from("instance-a"),
                activity_id: ActivityId::from("extract"),
                limit: crate::query::QueryLimit::new(10),
            })
        else {
            panic!("activity query returns metadata events");
        };
        assert_eq!(activity_events, vec![first, third]);

        assert_eq!(
            index.data_individuals_for_process(&ProcessInstanceId::from("instance-a")),
            vec![
                DataIndividualId::from("data-1"),
                DataIndividualId::from("data-2")
            ]
        );
        assert_eq!(
            index.snapshot(),
            DataProvenanceIndexSnapshot {
                metadata_event_count: 3,
                data_individual_count: 2,
                process_instance_count: 1,
                activity_count: 2,
            }
        );
    }

    #[test]
    fn provenance_index_is_idempotent() {
        let mut index = DataProvenanceIndex::new();
        let event = metadata_event("data-1", "meta-1", "instance-a", None);

        assert_eq!(
            index.ingest(event.clone()),
            DataProvenanceIngestOutcome::Accepted
        );
        assert_eq!(index.ingest(event), DataProvenanceIngestOutcome::Duplicate);
        assert_eq!(index.snapshot().metadata_event_count, 1);
    }

    fn metadata_event(
        data_id: &str,
        metadata_event_id: &str,
        process_instance_id: &str,
        activity_id: Option<&str>,
    ) -> DataIndividualMetadataEvent {
        let data_id = DataIndividualId::from(data_id);
        let shard_id = DataIndividualShardId::from_data_individual(&data_id);
        let event = DataIndividualMetadataEvent::new(
            MetadataEventId::from(metadata_event_id),
            data_id,
            shard_id,
            TenantId::from("tenant-a"),
            EnvironmentId::from("prod"),
            PipelineId::from("pipeline-a"),
            ProcessInstanceId::from(process_instance_id),
            EventTimestamp::from("2026-06-30T10:00:00Z"),
            MetadataOperation::Received,
        );
        if let Some(activity_id) = activity_id {
            event.with_activity_id(ActivityId::from(activity_id))
        } else {
            event
        }
    }
}
