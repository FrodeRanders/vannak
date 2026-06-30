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

//! Vannak integration test: full ingest → index → outbox → Ipto writer flow.
//!
//! Requires a PostgreSQL instance with the Ipto schema (see
//! `docker-compose.test.yml`). Set `VANNAK_PG_INTEGRATION=1` to enable.
//!
//! This test simulates a Durga data pipeline (transform → filter → enrich)
//! processing customer data, ingests process events and data-individual
//! metadata events into Vannak, and verifies the hot index state and
//! persisted Ipto metadata.
//!
//! Usage:
//!   docker compose -f docker-compose.test.yml up -d
//!   VANNAK_PG_INTEGRATION=1 cargo test --test vannak_integration -- --nocapture

use std::sync::Arc;

use ipto_rust::backends::postgres::PostgresBackend;
use ipto_rust::backend::Backend;
use ipto_rust::repo::RepoService;

use vannak::data::{ActiveMetadata, DataIndividualMetadataEvent, MetadataOperation, MetadataValue, PassiveMetadata};
use vannak::ingest::{EventId, EventTimestamp, PipelineEvent, SourceId, SourceSequence};
use vannak::index::HotIndex;
use vannak::ipto::{
    MetadataOutbox, IptoMapping, IptoWritePayload, IptoInstanceId,
};
use vannak::ipto_adapter::IptoRepoWriter;
use vannak::process::{
    ActivityId, EnvironmentId, EventKind, PipelineId, ProcessDefinitionId,
    ProcessInstanceId, ProcessStatus, TenantId,
};
use vannak::query::{ProcessInstanceQuery, QueryLimit, EventQuery, QueryResult};

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

fn integration_enabled() -> bool {
    matches!(
        std::env::var("VANNAK_PG_INTEGRATION").as_deref(),
        Ok("1" | "true" | "TRUE" | "yes" | "YES")
    )
}

fn build_durga_pipeline_events() -> Vec<PipelineEvent> {
    let tenant = TenantId::from("tenant-a");
    let env = EnvironmentId::from("prod");
    let pipeline = PipelineId::from("data-pipeline-demo");
    let definition = ProcessDefinitionId::from("data-pipeline-demo");
    let instance = ProcessInstanceId::from("customer-pipeline-run-001");
    let source = SourceId::from("durga-monitor");

    vec![
        // Process start
        PipelineEvent::new(
            EventId::from("evt-001"),
            source.clone(),
            SourceSequence(1),
            tenant.clone(),
            env.clone(),
            pipeline.clone(),
            definition.clone(),
            instance.clone(),
            EventTimestamp::from("2026-06-30T10:00:00.000Z"),
            EventKind::ProcessStarted,
        ),
        // Transform activity enter
        PipelineEvent::new(
            EventId::from("evt-002"),
            source.clone(),
            SourceSequence(2),
            tenant.clone(),
            env.clone(),
            pipeline.clone(),
            definition.clone(),
            instance.clone(),
            EventTimestamp::from("2026-06-30T10:00:01.000Z"),
            EventKind::ActivityEntered,
        )
        .with_activity_id(ActivityId::from("transform_data")),
        // Transform activity complete
        PipelineEvent::new(
            EventId::from("evt-003"),
            source.clone(),
            SourceSequence(3),
            tenant.clone(),
            env.clone(),
            pipeline.clone(),
            definition.clone(),
            instance.clone(),
            EventTimestamp::from("2026-06-30T10:00:03.500Z"),
            EventKind::ActivityCompleted,
        )
        .with_activity_id(ActivityId::from("transform_data")),
        // Filter activity enter
        PipelineEvent::new(
            EventId::from("evt-004"),
            source.clone(),
            SourceSequence(4),
            tenant.clone(),
            env.clone(),
            pipeline.clone(),
            definition.clone(),
            instance.clone(),
            EventTimestamp::from("2026-06-30T10:00:04.000Z"),
            EventKind::ActivityEntered,
        )
        .with_activity_id(ActivityId::from("filter_fields")),
        // Filter activity complete
        PipelineEvent::new(
            EventId::from("evt-005"),
            source.clone(),
            SourceSequence(5),
            tenant.clone(),
            env.clone(),
            pipeline.clone(),
            definition.clone(),
            instance.clone(),
            EventTimestamp::from("2026-06-30T10:00:05.200Z"),
            EventKind::ActivityCompleted,
        )
        .with_activity_id(ActivityId::from("filter_fields")),
        // Enrich activity enter
        PipelineEvent::new(
            EventId::from("evt-006"),
            source.clone(),
            SourceSequence(6),
            tenant.clone(),
            env.clone(),
            pipeline.clone(),
            definition.clone(),
            instance.clone(),
            EventTimestamp::from("2026-06-30T10:00:06.000Z"),
            EventKind::ActivityEntered,
        )
        .with_activity_id(ActivityId::from("enrich_data")),
        // Enrich activity complete
        PipelineEvent::new(
            EventId::from("evt-007"),
            source,
            SourceSequence(7),
            tenant.clone(),
            env.clone(),
            pipeline.clone(),
            definition.clone(),
            instance.clone(),
            EventTimestamp::from("2026-06-30T10:00:08.800Z"),
            EventKind::ActivityCompleted,
        )
        .with_activity_id(ActivityId::from("enrich_data")),
        // Process complete
        PipelineEvent::new(
            EventId::from("evt-008"),
            SourceId::from("durga-monitor"),
            SourceSequence(8),
            tenant,
            env,
            pipeline,
            definition,
            instance,
            EventTimestamp::from("2026-06-30T10:00:09.000Z"),
            EventKind::ProcessCompleted,
        ),
    ]
}

fn build_metadata_events() -> Vec<DataIndividualMetadataEvent> {
    let tenant = TenantId::from("tenant-a");
    let env = EnvironmentId::from("prod");
    let pipeline = PipelineId::from("data-pipeline-demo");
    let instance = ProcessInstanceId::from("customer-pipeline-run-001");

    let data_id = vannak::data::DataIndividualId::from("customer-record-42");
    let shard_id = vannak::data::DataIndividualShardId::from_data_individual(&data_id);

    vec![
        // Received: raw customer data ingested
        DataIndividualMetadataEvent::new(
            vannak::data::MetadataEventId::from("meta-001"),
            data_id.clone(),
            shard_id,
            tenant.clone(),
            env.clone(),
            pipeline.clone(),
            instance.clone(),
            EventTimestamp::from("2026-06-30T10:00:00.500Z"),
            MetadataOperation::Received,
        )
        .with_passive_metadata(
            PassiveMetadata::new()
                .insert("vannak:dataIndividualId", MetadataValue::string("customer-record-42"))
                .insert("vannak:pipelineId", MetadataValue::string("data-pipeline-demo"))
                .insert("vannak:processInstanceId", MetadataValue::string("customer-pipeline-run-001"))
                .insert("vannak:tenantId", MetadataValue::string("tenant-a"))
                .insert("vannak:environmentId", MetadataValue::string("prod")),
        ),

        // Transformed: name, email, data.amount extracted
        DataIndividualMetadataEvent::new(
            vannak::data::MetadataEventId::from("meta-002"),
            data_id.clone(),
            shard_id,
            tenant.clone(),
            env.clone(),
            pipeline.clone(),
            instance.clone(),
            EventTimestamp::from("2026-06-30T10:00:03.500Z"),
            MetadataOperation::Transformed {
                plugin_name: Some(vannak::data::PluginName::from("json-transform")),
                plugin_version: Some(vannak::data::PluginVersion::from("1.0")),
            },
        )
        .with_activity_id(ActivityId::from("transform_data"))
        .with_active_metadata(
            ActiveMetadata::new()
                .insert("vannak:activityId", MetadataValue::string("transform_data")),
        ),

        // Validated: schema check passed
        DataIndividualMetadataEvent::new(
            vannak::data::MetadataEventId::from("meta-003"),
            data_id.clone(),
            shard_id,
            tenant.clone(),
            env.clone(),
            pipeline.clone(),
            instance.clone(),
            EventTimestamp::from("2026-06-30T10:00:05.200Z"),
            MetadataOperation::Validated { passed: true },
        )
        .with_activity_id(ActivityId::from("filter_fields"))
        .with_passive_metadata(
            PassiveMetadata::new()
                .insert("vannak:activityId", MetadataValue::string("filter_fields")),
        ),

        // Enriched: KV lookup by email
        DataIndividualMetadataEvent::new(
            vannak::data::MetadataEventId::from("meta-004"),
            data_id.clone(),
            shard_id,
            tenant,
            env,
            pipeline,
            instance,
            EventTimestamp::from("2026-06-30T10:00:08.800Z"),
            MetadataOperation::Enriched {
                source: Some("crm-lookup".into()),
            },
        )
        .with_activity_id(ActivityId::from("enrich_data"))
        .with_active_metadata(
            ActiveMetadata::new()
                .insert("vannak:activityId", MetadataValue::string("enrich_data")),
        ),
    ]
}

// ---------------------------------------------------------------------------
// Integration tests
// ---------------------------------------------------------------------------

#[test]
fn full_ingest_index_outbox_ipto_flow() {
    if !integration_enabled() {
        eprintln!("SKIP: VANNAK_PG_INTEGRATION not set");
        return;
    }

    // --- 1. Connect to PostgreSQL ---
    let backend: Arc<dyn Backend> = Arc::new(PostgresBackend::new());
    let repo = Arc::new(RepoService::new(backend));

    // Ensure tenant exists (tenant id 1 = SCRATCH from boot.sql, or create)
    match repo.get_tenant_info("SCRATCH") {
        Ok(Some(_)) => {}
        _ => {
            eprintln!("INFO: tenant SCRATCH not found, will use tenant id 1");
        }
    }

    // --- 2. Configure PROV-O SDL ---
    let mut writer = IptoRepoWriter::new(repo.clone(), 1);
    writer.configure_sdl().expect("SDL configuration should succeed");

    // --- 3. Ingest Durga process events into HotIndex ---
    let mut hot_index = HotIndex::new();
    let pipeline_events = build_durga_pipeline_events();

    for event in &pipeline_events {
        let outcome = hot_index.ingest(event.clone()).expect("ingest should succeed");
        assert!(
            matches!(outcome, vannak::index::IngestOutcome::Accepted),
            "event {} should be accepted",
            event.event_id()
        );
    }

    // --- 4. Verify HotIndex process state ---
    let process = hot_index
        .process_instance(&ProcessInstanceQuery {
            process_instance_id: ProcessInstanceId::from("customer-pipeline-run-001"),
        })
        .expect("process instance should exist");

    assert_eq!(process.status, ProcessStatus::Completed);
    assert_eq!(process.pipeline_id, PipelineId::from("data-pipeline-demo"));
    assert_eq!(process.started_at.as_ref().map(|t| t.as_str()), Some("2026-06-30T10:00:00.000Z"));

    // Activity durations should be computed
    let transform_dur = process.activity_durations.get(&ActivityId::from("transform_data"));
    assert!(transform_dur.is_some(), "transform_data duration should be computed");
    assert!(*transform_dur.unwrap() > 0);

    let enrich_dur = process.activity_durations.get(&ActivityId::from("enrich_data"));
    assert!(enrich_dur.is_some(), "enrich_data duration should be computed");

    // --- 5. Verify HotIndex event query ---
    let events = hot_index.events(&EventQuery {
        process_instance_id: ProcessInstanceId::from("customer-pipeline-run-001"),
        limit: QueryLimit::new(20),
    });
    if let QueryResult::Events(evts) = events {
        assert_eq!(evts.len(), 8, "all 8 process events should be queryable");
    } else {
        panic!("expected Events result");
    }

    // --- 6. Build Ipto write payloads from metadata events ---
    let metadata_events = build_metadata_events();
    let mut outbox = MetadataOutbox::new();

    let mapping = IptoMapping::new("v1")
        .map_field("vannak:dataIndividualId", "vannak:dataIndividualId")
        .map_field("vannak:pipelineId", "vannak:pipelineId")
        .map_field("vannak:processInstanceId", "vannak:processInstanceId")
        .map_field("vannak:tenantId", "vannak:tenantId")
        .map_field("vannak:environmentId", "vannak:environmentId")
        .map_field("vannak:activityId", "vannak:activityId");

    // Create a placement map for the write payloads
    let placement = vannak::cluster::IptoPlacementMap::new(
        vannak::cluster::PlacementEpoch(1),
        vec![vannak::cluster::IptoPlacementSlot::new(
            IptoInstanceId::from("unused"),
            1,
        )
        .unwrap()],
        vec![],
    )
    .unwrap();

    for event in &metadata_events {
        let payload = IptoWritePayload::from_event(event, &placement, &mapping)
            .expect("payload construction should succeed");
        assert!(
            matches!(
                outbox.enqueue(payload),
                vannak::ipto::OutboxEnqueueResult::Enqueued
            ),
            "metadata event {} should enqueue",
            event.metadata_event_id()
        );
    }

    // --- 7. Deliver outbox entries to Ipto ---
    let snapshot_before = outbox.snapshot();
    assert_eq!(snapshot_before.pending, 4);
    assert_eq!(snapshot_before.total, 4);

    for _ in 0..4 {
        let result = vannak::ipto::deliver_next_pending(&mut outbox, &mut writer);
        assert!(
            matches!(result, vannak::ipto::MetadataOutboxDeliveryResult::Acknowledged { .. }),
            "outbox delivery should succeed"
        );
    }

    let snapshot_after = outbox.snapshot();
    assert_eq!(snapshot_after.acknowledged, 4);
    assert_eq!(snapshot_after.pending, 0);

    // --- 8. Verify metadata persisted in Ipto via correlation-id lookup ---
    for event in &metadata_events {
        // The IptoRepoWriter derives a corrid from the IdempotencyKey
        let key = event.idempotency_key();
        let corrid = {
            let mut state: u64 = 0xcbf2_9ce4_8422_2325;
            for byte in key.as_str().as_bytes() {
                state ^= u64::from(*byte);
                state = state.wrapping_mul(0x0000_0100_0000_01b3);
            }
            let hi = {
                let mut s = 0xcbf2_9ce4_8422_2325u64;
                s ^= 1u64;
                s = s.wrapping_mul(0x0000_0100_0000_01b3);
                for byte in key.as_str().as_bytes() {
                    s ^= u64::from(*byte);
                    s = s.wrapping_mul(0x0000_0100_0000_01b3);
                }
                s
            };
            format!(
                "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
                (hi >> 32) as u32,
                (hi >> 16) as u32 & 0xFFFF,
                0x7000 | (hi as u32 & 0x0FFF),
                0x8000 | (state as u32 >> 16 & 0x3FFF),
                state & 0xFFFF_FFFF,
            )
        };

        let unit = repo
            .get_unit_by_corrid_json(&corrid)
            .expect("corrid lookup should succeed");
        assert!(
            unit.is_some(),
            "metadata event {} should be persisted in Ipto (corrid: {})",
            event.metadata_event_id(),
            corrid,
        );
    }

    eprintln!("SUCCESS: full ingest→index→outbox→Ipto flow verified");
}

#[test]
fn duplicate_metadata_events_are_idempotent() {
    if !integration_enabled() {
        eprintln!("SKIP: VANNAK_PG_INTEGRATION not set");
        return;
    }

    let backend: Arc<dyn Backend> = Arc::new(PostgresBackend::new());
    let repo = Arc::new(RepoService::new(backend));
    let mut writer = IptoRepoWriter::new(repo, 1);
    writer.configure_sdl().expect("SDL configuration should succeed");

    let tenant = TenantId::from("tenant-a");
    let env = EnvironmentId::from("prod");
    let pipeline = PipelineId::from("pipeline-a");
    let instance = ProcessInstanceId::from("instance-dup");
    let data_id = vannak::data::DataIndividualId::from("customer-dup");
    let shard_id = vannak::data::DataIndividualShardId::from_data_individual(&data_id);

    let event = DataIndividualMetadataEvent::new(
        vannak::data::MetadataEventId::from("meta-dup"),
        data_id,
        shard_id,
        tenant,
        env,
        pipeline,
        instance,
        EventTimestamp::from("2026-06-30T12:00:00Z"),
        MetadataOperation::Received,
    )
    .with_passive_metadata(
        PassiveMetadata::new()
            .insert("vannak:dataIndividualId", MetadataValue::string("customer-dup")),
    );

    let mapping = IptoMapping::new("v1")
        .map_field("vannak:dataIndividualId", "vannak:dataIndividualId");

    let placement = vannak::cluster::IptoPlacementMap::new(
        vannak::cluster::PlacementEpoch(1),
        vec![vannak::cluster::IptoPlacementSlot::new(
            IptoInstanceId::from("unused"),
            1,
        )
        .unwrap()],
        vec![],
    )
    .unwrap();

    let payload = IptoWritePayload::from_event(&event, &placement, &mapping).unwrap();

    // First write
    let mut outbox = MetadataOutbox::new();
    outbox.enqueue(payload.clone());
    let result = vannak::ipto::deliver_next_pending(&mut outbox, &mut writer);
    assert!(
        matches!(result, vannak::ipto::MetadataOutboxDeliveryResult::Acknowledged { .. }),
        "first write should succeed"
    );

    // Second write of the same payload (idempotent)
    outbox.enqueue(payload.clone());
    // Re-enqueue should detect duplicate
    assert!(matches!(
        outbox.enqueue(payload),
        vannak::ipto::OutboxEnqueueResult::Duplicate
    ));

    eprintln!("SUCCESS: idempotent metadata write verified");
}
