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

//! Vannak load test — simulates high-throughput ingest from a
//! data-pipeline (transform → filter → enrich pattern, matching
//! `data_pipeline_demo.bpmn`) and measures throughput.
//!
//! Usage:
//!   vannak-load [options]
//!
//! Options:
//!   --pipelines N      number of pipeline instances (default: 100)
//!   --events N         process events per pipeline (default: 8)
//!   --metadata N       metadata events per pipeline (default: 4)
//!   --workers N        concurrency (default: 1)
//!   --report-interval N  print progress every N pipelines (default: 10)
//!   --with-ipto        also write to Ipto backend (needs PostgreSQL)
//!
//! The test runs entirely in-process — no network IO except when
//! --with-ipto is set and a PostgreSQL instance is available.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use vannak::data::{
    ActiveMetadata, DataIndividualId, DataIndividualMetadataEvent, DataIndividualShardId,
    MetadataEventId, MetadataOperation, MetadataValue, PassiveMetadata, PluginName, PluginVersion,
};
use vannak::index::HotIndex;
use vannak::ingest::{EventId, EventTimestamp, PipelineEvent, SourceId, SourceSequence};
use vannak::ipto::{IptoInstanceId, IptoMapping, IptoWritePayload};

#[cfg(feature = "ipto-writer")]
use vannak::ipto::MetadataOutbox;
use vannak::process::{
    ActivityId, EnvironmentId, EventKind, PipelineId, ProcessDefinitionId, ProcessInstanceId,
    TenantId,
};

#[cfg(feature = "ipto-writer")]
use {
    ipto_rust::backend::Backend, ipto_rust::backends::postgres::PostgresBackend,
    ipto_rust::repo::RepoService, std::sync::Mutex, vannak::ipto_adapter::IptoRepoWriter,
};

// ---------------------------------------------------------------------------
// Event generators
// ---------------------------------------------------------------------------

fn make_pipeline_id(i: u64) -> PipelineId {
    PipelineId::from(format!("data-pipeline-demo-{}", i % 10))
}

fn make_instance_id(i: u64) -> ProcessInstanceId {
    ProcessInstanceId::from(format!("pipeline-run-{:06}", i))
}

fn make_data_id(i: u64, step: u64) -> DataIndividualId {
    DataIndividualId::from(format!("customer-record-{:06}-step-{}", i, step))
}

fn make_process_events(instance: &ProcessInstanceId, pipeline: &PipelineId) -> Vec<PipelineEvent> {
    let tenant = TenantId::from("load-test");
    let env = EnvironmentId::from("perf");
    let source = SourceId::from("load-generator");
    let definition = ProcessDefinitionId::from(pipeline.as_str());
    let activities = ["transform_data", "filter_fields", "enrich_data"];
    let base_ts = 1_700_000_000_000u64; // ~Nov 2023

    let mut events = Vec::with_capacity(8);

    // Process start
    events.push(PipelineEvent::new(
        EventId::from(format!("evt-start-{}", instance.as_str())),
        source.clone(),
        SourceSequence(0),
        tenant.clone(),
        env.clone(),
        pipeline.clone(),
        definition.clone(),
        instance.clone(),
        EventTimestamp::from(format_ts(base_ts)),
        EventKind::ProcessStarted,
    ));

    // For each activity: entered + completed
    for (idx, activity) in activities.iter().enumerate() {
        let seq_base = (idx as u64 * 2) + 1;
        events.push(
            PipelineEvent::new(
                EventId::from(format!("evt-enter-{}-{}", instance.as_str(), activity)),
                source.clone(),
                SourceSequence(seq_base),
                tenant.clone(),
                env.clone(),
                pipeline.clone(),
                definition.clone(),
                instance.clone(),
                EventTimestamp::from(format_ts(base_ts + seq_base * 500)),
                EventKind::ActivityEntered,
            )
            .with_activity_id(ActivityId::from(*activity)),
        );
        events.push(
            PipelineEvent::new(
                EventId::from(format!("evt-done-{}-{}", instance.as_str(), activity)),
                source.clone(),
                SourceSequence(seq_base + 1),
                tenant.clone(),
                env.clone(),
                pipeline.clone(),
                definition.clone(),
                instance.clone(),
                EventTimestamp::from(format_ts(base_ts + (seq_base + 1) * 500)),
                EventKind::ActivityCompleted,
            )
            .with_activity_id(ActivityId::from(*activity)),
        );
    }

    // Process complete
    events.push(PipelineEvent::new(
        EventId::from(format!("evt-end-{}", instance.as_str())),
        source,
        SourceSequence(8),
        tenant,
        env,
        pipeline.clone(),
        definition,
        instance.clone(),
        EventTimestamp::from(format_ts(base_ts + 4500)),
        EventKind::ProcessCompleted,
    ));

    events
}

fn make_metadata_events(
    instance: &ProcessInstanceId,
    pipeline: &PipelineId,
    i: u64,
) -> Vec<DataIndividualMetadataEvent> {
    let tenant = TenantId::from("load-test");
    let env = EnvironmentId::from("perf");
    let base_ts = 1_700_000_000_000u64;
    let data_id = make_data_id(i, 0);
    let shard_id = DataIndividualShardId::from_data_individual(&data_id);

    vec![
        // Received
        DataIndividualMetadataEvent::new(
            MetadataEventId::from(format!("meta-rec-{:06}", i)),
            data_id.clone(),
            shard_id,
            tenant.clone(),
            env.clone(),
            pipeline.clone(),
            instance.clone(),
            EventTimestamp::from(format_ts(base_ts + 250)),
            MetadataOperation::Received,
        )
        .with_passive_metadata(
            PassiveMetadata::new()
                .insert(
                    "vannak:dataIndividualId",
                    MetadataValue::string(data_id.as_str()),
                )
                .insert(
                    "vannak:pipelineId",
                    MetadataValue::string(pipeline.as_str()),
                )
                .insert(
                    "vannak:processInstanceId",
                    MetadataValue::string(instance.as_str()),
                )
                .insert("vannak:tenantId", MetadataValue::string("load-test"))
                .insert("vannak:environmentId", MetadataValue::string("perf")),
        ),
        // Transformed
        DataIndividualMetadataEvent::new(
            MetadataEventId::from(format!("meta-xform-{:06}", i)),
            data_id.clone(),
            shard_id,
            tenant.clone(),
            env.clone(),
            pipeline.clone(),
            instance.clone(),
            EventTimestamp::from(format_ts(base_ts + 1500)),
            MetadataOperation::Transformed {
                plugin_name: Some(PluginName::from("json-transform")),
                plugin_version: Some(PluginVersion::from("1.0")),
            },
        )
        .with_activity_id(ActivityId::from("transform_data"))
        .with_active_metadata(
            ActiveMetadata::new()
                .insert("vannak:activityId", MetadataValue::string("transform_data")),
        ),
        // Validated
        DataIndividualMetadataEvent::new(
            MetadataEventId::from(format!("meta-valid-{:06}", i)),
            data_id.clone(),
            shard_id,
            tenant.clone(),
            env.clone(),
            pipeline.clone(),
            instance.clone(),
            EventTimestamp::from(format_ts(base_ts + 2500)),
            MetadataOperation::Validated { passed: true },
        )
        .with_activity_id(ActivityId::from("filter_fields")),
        // Enriched
        DataIndividualMetadataEvent::new(
            MetadataEventId::from(format!("meta-enrich-{:06}", i)),
            data_id,
            shard_id,
            tenant,
            env,
            pipeline.clone(),
            instance.clone(),
            EventTimestamp::from(format_ts(base_ts + 4000)),
            MetadataOperation::Enriched {
                source: Some("crm-lookup".into()),
            },
        )
        .with_activity_id(ActivityId::from("enrich_data"))
        .with_active_metadata(
            ActiveMetadata::new().insert("vannak:activityId", MetadataValue::string("enrich_data")),
        ),
    ]
}

fn format_ts(millis: u64) -> String {
    let secs = millis / 1000;
    let ms = millis % 1000;
    // Simple ISO-ish timestamp (not calendar-accurate, but valid format for the parser)
    format!("2026-06-30T12:00:{:02}.{:03}Z", secs % 60, ms)
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

struct Stats {
    process_events: AtomicU64,
    metadata_events: AtomicU64,
    index_ingest_ns: AtomicU64,
    metadata_payload_ns: AtomicU64,
    outbox_delivery_ns: AtomicU64,
}

impl Stats {
    fn new() -> Self {
        Self {
            process_events: AtomicU64::new(0),
            metadata_events: AtomicU64::new(0),
            index_ingest_ns: AtomicU64::new(0),
            metadata_payload_ns: AtomicU64::new(0),
            outbox_delivery_ns: AtomicU64::new(0),
        }
    }

    fn report(&self, elapsed: f64) {
        let pe = self.process_events.load(Ordering::Relaxed);
        let me = self.metadata_events.load(Ordering::Relaxed);
        let ingest_ns = self.index_ingest_ns.load(Ordering::Relaxed);
        let payload_ns = self.metadata_payload_ns.load(Ordering::Relaxed);
        let delivery_ns = self.outbox_delivery_ns.load(Ordering::Relaxed);

        println!();
        println!("═══════════════════════════════════════════");
        println!("  Load test results");
        println!("═══════════════════════════════════════════");
        println!("  Duration:           {:.2}s", elapsed);
        println!("  Process events:     {}", pe);
        println!("  Metadata events:    {}", me);
        println!("  Total events:       {}", pe + me);
        println!("  ───────────────────────────────────────");
        println!("  Process evt/sec:   {:.0}", pe as f64 / elapsed);
        println!("  Metadata evt/sec:  {:.0}", me as f64 / elapsed);
        println!("  Total evt/sec:     {:.0}", (pe + me) as f64 / elapsed);
        if pe > 0 {
            println!(
                "  Index ingest avg:  {:.1} µs",
                (ingest_ns as f64 / pe as f64) / 1000.0
            );
        }
        if me > 0 {
            println!(
                "  Payload build avg: {:.1} µs",
                (payload_ns as f64 / me as f64) / 1000.0
            );
            println!(
                "  Outbox deliver avg:{:.1} µs",
                (delivery_ns as f64 / me as f64) / 1000.0
            );
        }
        println!("═══════════════════════════════════════════");
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut pipelines: u64 = 100;
    let mut proc_events_per: u64 = 8;
    let mut meta_events_per: u64 = 4;
    let mut workers: usize = 1;
    let mut report_interval: u64 = 10;
    let mut with_ipto = false;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--pipelines" => {
                i += 1;
                pipelines = args[i].parse().unwrap_or(100);
            }
            "--events" => {
                i += 1;
                proc_events_per = args[i].parse().unwrap_or(8);
            }
            "--metadata" => {
                i += 1;
                meta_events_per = args[i].parse().unwrap_or(4);
            }
            "--workers" => {
                i += 1;
                workers = args[i].parse().unwrap_or(1);
            }
            "--report-interval" => {
                i += 1;
                report_interval = args[i].parse().unwrap_or(10);
            }
            "--with-ipto" => with_ipto = true,
            _ => {
                eprintln!("unknown arg: {}", args[i]);
                std::process::exit(1);
            }
        }
        i += 1;
    }

    println!("Vannak load test");
    println!("  pipelines:      {}", pipelines);
    println!("  proc events:    {} per pipeline", proc_events_per);
    println!("  meta events:    {} per pipeline", meta_events_per);
    println!("  workers:        {}", workers);
    println!("  with ipto:      {}", with_ipto);
    println!();

    let stats = Arc::new(Stats::new());
    let hot_index = Arc::new(std::sync::Mutex::new(HotIndex::new()));

    #[cfg(feature = "ipto-writer")]
    let ipto_writer: Option<Arc<Mutex<IptoRepoWriter>>> = if with_ipto {
        let backend: Arc<dyn Backend> = Arc::new(PostgresBackend::new());
        let repo = Arc::new(RepoService::new(backend));
        let mut writer = IptoRepoWriter::new(repo, 1);
        match writer.configure_sdl() {
            Ok(()) => {
                println!("  Ipto writer connected and configured.");
                Some(Arc::new(Mutex::new(writer)))
            }
            Err(e) => {
                eprintln!("  WARNING: Ipto SDL configuration failed: {e}");
                eprintln!("  Continuing without Ipto persistence.");
                None
            }
        }
    } else {
        None
    };

    #[cfg(not(feature = "ipto-writer"))]
    if with_ipto {
        eprintln!(
            "  WARNING: --with-ipto requires ipto-writer feature. Build with --features ipto-writer."
        );
    }

    let start = Instant::now();

    if workers <= 1 {
        run_single_threaded(
            pipelines,
            proc_events_per,
            meta_events_per,
            report_interval,
            &stats,
            &hot_index,
            start,
            #[cfg(feature = "ipto-writer")]
            &ipto_writer,
        );
    } else {
        run_multi_threaded(
            pipelines,
            proc_events_per,
            meta_events_per,
            workers,
            report_interval,
            &stats,
            &hot_index,
            #[cfg(feature = "ipto-writer")]
            &ipto_writer,
        );
    }

    let elapsed = start.elapsed().as_secs_f64();
    stats.report(elapsed);

    // Print hot index snapshot
    let snapshot = hot_index.lock().unwrap().snapshot();
    println!(
        "  HotIndex: {} instances, {} events, {} duplicates",
        snapshot.process_instance_count, snapshot.event_count, snapshot.duplicate_events
    );
}

#[allow(clippy::too_many_arguments)]
fn run_single_threaded(
    pipelines: u64,
    proc_events_per: u64,
    meta_events_per: u64,
    report_interval: u64,
    stats: &Stats,
    hot_index: &Arc<std::sync::Mutex<HotIndex>>,
    start: Instant,
    #[cfg(feature = "ipto-writer")] ipto_writer: &Option<Arc<Mutex<IptoRepoWriter>>>,
) {
    for i in 0..pipelines {
        let pipeline = make_pipeline_id(i);
        let instance = make_instance_id(i);

        // Process events
        if proc_events_per > 0 {
            let t0 = Instant::now();
            let events = make_process_events(&instance, &pipeline);
            let count = events.len() as u64;
            {
                let mut idx = hot_index.lock().unwrap();
                for event in events {
                    let _ = idx.ingest(event);
                }
            }
            stats
                .index_ingest_ns
                .fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
            stats.process_events.fetch_add(count, Ordering::Relaxed);
        }

        // Metadata events
        if meta_events_per > 0 {
            #[cfg(feature = "ipto-writer")]
            let mut outbox = MetadataOutbox::new();

            let t0 = Instant::now();
            let events = make_metadata_events(&instance, &pipeline, i);
            let count = events.len() as u64;

            let mapping = IptoMapping::new("v1")
                .map_field("vannak:dataIndividualId", "vannak:dataIndividualId")
                .map_field("vannak:pipelineId", "vannak:pipelineId")
                .map_field("vannak:processInstanceId", "vannak:processInstanceId")
                .map_field("vannak:tenantId", "vannak:tenantId")
                .map_field("vannak:environmentId", "vannak:environmentId")
                .map_field("vannak:activityId", "vannak:activityId")
                .without_relations();

            for event in events {
                let payload = IptoWritePayload::from_event(
                    &event,
                    &IptoInstanceId::from("load-test"),
                    &mapping,
                );

                #[cfg(feature = "ipto-writer")]
                outbox.enqueue(payload);
                let _ = payload; // suppress unused warning without ipto-writer
            }

            stats
                .metadata_payload_ns
                .fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);

            #[cfg(feature = "ipto-writer")]
            if let Some(writer) = ipto_writer.as_ref() {
                let t0 = Instant::now();
                let mut w = writer.lock().unwrap();
                for _ in 0..count {
                    let _ = vannak::ipto::deliver_next_pending(&mut outbox, &mut *w);
                }
                stats
                    .outbox_delivery_ns
                    .fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
            }

            stats.metadata_events.fetch_add(count, Ordering::Relaxed);
        }

        if report_interval > 0 && i > 0 && i % report_interval == 0 {
            let pe = stats.process_events.load(Ordering::Relaxed);
            let me = stats.metadata_events.load(Ordering::Relaxed);
            let elapsed = start.elapsed().as_secs_f64();
            println!(
                "  [{:>6}/{:>6}] {:>8} process + {:>8} metadata events ({:.0} evt/s)",
                i,
                pipelines,
                pe,
                me,
                (pe + me) as f64 / elapsed.max(0.001)
            );
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn run_multi_threaded(
    pipelines: u64,
    proc_events_per: u64,
    meta_events_per: u64,
    workers: usize,
    report_interval: u64,
    stats: &Arc<Stats>,
    hot_index: &Arc<std::sync::Mutex<HotIndex>>,
    #[cfg(feature = "ipto-writer")] ipto_writer: &Option<Arc<Mutex<IptoRepoWriter>>>,
) {
    let chunk_size = (pipelines as usize).div_ceil(workers) as u64;
    let mut handles = Vec::new();

    for w in 0..workers {
        let start_idx = w as u64 * chunk_size;
        let end_idx = std::cmp::min(start_idx + chunk_size, pipelines);
        let stats = Arc::clone(stats);
        let hot_index = Arc::clone(hot_index);

        #[cfg(feature = "ipto-writer")]
        let ipto_writer = ipto_writer.clone();

        let h = std::thread::spawn(move || {
            run_single_threaded(
                end_idx - start_idx,
                proc_events_per,
                meta_events_per,
                0, // no per-thread progress reports
                &stats,
                &hot_index,
                Instant::now(), // per-thread start
                #[cfg(feature = "ipto-writer")]
                &ipto_writer,
            );

            // Report thread completion
            let pe = stats.process_events.load(Ordering::Relaxed);
            let me = stats.metadata_events.load(Ordering::Relaxed);
            if report_interval > 0 {
                println!(
                    "  thread {} done: {} process + {} metadata events",
                    w, pe, me
                );
            }
        });
        handles.push(h);
    }

    for h in handles {
        h.join().unwrap();
    }
}
