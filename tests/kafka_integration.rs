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

//! Kafka integration test for the Kafka -> Sitas process-event path.
//!
//! Requires a Kafka-compatible broker, for example `docker-compose.kafka.yml`.
//! Set `VANNAK_KAFKA_INTEGRATION=1` to enable.
//!
//! Usage:
//!   docker compose -f docker-compose.kafka.yml up -d --wait redpanda
//!   VANNAK_KAFKA_INTEGRATION=1 cargo test --features kafka-client --test kafka_integration -- --nocapture
//!   docker compose -f docker-compose.kafka.yml down -v

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rdkafka::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};

use vannak::durga_process_events_topic;
use vannak::{
    EnvironmentId, KafkaPayloadFormat, KafkaProcessConsumer, KafkaProcessConsumerConfig, SourceId,
    TenantId,
};
use vannak::{SitasRuntimeConfig, SitasShardRuntime};

fn integration_enabled() -> bool {
    matches!(
        std::env::var("VANNAK_KAFKA_INTEGRATION").as_deref(),
        Ok("1" | "true" | "TRUE" | "yes" | "YES")
    )
}

fn kafka_brokers() -> String {
    std::env::var("VANNAK_KAFKA_BROKERS").unwrap_or_else(|_| "localhost:19092".to_string())
}

#[test]
fn kafka_durga_json_record_feeds_sitas_workers() {
    if !integration_enabled() {
        eprintln!("SKIP: VANNAK_KAFKA_INTEGRATION not set");
        return;
    }

    let brokers = kafka_brokers();
    let suffix = unique_suffix();
    let process_id = format!("pipeline-kafka-smoke-{suffix}");
    let topic = durga_process_events_topic(&process_id);
    let group_id = format!("vannak-kafka-integration-{suffix}");

    produce_durga_json_record(&brokers, &topic, &process_id);

    let mut runtime = SitasShardRuntime::start(SitasRuntimeConfig::new(2, 16)).unwrap();
    runtime.start_mailbox_workers().unwrap();
    let runtime = Arc::new(runtime);
    let dir = std::env::temp_dir().join(format!("vannak-kafka-service-{suffix}"));
    std::fs::create_dir_all(&dir).unwrap();
    let journal_path = dir.join("process.seg");
    let mut service = vannak::VannakService::create(
        vannak::IptoPlacementMap::new(
            vannak::PlacementEpoch(1),
            vec![
                vannak::IptoPlacementSlot::new(vannak::IptoInstanceId::from("ipto-a"), 1).unwrap(),
            ],
            vec![],
        )
        .unwrap(),
        vannak::IptoMapping::new("v1"),
        dir.join("outbox.seg"),
        vannak::SegmentId::from("outbox"),
        vannak::NodeId::from("node-a"),
    )
    .unwrap()
    .with_sitas_runtime(runtime.clone())
    .unwrap()
    .with_process_event_journal(
        vannak::ProcessEventJournal::create(
            &journal_path,
            vannak::SegmentId::from("process"),
            vannak::NodeId::from("node-a"),
        )
        .unwrap(),
    );
    let mut consumer = KafkaProcessConsumer::start(
        KafkaProcessConsumerConfig::new(&brokers, group_id, [topic.clone()])
            .with_payload_format(KafkaPayloadFormat::DurgaJson)
            .with_poll_timeout(Duration::from_millis(100))
            .with_auto_offset_reset("earliest")
            .with_durga_context(
                SourceId::from("durga-kafka"),
                TenantId::from("tenant-a"),
                EnvironmentId::from("prod"),
            ),
    )
    .unwrap();

    let deadline = Instant::now() + Duration::from_secs(20);
    let mut accepted = None;
    while Instant::now() < deadline {
        if let Some(outcome) = consumer
            .poll_with_sink(|pending| service.submit_kafka_record(pending))
            .unwrap()
        {
            accepted = Some(outcome);
            break;
        }
    }

    let outcome = accepted.expect("Kafka record should be accepted before deadline");
    assert_eq!(outcome.topic_partition.topic(), topic);
    assert_eq!(outcome.next_commit_offset.0, outcome.offset.0 + 1);

    wait_for_hot_events(&runtime, 1);
    let snapshot = runtime.snapshot().unwrap();
    assert_eq!(snapshot.runtime.totals.event_count, 1);
    assert!(consumer.snapshot().rebalance.assignment_count >= 1);

    assert!(outcome.journal_offset.is_some());
    assert_eq!(
        vannak::replay_process_event_segment(&journal_path)
            .unwrap()
            .events
            .len(),
        1
    );
    assert!(
        service
            .process_instance(&vannak::ProcessInstanceQuery {
                process_instance_id: vannak::ProcessInstanceId::from("instance-kafka-smoke")
            })
            .unwrap()
            .is_some()
    );
    drop(service);
    let mut runtime = Arc::try_unwrap(runtime).ok().unwrap();
    let summaries = runtime.stop_mailbox_workers().unwrap();
    assert_eq!(summaries.iter().map(|(_, s)| s.accepted).sum::<u64>(), 1);
    runtime.stop().unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

fn produce_durga_json_record(brokers: &str, topic: &str, process_id: &str) {
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "10000")
        .create()
        .unwrap();
    let payload = format!(
        r#"{{
        "processInstanceId": "instance-kafka-smoke",
        "processId": "{process_id}",
        "activityId": "extract",
        "tokenId": "token-1",
        "correlationId": "corr-1",
        "payload": {{"records": 1}},
        "status": "STARTED",
        "eventType": "ACTIVITY_ENTERED",
        "processVersion": "v1",
        "businessKey": "business-1",
        "timestamp": "2026-06-30T10:00:00Z"
    }}"#
    );
    producer
        .send(
            BaseRecord::to(topic)
                .key("instance-kafka-smoke")
                .payload(payload.as_bytes()),
        )
        .map_err(|(error, _)| error)
        .unwrap();
    producer.flush(Duration::from_secs(10)).unwrap();
}

fn wait_for_hot_events(runtime: &SitasShardRuntime, expected: usize) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if runtime.snapshot().unwrap().runtime.totals.event_count == expected {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        runtime.snapshot().unwrap().runtime.totals.event_count,
        expected
    );
}

fn unique_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{}-{nanos}", std::process::id())
}
