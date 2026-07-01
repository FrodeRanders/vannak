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

//! Kafka-to-Sitas process-event ingest runner.

use std::path::PathBuf;
use std::time::Duration;

use vannak::{
    EnvironmentId, KafkaPayloadFormat, KafkaProcessConsumer, KafkaProcessConsumerConfig, NodeId,
    ProcessEventJournal, SegmentId, SitasRuntimeConfig, SitasShardRuntime, SourceId, TenantId,
};

fn main() {
    if let Err(error) = run() {
        eprintln!("vannak-kafka-ingest: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let config = CliConfig::parse(std::env::args().skip(1).collect())?;
    let mut runtime = SitasShardRuntime::start(SitasRuntimeConfig::new(
        config.shards,
        config.mailbox_capacity,
    ))
    .map_err(|error| error.to_string())?;
    runtime
        .start_mailbox_workers()
        .map_err(|error| error.to_string())?;

    let mut consumer = KafkaProcessConsumer::start(
        KafkaProcessConsumerConfig::new(config.brokers, config.group_id, config.topics.clone())
            .with_poll_timeout(Duration::from_millis(config.poll_timeout_ms))
            .with_payload_format(config.payload_format)
            .with_durga_context(config.source_id, config.tenant_id, config.environment_id),
    )
    .map_err(|error| error.to_string())?;

    let mut journal = match config.journal {
        Some(journal) => Some(open_or_create_journal(journal)?),
        None => None,
    };

    eprintln!(
        "subscribed to {} topic(s), Sitas shards={}, mailbox_capacity={}",
        config.topics.len(),
        runtime.shard_count(),
        config.mailbox_capacity
    );

    let mut accepted = 0u64;
    loop {
        let outcome = match journal.as_mut() {
            Some(journal) => consumer.poll_once(&runtime, Some(journal)),
            None => consumer.poll_once(&runtime, None),
        }
        .map_err(|error| error.to_string())?;

        let Some(outcome) = outcome else {
            continue;
        };
        accepted += 1;
        if accepted == 1 || accepted.is_multiple_of(config.report_interval) {
            let snapshot = runtime.snapshot().map_err(|error| error.to_string())?;
            let consumer_snapshot = consumer.snapshot();
            eprintln!(
                "accepted={} topic={} partition={} next_commit_offset={} hot_events={} queued={} pending={} paused={} assignments={} revocations={}",
                accepted,
                outcome.topic_partition.topic(),
                outcome.topic_partition.partition(),
                outcome.next_commit_offset.0,
                snapshot.runtime.totals.event_count,
                snapshot
                    .mailboxes
                    .iter()
                    .map(|mailbox| mailbox.len)
                    .sum::<usize>(),
                consumer_snapshot.pending_records,
                consumer_snapshot.paused_partitions.len(),
                consumer_snapshot.rebalance.assignment_count,
                consumer_snapshot.rebalance.revocation_count
            );
        }
    }
}

fn open_or_create_journal(config: JournalConfig) -> Result<ProcessEventJournal, String> {
    if config.path.exists() {
        ProcessEventJournal::recover(&config.path, config.segment_id, config.node_id)
            .map(|recovery| recovery.journal)
            .map_err(|error| error.to_string())
    } else {
        ProcessEventJournal::create(&config.path, config.segment_id, config.node_id)
            .map_err(|error| error.to_string())
    }
}

#[derive(Debug)]
struct CliConfig {
    brokers: String,
    group_id: String,
    shards: usize,
    mailbox_capacity: usize,
    poll_timeout_ms: u64,
    report_interval: u64,
    payload_format: KafkaPayloadFormat,
    source_id: SourceId,
    tenant_id: TenantId,
    environment_id: EnvironmentId,
    journal: Option<JournalConfig>,
    topics: Vec<String>,
}

#[derive(Debug)]
struct JournalConfig {
    path: PathBuf,
    segment_id: SegmentId,
    node_id: NodeId,
}

impl CliConfig {
    fn parse(args: Vec<String>) -> Result<Self, String> {
        if args.len() < 5 {
            return Err(usage());
        }

        let brokers = args[0].clone();
        let group_id = args[1].clone();
        let shards = parse_positive_usize(&args[2], "shards")?;
        let mailbox_capacity = parse_positive_usize(&args[3], "mailbox-capacity")?;
        let mut poll_timeout_ms = 100u64;
        let mut report_interval = 1000u64;
        let mut payload_format = KafkaPayloadFormat::VannakBinary;
        let mut source_id = SourceId::from("kafka");
        let mut tenant_id = TenantId::from("default");
        let mut environment_id = EnvironmentId::from("default");
        let mut journal = None;
        let mut topics = Vec::new();
        let mut idx = 4;

        while idx < args.len() {
            match args[idx].as_str() {
                "--journal" => {
                    if idx + 3 >= args.len() {
                        return Err(usage());
                    }
                    journal = Some(JournalConfig {
                        path: PathBuf::from(&args[idx + 1]),
                        segment_id: SegmentId::from(args[idx + 2].clone()),
                        node_id: NodeId::from(args[idx + 3].clone()),
                    });
                    idx += 4;
                }
                "--poll-timeout-ms" => {
                    if idx + 1 >= args.len() {
                        return Err(usage());
                    }
                    poll_timeout_ms = args[idx + 1]
                        .parse()
                        .map_err(|_| "poll-timeout-ms must be a positive integer".to_string())?;
                    idx += 2;
                }
                "--report-interval" => {
                    if idx + 1 >= args.len() {
                        return Err(usage());
                    }
                    report_interval = args[idx + 1]
                        .parse()
                        .map_err(|_| "report-interval must be a positive integer".to_string())?;
                    if report_interval == 0 {
                        return Err("report-interval must be greater than zero".to_string());
                    }
                    idx += 2;
                }
                "--payload-format" => {
                    if idx + 1 >= args.len() {
                        return Err(usage());
                    }
                    payload_format =
                        KafkaPayloadFormat::parse(&args[idx + 1]).ok_or_else(|| {
                            "payload-format must be vannak-binary or durga-json".to_string()
                        })?;
                    idx += 2;
                }
                "--source-id" => {
                    if idx + 1 >= args.len() {
                        return Err(usage());
                    }
                    source_id = SourceId::from(args[idx + 1].clone());
                    idx += 2;
                }
                "--tenant-id" => {
                    if idx + 1 >= args.len() {
                        return Err(usage());
                    }
                    tenant_id = TenantId::from(args[idx + 1].clone());
                    idx += 2;
                }
                "--environment-id" => {
                    if idx + 1 >= args.len() {
                        return Err(usage());
                    }
                    environment_id = EnvironmentId::from(args[idx + 1].clone());
                    idx += 2;
                }
                value if value.starts_with("--") => return Err(format!("unknown option {value}")),
                topic => {
                    topics.push(topic.to_string());
                    idx += 1;
                }
            }
        }

        if topics.is_empty() {
            return Err(usage());
        }

        Ok(Self {
            brokers,
            group_id,
            shards,
            mailbox_capacity,
            poll_timeout_ms,
            report_interval,
            payload_format,
            source_id,
            tenant_id,
            environment_id,
            journal,
            topics,
        })
    }
}

fn parse_positive_usize(value: &str, label: &str) -> Result<usize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|_| format!("{label} must be a positive integer"))?;
    if parsed == 0 {
        return Err(format!("{label} must be greater than zero"));
    }
    Ok(parsed)
}

fn usage() -> String {
    "usage: vannak-kafka-ingest <brokers> <group-id> <shards> <mailbox-capacity> [--payload-format vannak-binary|durga-json] [--source-id <id>] [--tenant-id <id>] [--environment-id <id>] [--journal <path> <segment-id> <node-id>] [--poll-timeout-ms <ms>] [--report-interval <n>] <topic>..."
        .to_string()
}
