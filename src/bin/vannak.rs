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

//! Vannak supervised daemon: Raft control plane + service plane + optional
//! Kafka ingest in one process.
//!
//! Usage:
//!   vannak [--raft <host> <port> <peer-id> <data-dir> [peers...]]
//!          [--srv _raft._tcp.vannak.local <host> <port> <peer-id> <data-dir>]
//!          [--kafka <brokers> <group-id> <shards> <mailbox-capacity> [topics...]]
//!          [--health <bind-addr>]
//!          [--outbox <segment-path> <segment-id> <node-id>]
//!          [--writer-interval <secs>]

use std::collections::HashMap;
use std::net::{SocketAddr, ToSocketAddrs};
use std::process;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use graft_core::membership::ClusterConfiguration;
use graft_core::raft_node::{LogStore, PersistentStateStore, RaftNode};
use graft_core::state_machine::StateMachine;
use graft_core::types::Peer;
use graft_runtime::handlers::RaftHandler;
use graft_runtime::runtime::RaftRuntime;
use graft_storage::log_store::FileLogStore;
use graft_storage::state_store::FilePersistentStateStore;
use graft_transport::client::RaftClient;
use tokio::runtime::Runtime;

use vannak::cluster::{
    ClusterControlState, IptoPlacementMap, IptoPlacementSlot, NodeId, PlacementEpoch,
};
use vannak::ipto::{IptoInstanceId, IptoMapping};
use vannak::raft_sm::ClusterStateMachine;
use vannak::service::VannakService;
use vannak::storage::SegmentId;
use vannak::{BackgroundWriterConfig, Daemon, DaemonConfig};

fn main() {
    let args: Vec<String> = std::env::args().collect();

    // probe subcommand: connect to a running node, no daemon startup
    if args.len() >= 2 && args[1] == "probe" {
        if args.len() < 4 {
            eprintln!("Usage: vannak probe <host> <port>");
            process::exit(1);
        }
        let host = &args[2];
        let port: u16 = args[3].parse().unwrap_or(10081);
        #[cfg(feature = "node")]
        probe_node(host, port);
        #[cfg(not(feature = "node"))]
        eprintln!("vannak: probe not available (node feature not enabled)");
        return;
    }

    tracing_subscriber::fmt()
        .with_env_filter("graft=info")
        .with_target(false)
        .try_init()
        .ok();

    if let Err(error) = run() {
        eprintln!("vannak: {error}");
        process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let config = parse_args(&args)?;

    let stop_token = Arc::new(AtomicBool::new(false));

    // -- signal handler for graceful shutdown --
    {
        let stop = stop_token.clone();
        std::thread::spawn(move || {
            // Simple: just wait for stdin close or park
            std::thread::park();
            stop.store(true, Ordering::Relaxed);
        });
    }

    // -- placement map --
    let placement = config.placement.unwrap_or_else(|| {
        IptoPlacementMap::new(
            PlacementEpoch(1),
            vec![IptoPlacementSlot::new(IptoInstanceId::from("ipto-default"), 1).unwrap()],
            vec![],
        )
        .unwrap()
    });

    // -- mapping --
    let mapping = IptoMapping::new("v1")
        .map_field("vannak:dataIndividualId", "vannak:dataIndividualId")
        .map_field("vannak:processInstanceId", "vannak:processInstanceId")
        .map_field("vannak:activityId", "vannak:activityId");

    // -- outbox --
    let (outbox_path, outbox_segment_id, outbox_node_id) = {
        let path = config.outbox_path.clone().unwrap_or_else(|| {
            let dir = std::env::temp_dir().join("vannak");
            let _ = std::fs::create_dir_all(&dir);
            dir.join("metadata-outbox.seg")
                .to_string_lossy()
                .to_string()
        });
        let seg_id = config
            .outbox_segment_id
            .clone()
            .unwrap_or_else(|| SegmentId::from("default-outbox"));
        let node_id = config
            .outbox_node_id
            .clone()
            .unwrap_or_else(|| NodeId::from("vannak"));
        (path, seg_id, node_id)
    };

    // -- discover existing segments at startup --
    if let Some(parent) = std::path::Path::new(&outbox_path).parent()
        && let Ok(discovery) = vannak::discover_segments(parent)
        && !discovery.is_empty()
    {
        eprintln!(
            "vannak: discovered {} segment(s), {} invalid",
            discovery.segments.len(),
            discovery.invalid.len()
        );
    }

    // -- service --
    let mut svc = VannakService::create(
        placement,
        mapping,
        &outbox_path,
        outbox_segment_id.clone(),
        outbox_node_id.clone(),
    )
    .map_err(|e| e.to_string())?;

    // -- optional process event journal --
    if let Some(ref journal_path) = config.process_journal_path {
        let journal = vannak::ProcessEventJournal::create(
            journal_path,
            SegmentId::from("process-journal"),
            outbox_node_id.clone(),
        )
        .map_err(|e| e.to_string())?;
        svc = svc.with_process_event_journal(journal);
    }

    // -- daemon --
    let daemon_config = DaemonConfig {
        bind_address: config
            .health_bind_address
            .unwrap_or_else(|| String::from("127.0.0.1:9090")),
        read_timeout: Duration::from_secs(10),
    };
    let writer_config = BackgroundWriterConfig {
        interval: Duration::from_secs(config.writer_interval_secs),
        max_drain_attempts_per_target: 100,
        verbose: false,
    };
    let daemon = Daemon::new(svc, daemon_config.clone(), writer_config);

    // -- control state (shared between raft and daemon) --
    let control_state = Arc::new(Mutex::new(ClusterControlState::new()));

    // -- initialize control state with placement map --
    {
        let placement_map = IptoPlacementMap::new(
            PlacementEpoch(1),
            vec![IptoPlacementSlot::new(IptoInstanceId::from("ipto-default"), 1).unwrap()],
            vec![],
        )
        .map_err(|e| e.to_string())?;
        let mut state = control_state.lock().unwrap();
        state
            .apply(vannak::cluster::ClusterControlCommand::AddNode(
                outbox_node_id.clone(),
            ))
            .unwrap();
        state
            .apply(vannak::cluster::ClusterControlCommand::SetIptoPlacementMap(
                placement_map,
            ))
            .unwrap();
    }

    // -- background writer snapshot handle --
    let writer_snapshot: Arc<Mutex<Option<vannak::BackgroundWriterSnapshot>>> =
        Arc::new(Mutex::new(None));

    // -- raft node (background thread) --
    if let Some(ref raft_config) = config.raft {
        let stop = stop_token.clone();
        let host = raft_config.host.clone();
        let port = raft_config.port;
        let peer_id = raft_config.peer_id.clone();
        let data_dir = raft_config.data_dir.clone();
        let peers = raft_config.peers.clone();
        let log_host = host.clone();
        let log_peer_id = peer_id.clone();
        std::thread::spawn(move || {
            if let Err(e) = run_raft_on_thread(&host, port, &peer_id, &data_dir, &peers, stop) {
                eprintln!("vannak: raft node error: {e}");
            }
        });
        eprintln!("vannak: raft node {log_peer_id} starting on {log_host}:{port}");
    }

    // -- kafka consumer (background thread) --
    #[cfg(feature = "kafka-client")]
    if let Some(ref kafka_config) = config.kafka {
        let stop = stop_token.clone();
        spawn_kafka_consumer(kafka_config, stop);
    }
    #[cfg(not(feature = "kafka-client"))]
    if config.kafka.is_some() {
        eprintln!("vannak: --kafka flag ignored (kafka-client feature not enabled)");
    }

    // -- health server (blocks main thread until stop) --
    eprintln!("vannak: health endpoint on {}", daemon_config.bind_address);
    daemon.serve_health(writer_snapshot);

    Ok(())
}

// ---------------------------------------------------------------------------
// CLI config parsing
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct SupervisorConfig {
    raft: Option<RaftArgs>,
    kafka: Option<KafkaArgs>,
    health_bind_address: Option<String>,
    outbox_path: Option<String>,
    outbox_segment_id: Option<SegmentId>,
    outbox_node_id: Option<NodeId>,
    process_journal_path: Option<String>,
    writer_interval_secs: u64,
    placement: Option<IptoPlacementMap>,
}

#[derive(Debug, Clone)]
struct RaftArgs {
    host: String,
    port: u16,
    peer_id: String,
    data_dir: String,
    peers: Vec<String>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct KafkaArgs {
    brokers: String,
    group_id: String,
    shards: usize,
    mailbox_capacity: usize,
    topics: Vec<String>,
}

fn parse_args(args: &[String]) -> Result<SupervisorConfig, String> {
    let mut config = SupervisorConfig::default();
    let mut idx = 0usize;

    while idx < args.len() {
        match args[idx].as_str() {
            "--raft" => {
                if idx + 4 >= args.len() {
                    return Err(usage());
                }
                let host = args[idx + 1].clone();
                let port: u16 = args[idx + 2].parse().map_err(|_| "invalid raft port")?;
                let peer_id = args[idx + 3].clone();
                let data_dir = args[idx + 4].clone();
                idx += 5;
                let mut peers = Vec::new();
                while idx < args.len() && !args[idx].starts_with("--") {
                    peers.push(args[idx].clone());
                    idx += 1;
                }
                config.raft = Some(RaftArgs {
                    host,
                    port,
                    peer_id,
                    data_dir,
                    peers,
                });
            }
            "--srv" => {
                if idx + 5 >= args.len() {
                    return Err(usage());
                }
                let service = args[idx + 1].clone();
                let host = args[idx + 2].clone();
                let port: u16 = args[idx + 3].parse().map_err(|_| "invalid raft port")?;
                let peer_id = args[idx + 4].clone();
                let data_dir = args[idx + 5].clone();
                idx += 6;
                let peers = resolve_dns_srv(&service, &peer_id);
                config.raft = Some(RaftArgs {
                    host,
                    port,
                    peer_id,
                    data_dir,
                    peers,
                });
            }
            "--kafka" => {
                if idx + 4 >= args.len() {
                    return Err(usage());
                }
                let brokers = args[idx + 1].clone();
                let group_id = args[idx + 2].clone();
                let shards: usize = args[idx + 3].parse().map_err(|_| "invalid shards")?;
                let mailbox_capacity: usize = args[idx + 4]
                    .parse()
                    .map_err(|_| "invalid mailbox capacity")?;
                idx += 5;
                let mut topics = Vec::new();
                while idx < args.len() && !args[idx].starts_with("--") {
                    topics.push(args[idx].clone());
                    idx += 1;
                }
                if topics.is_empty() {
                    return Err("--kafka requires at least one topic".to_string());
                }
                config.kafka = Some(KafkaArgs {
                    brokers,
                    group_id,
                    shards,
                    mailbox_capacity,
                    topics,
                });
            }
            "--health" => {
                if idx + 1 >= args.len() {
                    return Err(usage());
                }
                config.health_bind_address = Some(args[idx + 1].clone());
                idx += 2;
            }
            "--outbox" => {
                if idx + 3 >= args.len() {
                    return Err(usage());
                }
                config.outbox_path = Some(args[idx + 1].clone());
                config.outbox_segment_id = Some(SegmentId::from(args[idx + 2].clone()));
                config.outbox_node_id = Some(NodeId::from(args[idx + 3].clone()));
                idx += 4;
            }
            "--journal" => {
                if idx + 1 >= args.len() {
                    return Err(usage());
                }
                config.process_journal_path = Some(args[idx + 1].clone());
                idx += 2;
            }
            "--writer-interval" => {
                if idx + 1 >= args.len() {
                    return Err(usage());
                }
                config.writer_interval_secs =
                    args[idx + 1].parse().map_err(|_| "invalid interval")?;
                idx += 2;
            }
            "--placement" => {
                idx += 1;
                let epoch: u64 = args.get(idx).and_then(|s| s.parse().ok()).unwrap_or(1);
                idx += 1;
                let mut slots = Vec::new();
                while idx + 1 < args.len() && !args[idx].starts_with("--") {
                    let target = IptoInstanceId::from(args[idx].clone());
                    let vnodes: u32 = args[idx + 1].parse().map_err(|_| "invalid vnodes")?;
                    slots.push(IptoPlacementSlot::new(target, vnodes).map_err(|e| e.to_string())?);
                    idx += 2;
                }
                if !slots.is_empty() {
                    config.placement = Some(
                        IptoPlacementMap::new(PlacementEpoch(epoch), slots, vec![])
                            .map_err(|e| e.to_string())?,
                    );
                }
            }
            other if other.starts_with("--") => return Err(format!("unknown option {other}")),
            _ => idx += 1,
        }
    }

    Ok(config)
}

// ---------------------------------------------------------------------------
// Raft node (spawned on background tokio thread)
// ---------------------------------------------------------------------------

fn run_raft_on_thread(
    host: &str,
    port: u16,
    peer_id: &str,
    data_dir: &str,
    peers: &[String],
    stop_token: Arc<AtomicBool>,
) -> Result<(), String> {
    let bind_addr = SocketAddr::from_str(&format!("{host}:{port}")).unwrap();
    let me = Peer::voter(peer_id.to_string(), bind_addr);

    let mut peer_addrs: HashMap<String, SocketAddr> = HashMap::new();
    let mut members = vec![me.clone()];

    for spec in peers {
        if let Some((id, addr)) = parse_peer_spec(spec)
            && !peer_addrs.contains_key(&id)
        {
            peer_addrs.entry(id.clone()).or_insert(addr);
            members.push(Peer::voter(id, addr));
        }
    }

    let config = ClusterConfiguration::stable(members);

    let state_path = format!("{data_dir}/state");
    let log_path = format!("{data_dir}/log");

    std::fs::create_dir_all(data_dir).map_err(|e| e.to_string())?;

    let log_store: Arc<dyn LogStore> = Arc::new(FileLogStore::new(log_path.into()));
    let state_store: Arc<dyn PersistentStateStore> =
        Arc::new(FilePersistentStateStore::new(state_path.into()));
    let sm: Arc<dyn StateMachine> = Arc::new(ClusterStateMachine::new());

    let raft_node = Arc::new(parking_lot::Mutex::new(RaftNode::new(
        me,
        500,
        log_store,
        state_store,
        Some(sm),
        config,
        100,
        4096,
    )));

    let client = Arc::new(RaftClient::new());
    client.set_known_peers(peer_addrs.clone());
    let runtime = Arc::new(RaftRuntime::new(raft_node.clone(), client.clone()));
    runtime.set_peers(peer_addrs.clone());
    let handler = Arc::new(RaftHandler::new(
        raft_node.clone(),
        client.clone(),
        runtime.clone(),
    ));

    let rt = Runtime::new().map_err(|e| e.to_string())?;

    let runtime_clone = runtime.clone();
    rt.spawn(async move {
        runtime_clone.run().await;
    });

    rt.block_on(async move {
        let listener = tokio::net::TcpListener::bind(bind_addr)
            .await
            .map_err(|e| e.to_string())?;

        loop {
            if stop_token.load(Ordering::Relaxed) {
                break;
            }

            let accept_result =
                tokio::time::timeout(Duration::from_millis(500), listener.accept()).await;

            match accept_result {
                Ok(Ok((mut stream, _addr))) => {
                    let h = handler.clone();
                    tokio::spawn(async move {
                        let mut buf = bytes::BytesMut::with_capacity(8192);
                        loop {
                            let envelope =
                                match graft_transport::codec::read_envelope(&mut stream, &mut buf)
                                    .await
                                {
                                    Ok(e) => e,
                                    Err(_) => return,
                                };
                            let resp_payload =
                                h.dispatch(&envelope.r#type, &envelope.payload).await;
                            let resp = graft_proto::Envelope {
                                correlation_id: envelope.correlation_id,
                                r#type: response_type_for(&envelope.r#type),
                                payload: resp_payload,
                            };
                            if graft_transport::codec::write_envelope(&mut stream, &resp)
                                .await
                                .is_err()
                            {
                                return;
                            }
                        }
                    });
                }
                Ok(Err(_)) => {}
                Err(_) => break,
            }
        }
        Ok::<_, String>(())
    })
}

fn parse_peer_spec(spec: &str) -> Option<(String, SocketAddr)> {
    let (id_part, addr_part) = spec.split_once('@')?;
    if let Ok(addr) = SocketAddr::from_str(addr_part) {
        return Some((id_part.to_string(), addr));
    }
    addr_part
        .to_socket_addrs()
        .ok()?
        .next()
        .map(|addr| (id_part.to_string(), addr))
}

fn response_type_for(request_type: &str) -> String {
    if request_type.ends_with("Request") {
        request_type.replace("Request", "Response")
    } else {
        format!("{request_type}Response")
    }
}

// ---------------------------------------------------------------------------
// Kafka consumer (spawned on Sitas + background thread)
// ---------------------------------------------------------------------------

#[cfg(feature = "kafka-client")]
fn spawn_kafka_consumer(config: &KafkaArgs, stop_token: Arc<AtomicBool>) {
    use vannak::{
        KafkaPayloadFormat, KafkaProcessConsumer, KafkaProcessConsumerConfig, SitasRuntimeConfig,
        SitasShardRuntime,
    };

    let brokers = config.brokers.clone();
    let group_id = config.group_id.clone();
    let shards = config.shards;
    let mailbox_capacity = config.mailbox_capacity;
    let topics = config.topics.clone();

    std::thread::spawn(move || {
        let runtime =
            match SitasShardRuntime::start(SitasRuntimeConfig::new(shards, mailbox_capacity)) {
                Ok(mut rt) => {
                    if let Err(e) = rt.start_mailbox_workers() {
                        eprintln!("vannak: kafka consumer: failed to start workers: {e}");
                        return;
                    }
                    rt
                }
                Err(e) => {
                    eprintln!("vannak: kafka consumer: failed to start Sitas: {e}");
                    return;
                }
            };

        let mut consumer = match KafkaProcessConsumer::start(
            KafkaProcessConsumerConfig::new(&brokers, &group_id, topics.iter().map(String::as_str))
                .with_payload_format(KafkaPayloadFormat::DurgaJson),
        ) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("vannak: kafka consumer: failed to start: {e}");
                return;
            }
        };

        eprintln!(
            "vannak: kafka consumer subscribed to {} topic(s), {} shards",
            topics.len(),
            shards,
        );

        let mut accepted = 0u64;
        while !stop_token.load(Ordering::Relaxed) {
            match consumer.poll_once(&runtime, None) {
                Ok(Some(_)) => accepted += 1,
                Ok(None) => {}
                Err(e) => eprintln!("vannak: kafka consumer: {e}"),
            }
            if accepted > 0 && accepted.is_multiple_of(1000) {
                let snapshot = consumer.snapshot();
                eprintln!(
                    "vannak: kafka accepted={accepted} pending={} paused={} polled={}",
                    snapshot.pending_records,
                    snapshot.paused_partitions.len(),
                    snapshot.total_polled,
                );
            }
        }

        let _ = runtime.stop();
        eprintln!("vannak: kafka consumer stopped after {accepted} records");
    });
}

// ---------------------------------------------------------------------------
// DNS SRV resolution
// ---------------------------------------------------------------------------

fn resolve_dns_srv(service: &str, my_peer_id: &str) -> Vec<String> {
    for attempt in 0..30 {
        if attempt > 0 {
            std::thread::sleep(Duration::from_secs(1));
        }
        let output = process::Command::new("dig")
            .args(["+short", "SRV", service])
            .output();

        if let Ok(out) = output
            && out.status.success()
        {
            let stdout = String::from_utf8_lossy(&out.stdout);
            if !stdout.trim().is_empty() {
                let mut peers = Vec::new();
                for line in stdout.lines() {
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if parts.len() >= 4 {
                        let port = parts[2];
                        let hostname = parts[3].trim_end_matches('.');
                        let peer_id = hostname.split('.').next().unwrap_or(hostname);
                        if peer_id == my_peer_id {
                            continue;
                        }
                        let addr_str = format!("{hostname}:{port}");
                        if let Ok(mut addrs) = addr_str.to_socket_addrs()
                            && let Some(addr) = addrs.next()
                        {
                            peers.push(format!("{peer_id}@{addr}"));
                        }
                    }
                }
                if !peers.is_empty() {
                    eprintln!("vannak: resolved {} peers via DNS SRV", peers.len());
                    return peers;
                }
            }
        }
    }

    eprintln!("vannak: DNS SRV resolution failed for {service}, starting without peers");
    Vec::new()
}

// ---------------------------------------------------------------------------
// Probe command
// ---------------------------------------------------------------------------

#[cfg(feature = "node")]
fn probe_node(host: &str, port: u16) {
    use graft_proto::raft::ClusterSummaryResponse;
    use prost::Message;
    use tokio::runtime::Runtime;

    let addr = format!("{host}:{port}")
        .to_socket_addrs()
        .unwrap()
        .next()
        .unwrap();
    let rt = Runtime::new().unwrap();
    let result = rt.block_on(async move {
        tokio::time::timeout(Duration::from_secs(5), async move {
            let mut stream = tokio::net::TcpStream::connect(addr)
                .await
                .map_err(|e| e.to_string())?;
            let envelope = graft_proto::Envelope {
                correlation_id: "probe-1".to_string(),
                r#type: "ClusterSummaryRequest".to_string(),
                payload: Vec::new(),
            };
            graft_transport::codec::write_envelope(&mut stream, &envelope)
                .await
                .map_err(|e| e.to_string())?;
            let mut buf = bytes::BytesMut::with_capacity(8192);
            let resp = graft_transport::codec::read_envelope(&mut stream, &mut buf)
                .await
                .map_err(|e| e.to_string())?;
            Ok::<_, String>(resp)
        })
        .await
    });

    match result {
        Ok(Ok(resp)) => match ClusterSummaryResponse::decode(resp.payload.as_slice()) {
            Ok(summary) => {
                println!(
                    "node={} leader={} health={} status={}",
                    summary.peer_id, summary.leader_id, summary.cluster_health, summary.status,
                );
            }
            Err(_) => println!("(unable to decode response)"),
        },
        _ => {
            eprintln!("probe: connection to {addr} timed out or failed");
            process::exit(1);
        }
    };
}

// ---------------------------------------------------------------------------
// Usage
// ---------------------------------------------------------------------------

fn usage() -> String {
    r#"usage: vannak [probe <host> <port>] [options]

Commands:
  probe <host> <port>         Probe a running Vannak node for cluster summary

Options:
  --raft <host> <port> <peer-id> <data-dir> [peers...]
      Start Raft control-plane node
  --srv <service> <host> <port> <peer-id> <data-dir>
      Discover Raft peers via DNS SRV
  --kafka <brokers> <group-id> <shards> <mailbox-capacity> <topic> [<topic>...]
      Start Kafka process-event consumer (requires kafka-client feature)
  --health <bind-addr>
      Health/query HTTP server bind address (default: 127.0.0.1:9090)
  --outbox <segment-path> <segment-id> <node-id>
      Metadata outbox segment path and identity
  --journal <path>
      Process event journal segment path
  --writer-interval <secs>
      Background writer drain interval in seconds (default: 5)
  --placement <epoch> <target> <vnodes> [<target> <vnodes> ...]
      Ipto placement map (default: single ipto-default with 1 vnode)
"#
    .to_string()
}
