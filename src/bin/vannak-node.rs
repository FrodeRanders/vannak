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

//! Vannak Raft node binary.
//!
//! Starts a Raft node backed by `ClusterStateMachine`. Peers are discovered
//! via DNS SRV records or an explicit peer list.
//!
//! Usage:
//!   vannak-node serve <host> <port> <peer-id> <data-dir> <peers...>
//!   vannak-node serve --srv _raft._tcp.vannak.local <host> <port> <peer-id> <data-dir>

use std::collections::HashMap;
use std::net::{SocketAddr, ToSocketAddrs};
use std::process;
use std::str::FromStr;
use std::sync::Arc;

use graft_core::membership::ClusterConfiguration;
use graft_core::raft_node::{LogStore, PersistentStateStore, RaftNode};
use graft_core::state_machine::StateMachine;
use graft_core::types::Peer;
use graft_proto::raft::ClusterSummaryResponse;
use graft_runtime::handlers::RaftHandler;
use graft_runtime::runtime::RaftRuntime;
use graft_storage::log_store::FileLogStore;
use graft_storage::state_store::FilePersistentStateStore;
use graft_transport::client::RaftClient;
use prost::Message;
use tokio::runtime::Runtime;

use vannak::raft_sm::ClusterStateMachine;

fn resolve_dns_srv(service: &str, my_peer_id: &str) -> Vec<String> {
    for attempt in 0..30 {
        if attempt > 0 {
            std::thread::sleep(std::time::Duration::from_secs(1));
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
                    eprintln!("Resolved {} peers via DNS SRV", peers.len());
                    return peers;
                }
            }
        }
    }

    eprintln!("Warning: DNS SRV resolution failed for {service}, starting without peers");
    Vec::new()
}

fn parse_peer_spec(spec: &str) -> Option<(String, SocketAddr)> {
    let (id_part, addr_part) = spec.split_once('@')?;
    let addr = SocketAddr::from_str(addr_part).ok()?;
    Some((id_part.to_string(), addr))
}

fn listen_addr(host: &str, port: u16) -> SocketAddr {
    SocketAddr::from_str(&format!("{}:{}", host, port)).unwrap()
}

fn run_server(
    host: &str,
    port: u16,
    peer_id: &str,
    data_dir: &str,
    peers: &[String],
) -> Result<(), String> {
    let bind_addr = listen_addr(host, port);

    // Resolve our own address for the Raft peer identity (can't use 0.0.0.0).
    let peer_addr = format!("{peer_id}.vannak.local:{port}")
        .to_socket_addrs()
        .ok()
        .and_then(|mut a| a.next())
        .unwrap_or(bind_addr);

    let me = Peer::voter(peer_id.to_string(), peer_addr);

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

    let state_path = format!("{}/state", data_dir);
    let log_path = format!("{}/log", data_dir);

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

        eprintln!("Vannak node {peer_id} listening on {bind_addr}");

        loop {
            let (mut stream, _addr) = listener.accept().await.map_err(|e| e.to_string())?;
            let h = handler.clone();
            tokio::spawn(async move {
                let mut buf = bytes::BytesMut::with_capacity(8192);
                loop {
                    let envelope =
                        match graft_transport::codec::read_envelope(&mut stream, &mut buf).await {
                            Ok(e) => e,
                            Err(_) => return,
                        };
                    let resp_payload = h.dispatch(&envelope.r#type, &envelope.payload).await;
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
    })
}

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter("graft=trace")
        .with_target(false)
        .try_init()
        .ok();

    let args: Vec<String> = std::env::args().collect();

    if args.len() >= 2 && args[1] == "probe" {
        if args.len() < 4 {
            eprintln!("Usage: vannak-node probe <host> <port>");
            process::exit(1);
        }
        let host = &args[2];
        let port: u16 = args[3].parse().unwrap_or(10081);
        probe_node(host, port);
        return;
    }

    let (host, port, peer_id, data_dir, peers) = if args.len() >= 2 && args[1] == "--srv" {
        if args.len() < 7 {
            eprintln!("Usage: vannak-node --srv <service> <host> <port> <peer-id> <data-dir>");
            process::exit(1);
        }
        let service = &args[2];
        let host = &args[3];
        let port: u16 = args[4].parse().unwrap_or(10081);
        let peer_id = &args[5];
        let data_dir = &args[6];
        let peers = resolve_dns_srv(service, peer_id);
        if peers.is_empty() {
            eprintln!("Warning: no peers resolved from DNS SRV {service}, starting as single node");
        }
        (host.clone(), port, peer_id.clone(), data_dir.clone(), peers)
    } else if args.len() >= 6 {
        let host = &args[1];
        let port: u16 = args[2].parse().unwrap_or(10081);
        let peer_id = &args[3];
        let data_dir = &args[4];
        let peers: Vec<String> = args[5..].to_vec();
        (host.clone(), port, peer_id.clone(), data_dir.clone(), peers)
    } else {
        eprintln!(
            "Usage: vannak-node [--srv <service>] <host> <port> <peer-id> <data-dir> [peers...]"
        );
        process::exit(1);
    };

    if let Err(e) = run_server(&host, port, &peer_id, &data_dir, &peers) {
        eprintln!("Fatal: {e}");
        process::exit(1);
    }
}

fn probe_node(host: &str, port: u16) {
    let addr = format!("{host}:{port}")
        .to_socket_addrs()
        .unwrap()
        .next()
        .unwrap();
    let rt = Runtime::new().unwrap();
    let result = rt.block_on(async move {
        tokio::time::timeout(std::time::Duration::from_secs(5), async move {
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

fn response_type_for(request_type: &str) -> String {
    if request_type.ends_with("Request") {
        request_type.replace("Request", "Response")
    } else {
        format!("{}Response", request_type)
    }
}
