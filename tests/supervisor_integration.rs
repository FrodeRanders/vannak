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

//! Opt-in end-to-end check of the actual supervisor entry point.
//! Requires Ipto PostgreSQL and Kafka; set VANNAK_SUPERVISOR_INTEGRATION=1.

use rdkafka::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use serde_json::{Value, json};
use std::fs::File;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

struct Supervisor {
    child: Option<Child>,
    directory: PathBuf,
    http: SocketAddr,
    raft: SocketAddr,
    topic: String,
    brokers: String,
}

impl Supervisor {
    fn start(&mut self) {
        let log = File::create(self.directory.join("supervisor.log")).unwrap();
        self.child = Some(
            Command::new(env!("CARGO_BIN_EXE_vannak"))
                .args([
                    "--raft",
                    "127.0.0.1",
                    &self.raft.port().to_string(),
                    "node-a",
                    self.directory.join("raft").to_str().unwrap(),
                    "--health",
                    &self.http.to_string(),
                    "--outbox",
                    self.directory.join("outbox.seg").to_str().unwrap(),
                    "outbox",
                    "node-a",
                    "--writer-interval",
                    "1",
                    "--with-ipto",
                    "--require-ipto",
                    "--kafka",
                    &self.brokers,
                    &self.topic,
                    "2",
                    "16",
                    &self.topic,
                ])
                .stdout(Stdio::from(log.try_clone().unwrap()))
                .stderr(Stdio::from(log))
                .spawn()
                .unwrap(),
        );
        self.wait_for(|this| this.request("GET", "/health", None));
    }

    fn request(&self, method: &str, path: &str, body: Option<&Value>) -> Option<Value> {
        let mut stream = TcpStream::connect_timeout(&self.http, Duration::from_millis(200)).ok()?;
        stream.set_read_timeout(Some(Duration::from_secs(2))).ok()?;
        let body = body.map_or(String::new(), Value::to_string);
        write!(
            stream,
            "{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
        .ok()?;
        stream.shutdown(std::net::Shutdown::Write).ok()?;
        let mut response = String::new();
        stream.read_to_string(&mut response).ok()?;
        if !response.starts_with("HTTP/1.1 200 ") {
            return None;
        }
        serde_json::from_str(response.split_once("\r\n\r\n")?.1).ok()
    }

    fn wait_for(&mut self, mut probe: impl FnMut(&Self) -> Option<Value>) -> Value {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(value) = probe(self) {
                return value;
            }
            if let Some(status) = self.child.as_mut().unwrap().try_wait().unwrap() {
                panic!(
                    "supervisor exited {status}: {}",
                    std::fs::read_to_string(self.directory.join("supervisor.log")).unwrap()
                );
            }
            assert!(
                Instant::now() < deadline,
                "supervisor timed out: health={:?}: {}",
                self.request("GET", "/health", None),
                std::fs::read_to_string(self.directory.join("supervisor.log")).unwrap()
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    fn stop(&mut self) {
        let mut child = self.child.take().unwrap();
        #[cfg(unix)]
        assert!(
            Command::new("kill")
                .args(["-TERM", &child.id().to_string()])
                .status()
                .unwrap()
                .success()
        );
        #[cfg(not(unix))]
        child.kill().unwrap();
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                #[cfg(unix)]
                assert!(status.success(), "shutdown failed: {status}");
                break;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("supervisor did not stop");
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

impl Drop for Supervisor {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn unused_address() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

#[test]
fn supervisor_journals_http_and_kafka_delivers_metadata_and_recovers() {
    if std::env::var("VANNAK_SUPERVISOR_INTEGRATION").as_deref() != Ok("1") {
        eprintln!("SKIP: VANNAK_SUPERVISOR_INTEGRATION not set");
        return;
    }
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let directory =
        std::env::temp_dir().join(format!("vannak-supervisor-{}-{suffix}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    let mut supervisor = Supervisor {
        child: None,
        directory,
        http: unused_address(),
        raft: unused_address(),
        topic: vannak::durga_process_events_topic(&format!("supervisor-{suffix}")),
        brokers: std::env::var("VANNAK_KAFKA_BROKERS").unwrap_or_else(|_| "localhost:19092".into()),
    };
    supervisor.start();
    let event = json!({"event_id": format!("http-{suffix}"), "process_instance_id":"http-instance", "pipeline_id":"pipe",
        "kind":"ProcessStarted", "timestamp":"2026-10-31T10:00:00Z"});
    assert_eq!(
        supervisor.request("POST", "/ingest", Some(&event)).unwrap()["status"],
        "accepted"
    );
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", &supervisor.brokers)
        .set("message.timeout.ms", "10000")
        .create()
        .unwrap();
    let payload =
        json!({"processInstanceId":"kafka-instance", "processId":"pipe", "status":"STARTED",
        "eventType":"ACTIVITY_ENTERED", "activityId":"extract", "timestamp":"2026-10-31T10:00:01Z"})
        .to_string();
    producer
        .send(
            BaseRecord::to(&supervisor.topic)
                .key("kafka-instance")
                .payload(&payload),
        )
        .unwrap();
    producer.flush(Duration::from_secs(10)).unwrap();
    let query = json!({"type":"ProcessInstance", "process_instance_id":"kafka-instance"});
    supervisor.wait_for(|this| {
        this.request("POST", "/query", Some(&query))
            .filter(|result| {
                result["instance"]
                    .as_array()
                    .is_some_and(|items| items.len() == 1)
            })
    });
    let metadata = json!({"metadata_event_id":format!("m-{suffix}"), "data_individual_id":format!("data-{suffix}"),
        "tenant_id":"t", "environment_id":"e", "pipeline_id":"pipe", "process_instance_id":"http-instance",
        "timestamp":"2026-10-31T10:00:02Z", "operation":"Received", "passive_metadata":{"source":"durable"}});
    assert_eq!(
        supervisor
            .request("POST", "/ingest/metadata", Some(&metadata))
            .unwrap()["status"],
        "accepted"
    );
    let health = supervisor.wait_for(|this| {
        this.request("GET", "/health", None)
            .filter(|result| result["outbox"]["acknowledged"] == 1)
    });
    assert_eq!(health["hot_index"]["event_count"], 2);
    assert_eq!(health["process_event_journal"]["record_count"], 2);
    supervisor.stop();
    supervisor.start();
    let restored = supervisor.request("POST", "/query", Some(&query)).unwrap();
    assert_eq!(restored["instance"].as_array().unwrap().len(), 1);
    let query =
        json!({"type":"DataIndividualMetadata", "data_individual_id":format!("data-{suffix}")});
    assert_eq!(
        supervisor.request("POST", "/query", Some(&query)).unwrap()["events"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    supervisor.wait_for(|this| {
        this.request("GET", "/health", None)
            .filter(|result| result["outbox"]["acknowledged"] == 1)
    });
    supervisor.stop();
    std::fs::remove_dir_all(&supervisor.directory).unwrap();
}
