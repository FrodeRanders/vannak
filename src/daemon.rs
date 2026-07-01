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

//! Daemon runtime: segment discovery, background writer scheduling, and HTTP
//! health endpoint.
//!
//! Enabled with the `daemon` feature flag. Uses only `std::net` for the HTTP
//! server and `std::thread` for the background writer loop. The dependency-free
//! core modules remain usable without this.

use crate::cluster::{ClusterControlState, NodeId};
use crate::ipto::{IptoWriter, MetadataOutboxDrainSummary};
use crate::service::{VannakService, VannakServiceError, VannakServiceSnapshot};
use crate::storage::{SegmentError, SegmentManifest};
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Segment discovery
// ---------------------------------------------------------------------------

const SEGMENT_EXTENSION: &str = "seg";

/// Discovers Vannak segment files in a directory.
///
/// Scans the directory for files ending with `.seg`, validates the Vannak
/// segment magic header, and returns manifests for valid segments. Files
/// that fail magic validation are returned separately as `InvalidSegment` so
/// operators can investigate them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SegmentDiscoveryResult {
    pub segments: Vec<SegmentManifest>,
    pub invalid: Vec<PathBuf>,
    pub io_errors: Vec<(PathBuf, String)>,
}

impl SegmentDiscoveryResult {
    pub fn is_empty(&self) -> bool {
        self.segments.is_empty() && self.invalid.is_empty() && self.io_errors.is_empty()
    }
}

/// Scans `dir` for `.seg` files, validates their magic header, and returns
/// manifests for each valid segment.
pub fn discover_segments(dir: impl AsRef<Path>) -> Result<SegmentDiscoveryResult, std::io::Error> {
    let dir = dir.as_ref();
    let mut result = SegmentDiscoveryResult::default();

    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(result),
        Err(error) => return Err(error),
    };

    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(_) => continue,
        };

        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if path.extension().is_none_or(|ext| ext != SEGMENT_EXTENSION) {
            continue;
        }

        match validate_and_manifest_segment(&path) {
            Ok(manifest) => result.segments.push(manifest),
            Err(SegmentError::InvalidMagic) => result.invalid.push(path),
            Err(SegmentError::Io(error)) => result.io_errors.push((path, error.to_string())),
            Err(_) => result.invalid.push(path),
        }
    }

    result
        .segments
        .sort_by(|left, right| left.path.cmp(&right.path));
    result.invalid.sort();

    Ok(result)
}

fn validate_and_manifest_segment(path: &Path) -> Result<SegmentManifest, SegmentError> {
    let mut inner = SegmentScanner::open(path)?;
    let mut record_count: u64 = 0;
    let mut byte_len: u64 = SEGMENT_MAGIC_LEN;
    let mut checksum = 0u64;

    while let Some(record) = inner.read_next()? {
        record_count += 1;
        byte_len = inner.current_offset();
        checksum = combine_checksum(checksum, record.checksum);
    }

    let file_len = fs::metadata(path)?.len();
    if file_len != byte_len {
        return Err(SegmentError::TrailingBytes {
            offset: byte_len,
            byte_len: file_len,
        });
    }

    Ok(SegmentManifest {
        segment_id: crate::storage::SegmentId::from(
            path.file_stem()
                .unwrap_or_default()
                .to_string_lossy()
                .into_owned(),
        ),
        node_id: NodeId::from("discovery"),
        path: path.to_path_buf(),
        record_count,
        byte_len,
        checksum,
    })
}

const SEGMENT_MAGIC_LEN: u64 = 8;

fn combine_checksum(current: u64, next: u64) -> u64 {
    current.rotate_left(7) ^ next
}

/// A recoverable segment scanner used only for discovery.
struct SegmentScanner {
    reader: std::io::BufReader<fs::File>,
    offset: u64,
}

impl SegmentScanner {
    fn open(path: &Path) -> Result<Self, SegmentError> {
        use std::io::Read;
        let file = fs::File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut magic = [0u8; 8];
        reader.read_exact(&mut magic)?;
        if &magic != b"VANNAK01" {
            return Err(SegmentError::InvalidMagic);
        }
        Ok(Self {
            reader,
            offset: SEGMENT_MAGIC_LEN,
        })
    }

    fn read_next(&mut self) -> Result<Option<SegmentRecord>, SegmentError> {
        use std::io::Read;
        let mut len_buf = [0u8; 4];
        let mut read = 0usize;
        while read < len_buf.len() {
            match self.reader.read(&mut len_buf[read..]) {
                Ok(0) if read == 0 => return Ok(None),
                Ok(0) => {
                    return Err(SegmentError::TrailingBytes {
                        offset: self.offset,
                        byte_len: self.offset + read as u64,
                    });
                }
                Ok(n) => read += n,
                Err(error) => return Err(error.into()),
            }
        }

        let mut checksum_buf = [0u8; 8];
        self.reader.read_exact(&mut checksum_buf)?;

        let len = u32::from_le_bytes(len_buf) as usize;
        let expected_checksum = u64::from_le_bytes(checksum_buf);
        let mut payload = vec![0u8; len];
        self.reader.read_exact(&mut payload)?;

        let offset = self.offset;
        self.offset += 4 + 8 + len as u64;

        let actual = checksum_value(&payload);
        if actual != expected_checksum {
            return Err(SegmentError::ChecksumMismatch {
                offset,
                expected: expected_checksum,
                actual,
            });
        }

        Ok(Some(SegmentRecord {
            offset: crate::storage::RecordOffset(offset),
            checksum: actual,
            payload,
        }))
    }

    fn current_offset(&self) -> u64 {
        self.offset
    }
}

fn checksum_value(payload: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in payload {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

#[derive(Debug)]
#[allow(dead_code)]
struct SegmentRecord {
    offset: crate::storage::RecordOffset,
    checksum: u64,
    payload: Vec<u8>,
}

// ---------------------------------------------------------------------------
// Background writer
// ---------------------------------------------------------------------------

/// Configuration for the background metadata-outbox writer loop.
#[derive(Debug, Clone)]
pub struct BackgroundWriterConfig {
    /// Interval between drain attempts.
    pub interval: Duration,
    /// Maximum delivery attempts per drain cycle per target.
    pub max_drain_attempts_per_target: usize,
    /// If true, the writer loop logs drain summaries.
    pub verbose: bool,
}

impl Default for BackgroundWriterConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(5),
            max_drain_attempts_per_target: 100,
            verbose: false,
        }
    }
}

/// Owned snapshot of the background writer state.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BackgroundWriterSnapshot {
    /// Total completed drain cycles.
    pub cycles: u64,
    /// Total acknowledged deliveries.
    pub total_acknowledged: usize,
    /// Total failed deliveries.
    pub total_failed: usize,
    pub current_cycle: Option<u64>,
    pub last_summary: Option<Vec<(crate::ipto::IptoInstanceId, MetadataOutboxDrainSummary)>>,
}

/// Runs background writer with a real `IptoWriter`, draining entries to the
/// configured Ipto backend.
///
/// The service and writer are held behind `Arc<Mutex<...>>` for concurrent
/// access from the health endpoint.
pub fn run_background_writer(
    service: Arc<Mutex<VannakService>>,
    control_state: Arc<Mutex<ClusterControlState>>,
    node_id: NodeId,
    writer: Arc<Mutex<dyn IptoWriter + Send>>,
    config: BackgroundWriterConfig,
    stop_token: Arc<AtomicBool>,
) -> BackgroundWriterSnapshot {
    let mut snapshot = BackgroundWriterSnapshot::default();

    while !stop_token.load(Ordering::Relaxed) {
        snapshot.cycles += 1;

        let targets: Vec<_> = {
            let control = control_state.lock().unwrap();
            control
                .placement_map()
                .map(|map| map.instances())
                .unwrap_or_default()
        };

        let mut cycle_summaries = Vec::new();
        for target in &targets {
            let mut svc = service.lock().unwrap();
            let control = control_state.lock().unwrap();
            let mut w = writer.lock().unwrap();

            match svc.drain_metadata_for_target_if_lease_held(
                &control,
                &node_id,
                target,
                &mut *w,
                config.max_drain_attempts_per_target,
            ) {
                Ok(summary) if !summary.is_empty() => {
                    snapshot.total_acknowledged += summary.acknowledged;
                    snapshot.total_failed += summary.failed;
                    cycle_summaries.push((target.clone(), summary));
                }
                Ok(_) => {}
                Err(VannakServiceError::WriterLeaseNotHeld { .. }) => {}
                Err(_) => {}
            }
        }

        if !cycle_summaries.is_empty() {
            snapshot.last_summary = Some(cycle_summaries);
        }
        std::thread::sleep(config.interval);
    }

    snapshot.current_cycle = Some(snapshot.cycles);
    snapshot
}

// ---------------------------------------------------------------------------
// Health HTTP endpoint
// ---------------------------------------------------------------------------

/// Configuration for the daemon health HTTP server.
#[derive(Debug, Clone)]
pub struct DaemonConfig {
    /// TCP address to bind (e.g. "127.0.0.1:9090").
    pub bind_address: String,
    /// Read timeout for client requests.
    pub read_timeout: Duration,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            bind_address: String::from("127.0.0.1:9090"),
            read_timeout: Duration::from_secs(10),
        }
    }
}

/// Owned daemon snapshot exposed over the health endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonSnapshot {
    pub service: VannakServiceSnapshot,
    pub background_writer: Option<BackgroundWriterSnapshot>,
    pub segment_discovery: Option<Vec<SegmentDiscoveryResult>>,
    pub uptime_secs: u64,
}

/// Starts a blocking HTTP health server on the configured address.
///
/// Serves a single `/health` endpoint returning JSON. The server blocks the
/// calling thread until `stop_token` becomes `true`, then shuts down
/// gracefully.
///
/// # Panics
///
/// Panics if the listener cannot bind.
pub fn start_health_server(
    service: Arc<Mutex<VannakService>>,
    background_writer_snapshot: Arc<Mutex<Option<BackgroundWriterSnapshot>>>,
    config: DaemonConfig,
    start_time: Instant,
    stop_token: Arc<AtomicBool>,
) {
    let listener =
        TcpListener::bind(&config.bind_address).expect("daemon: failed to bind health listener");
    listener
        .set_nonblocking(true)
        .expect("daemon: failed to set listener nonblocking");

    let poll_interval = Duration::from_millis(100);

    while !stop_token.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((stream, _addr)) => {
                let svc = service.clone();
                let bw = background_writer_snapshot.clone();
                let uptime = start_time;
                std::thread::spawn(move || {
                    handle_health_request(stream, &svc, &bw, uptime);
                });
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(poll_interval);
            }
            Err(_) => {
                std::thread::sleep(poll_interval);
            }
        }
    }
}

fn handle_health_request(
    mut stream: TcpStream,
    service: &Arc<Mutex<VannakService>>,
    background_writer: &Arc<Mutex<Option<BackgroundWriterSnapshot>>>,
    start_time: Instant,
) {
    let mut reader = BufReader::new(stream.try_clone().unwrap_or_else(|_| unreachable!()));

    let mut request_line = String::new();
    if reader.read_line(&mut request_line).is_err() {
        return;
    }

    // Read headers to find Content-Length for POST bodies
    let mut content_length: Option<usize> = None;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).is_err() {
            break;
        }
        if line == "\r\n" || line.is_empty() {
            break;
        }
        if let Some(rest) = line.strip_prefix("Content-Length:") {
            content_length = rest.trim().parse().ok();
        }
    }

    let _ = stream.set_nonblocking(false);

    if request_line.starts_with("POST /ingest") {
        let mut body = String::new();
        if let Some(len) = content_length {
            let mut buf = vec![0u8; len];
            use std::io::Read;
            if reader.read_exact(&mut buf).is_ok() {
                body = String::from_utf8_lossy(&buf).to_string();
            }
        }

        let (status, result_json) = handle_ingest_request(service, &body);
        let response = format!(
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{result_json}",
            result_json.len()
        );
        let _ = stream.write_all(response.as_bytes());
        let _ = stream.flush();
        return;
    }

    if request_line.starts_with("POST /query") {
        let mut body = String::new();
        if let Some(len) = content_length {
            let mut buf = vec![0u8; len];
            use std::io::Read;
            if reader.read_exact(&mut buf).is_ok() {
                body = String::from_utf8_lossy(&buf).to_string();
            }
        }

        let (status, result_json) = handle_query_request(service, &body);
        let response = format!(
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{result_json}",
            result_json.len()
        );
        let _ = stream.write_all(response.as_bytes());
        let _ = stream.flush();
        return;
    }

    let status = if request_line.starts_with("GET /") {
        "200 OK"
    } else {
        "404 Not Found"
    };

    let body = if status == "200 OK" {
        build_health_json(service, background_writer, start_time)
    } else {
        r#"{"error":"not found"}"#.to_string()
    };

    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );

    let _ = stream.write_all(response.as_bytes());
    let _ = stream.flush();
}

fn handle_ingest_request(
    service: &Arc<Mutex<VannakService>>,
    body: &str,
) -> (&'static str, String) {
    let mut svc = match service.lock() {
        Ok(svc) => svc,
        Err(_) => {
            return (
                "503 Service Unavailable",
                r#"{"error":"service unavailable"}"#.to_string(),
            );
        }
    };

    let Some(event) = parse_ingest_event_json(body) else {
        return (
            "400 Bad Request",
            r#"{"error":"invalid event JSON"}"#.to_string(),
        );
    };

    match svc.ingest_process_event(event) {
        Ok(crate::index::IngestOutcome::Accepted) => {
            ("200 OK", r#"{"status":"accepted"}"#.to_string())
        }
        Ok(crate::index::IngestOutcome::Duplicate) => {
            ("200 OK", r#"{"status":"duplicate"}"#.to_string())
        }
        Err(e) => ("400 Bad Request", format!(r#"{{"error":"{}"}}"#, e)),
    }
}

/// Parses a minimal JSON process event into a `PipelineEvent`.
///
/// Required fields: `process_instance_id`, `pipeline_id`, `kind`.
/// Optional: `event_id` (auto-generated), `timestamp` (defaults to now-ish),
/// `tenant_id`, `environment_id`, `activity_id`.
///
/// `kind` accepts Durga-like names: `ProcessStarted`, `ProcessCompleted`,
/// `ProcessFailed`, `ActivityEntered`, `ActivityCompleted`, etc.
fn parse_ingest_event_json(json: &str) -> Option<crate::ingest::PipelineEvent> {
    let pi_id = extract_json_string_field(json, "process_instance_id")?;
    let pipeline_id = extract_json_string_field(json, "pipeline_id")?;
    let kind_str = extract_json_string_field(json, "kind")?;

    let kind = match kind_str.as_str() {
        "ProcessStarted" => crate::process::EventKind::ProcessStarted,
        "ProcessCompleted" => crate::process::EventKind::ProcessCompleted,
        "ProcessFailed" => crate::process::EventKind::ProcessFailed,
        "ActivityEntered" => crate::process::EventKind::ActivityEntered,
        "ActivityCompleted" => crate::process::EventKind::ActivityCompleted,
        "ActivityCancelled" => crate::process::EventKind::ActivityCancelled,
        "ActivityEscalated" => crate::process::EventKind::ActivityEscalated,
        "GatewayTaken" => crate::process::EventKind::GatewayTaken,
        _ => crate::process::EventKind::ActivityEntered,
    };

    let event_id = extract_json_string_field(json, "event_id")
        .map(crate::ingest::EventId::from)
        .unwrap_or_else(|| {
            use std::sync::atomic::AtomicU64;
            static COUNTER: AtomicU64 = AtomicU64::new(1);
            crate::ingest::EventId::from(format!(
                "ingest-{}",
                COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            ))
        });

    let timestamp = extract_json_string_field(json, "timestamp")
        .map(crate::ingest::EventTimestamp::from)
        .unwrap_or_else(|| crate::ingest::EventTimestamp::from("1970-01-01T00:00:00Z"));

    let tenant_id = extract_json_string_field(json, "tenant_id")
        .map(crate::process::TenantId::from)
        .unwrap_or_else(|| crate::process::TenantId::from("demo"));

    let environment_id = extract_json_string_field(json, "environment_id")
        .map(crate::process::EnvironmentId::from)
        .unwrap_or_else(|| crate::process::EnvironmentId::from("demo"));

    let activity_id =
        extract_json_string_field(json, "activity_id").map(crate::process::ActivityId::from);

    let mut event = crate::ingest::PipelineEvent::new(
        event_id,
        crate::ingest::SourceId::from("daemon-http"),
        crate::ingest::SourceSequence(0),
        tenant_id,
        environment_id,
        crate::process::PipelineId::from(pipeline_id),
        crate::process::ProcessDefinitionId::from("demo-definition"),
        crate::process::ProcessInstanceId::from(pi_id),
        timestamp,
        kind,
    );

    if let Some(activity_id) = activity_id {
        event = event.with_activity_id(activity_id);
    }

    Some(event)
}

fn handle_query_request(service: &Arc<Mutex<VannakService>>, body: &str) -> (&'static str, String) {
    let svc = match service.lock() {
        Ok(svc) => svc,
        Err(_) => {
            return (
                "503 Service Unavailable",
                r#"{"error":"service unavailable"}"#.to_string(),
            );
        }
    };

    if body.contains("\"type\":\"ProcessInstance\"")
        && let Some(id) = extract_json_string_field(body, "process_instance_id")
    {
        let result = svc.process_instance(&crate::query::ProcessInstanceQuery {
            process_instance_id: crate::process::ProcessInstanceId::from(id),
        });
        match result {
            Some(snap) => {
                let json = format!(
                    r#"{{"type":"ProcessInstances","instance":[{{"process_instance_id":"{}","pipeline_id":"{}","status":"{:?}"}}]}}"#,
                    snap.process_instance_id.as_str(),
                    snap.pipeline_id.as_str(),
                    snap.status,
                );
                return ("200 OK", json);
            }
            None => {
                return (
                    "200 OK",
                    r#"{"type":"ProcessInstances","instance":[]}"#.to_string(),
                );
            }
        }
    }

    if body.contains("\"type\":\"Pipeline\"")
        && let Some(id) = extract_json_string_field(body, "pipeline_id")
    {
        let limit_val = extract_json_u64_field(body, "limit").unwrap_or(100);
        let result = svc.pipeline_instances(&crate::query::PipelineQuery {
            pipeline_id: crate::process::PipelineId::from(id),
            limit: crate::query::QueryLimit::new(limit_val as usize),
        });
        return ("200 OK", serialize_query_result(&result));
    }

    if body.contains("\"type\":\"ProcessStatus\"")
        && let Some(status_str) = extract_json_string_field(body, "status")
    {
        let status = match status_str.as_str() {
            "Active" => crate::process::ProcessStatus::Active,
            "Completed" => crate::process::ProcessStatus::Completed,
            "Failed" => crate::process::ProcessStatus::Failed,
            "Cancelled" => crate::process::ProcessStatus::Cancelled,
            _ => crate::process::ProcessStatus::Active,
        };
        let limit_val = extract_json_u64_field(body, "limit").unwrap_or(100);
        let result = svc.process_instances_by_status(&crate::query::ProcessStatusQuery {
            status,
            limit: crate::query::QueryLimit::new(limit_val as usize),
        });
        return ("200 OK", serialize_query_result(&result));
    }

    (
        "400 Bad Request",
        r#"{"error":"invalid query"}"#.to_string(),
    )
}

fn serialize_query_result(result: &crate::query::QueryResult) -> String {
    match result {
        crate::query::QueryResult::ProcessInstances(instances) => {
            let items: Vec<String> = instances
                .iter()
                .map(|snap| {
                    format!(
                        r#"{{"process_instance_id":"{}","pipeline_id":"{}","status":"{:?}"}}"#,
                        snap.process_instance_id.as_str(),
                        snap.pipeline_id.as_str(),
                        snap.status,
                    )
                })
                .collect();
            format!(
                r#"{{"type":"ProcessInstances","instance":[{}]}}"#,
                items.join(",")
            )
        }
        crate::query::QueryResult::Events(events) => {
            let items: Vec<String> = events
                .iter()
                .map(|ev| {
                    format!(
                        r#"{{"event_id":"{}","pipeline_id":"{}","process_instance_id":"{}","kind":"{:?}"}}"#,
                        ev.event_id().as_str(),
                        ev.pipeline_id().as_str(),
                        ev.process_instance_id().as_str(),
                        ev.kind(),
                    )
                })
                .collect();
            format!(r#"{{"type":"Events","events":[{}]}}"#, items.join(","))
        }
        crate::query::QueryResult::MetadataEvents(events) => {
            let items: Vec<String> = events
                .iter()
                .map(|ev| {
                    format!(
                        r#"{{"metadata_event_id":"{}","data_individual_id":"{}","operation":"{:?}"}}"#,
                        ev.metadata_event_id().as_str(),
                        ev.data_individual_id().as_str(),
                        ev.operation(),
                    )
                })
                .collect();
            format!(
                r#"{{"type":"MetadataEvents","events":[{}]}}"#,
                items.join(",")
            )
        }
    }
}

fn extract_json_string_field(json: &str, field: &str) -> Option<String> {
    let pattern = format!("\"{field}\":\"");
    let start = json.find(&pattern)?;
    let after = &json[start + pattern.len()..];
    let end = after.find('"')?;
    Some(after[..end].to_string())
}

fn extract_json_u64_field(json: &str, field: &str) -> Option<u64> {
    let pattern = format!("\"{field}\":");
    let start = json.find(&pattern)?;
    let after = &json[start + pattern.len()..];
    let end = after
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(after.len());
    after[..end].parse().ok()
}

fn build_health_json(
    service: &Arc<Mutex<VannakService>>,
    background_writer: &Arc<Mutex<Option<BackgroundWriterSnapshot>>>,
    start_time: Instant,
) -> String {
    let svc_snapshot = service.lock().map(|svc| svc.snapshot()).ok();
    let bw_snapshot = background_writer
        .lock()
        .ok()
        .and_then(|guard| guard.clone());
    let uptime = start_time.elapsed().as_secs();

    let mut parts: Vec<String> = Vec::new();

    parts.push(format!("\"uptime_secs\":{uptime}"));

    if let Some(ref snap) = svc_snapshot {
        parts.push(format!(
            "\"hot_index\":{{\"event_count\":{},\"process_instance_count\":{},\"pipeline_count\":{},\"duplicate_events\":{},\"rejected_events\":{}}}",
            snap.hot_index.event_count,
            snap.hot_index.process_instance_count,
            snap.hot_index.pipeline_count,
            snap.hot_index.duplicate_events,
            snap.hot_index.rejected_events,
        ));
        parts.push(format!(
            "\"outbox\":{{\"total\":{},\"pending\":{},\"acknowledged\":{},\"failed\":{}}}",
            snap.metadata_outbox.outbox.total,
            snap.metadata_outbox.outbox.pending,
            snap.metadata_outbox.outbox.acknowledged,
            snap.metadata_outbox.outbox.failed,
        ));
        parts.push(format!(
            "\"provenance\":{{\"metadata_event_count\":{}}}",
            snap.provenance_index.metadata_event_count,
        ));
        parts.push(format!("\"placement_epoch\":{}", snap.placement_epoch.0));
        if let Some(ref manifest) = snap.process_event_journal {
            parts.push(format!(
                "\"process_event_journal\":{{\"record_count\":{},\"byte_len\":{}}}",
                manifest.record_count, manifest.byte_len,
            ));
        }
    }

    if let Some(ref bw) = bw_snapshot {
        let mut bw_parts = vec![
            format!("\"cycles\":{}", bw.cycles),
            format!("\"total_acknowledged\":{}", bw.total_acknowledged),
            format!("\"total_failed\":{}", bw.total_failed),
        ];
        if let Some(cycle) = bw.current_cycle {
            bw_parts.push(format!("\"current_cycle\":{cycle}"));
        }
        parts.push(format!("\"background_writer\":{{{}}}", bw_parts.join(",")));
    }

    format!("{{{}}}", parts.join(","))
}

// ---------------------------------------------------------------------------
// Cluster query fanout
// ---------------------------------------------------------------------------

/// Configuration for cross-node cluster query fanout.
#[derive(Debug, Clone)]
pub struct ClusterQueryConfig {
    /// Peer node addresses to fan out queries to (e.g. "192.168.1.2:9090").
    pub peers: Vec<String>,
    /// Connection timeout for peer requests.
    pub connect_timeout: Duration,
    /// Read timeout per peer request.
    pub read_timeout: Duration,
}

impl Default for ClusterQueryConfig {
    fn default() -> Self {
        Self {
            peers: Vec::new(),
            connect_timeout: Duration::from_secs(3),
            read_timeout: Duration::from_secs(5),
        }
    }
}

/// Result of a cluster-scattered query merged from local and peer results.
#[derive(Debug, Clone)]
pub struct ClusterQueryResult {
    /// Nodes that responded.
    pub peer_count: usize,
    /// Nodes that were unreachable or timed out.
    pub failed_peers: Vec<String>,
    /// Merged query results from all reachable nodes.
    pub merged: crate::query::QueryResult,
}

/// Fans out a process-instance query to peer nodes and merges their results
/// with the local result.
///
/// Sends a POST /query request to each configured peer node, collects their
/// `QueryResult` values, and merges them together.  Uses `std::net::TcpStream`
/// for transport — no external HTTP client dependency.
pub fn fanout_process_instance_query(
    service: &Arc<Mutex<VannakService>>,
    query: &crate::query::ProcessInstanceQuery,
    config: &ClusterQueryConfig,
) -> ClusterQueryResult {
    let local = {
        let svc = service.lock().unwrap();
        svc.process_instance(query)
    };
    let mut instances: Vec<crate::process::ProcessInstanceSnapshot> = local.into_iter().collect();
    let mut failed = Vec::new();
    let seen_ids: Vec<_> = instances
        .iter()
        .map(|i| i.process_instance_id.clone())
        .collect();

    for peer in &config.peers {
        let json = format!(
            r#"{{"type":"ProcessInstance","process_instance_id":"{}"}}"#,
            query.process_instance_id.as_str(),
        );
        match send_query_to_peer(peer, &json, config.connect_timeout, config.read_timeout) {
            Ok(Some(crate::query::QueryResult::ProcessInstances(mut more))) => {
                for inst in more.drain(..) {
                    if !seen_ids.contains(&inst.process_instance_id) {
                        instances.push(inst);
                    }
                }
            }
            Ok(_) => {}
            Err(_) => {
                failed.push(peer.clone());
            }
        }
    }

    ClusterQueryResult {
        peer_count: config.peers.len(),
        failed_peers: failed,
        merged: crate::query::QueryResult::ProcessInstances(instances),
    }
}

/// Fans out a pipeline-instances query to peer nodes.
pub fn fanout_pipeline_query(
    service: &Arc<Mutex<VannakService>>,
    query: &crate::query::PipelineQuery,
    config: &ClusterQueryConfig,
) -> ClusterQueryResult {
    let local = service.lock().unwrap().pipeline_instances(query);
    let mut all_instances = match local {
        crate::query::QueryResult::ProcessInstances(instances) => instances,
        _ => Vec::new(),
    };
    let mut failed = Vec::new();

    for peer in &config.peers {
        let json = format!(
            r#"{{"type":"Pipeline","pipeline_id":"{}","limit":{}}}"#,
            query.pipeline_id.as_str(),
            query.limit.value(),
        );
        match send_query_to_peer(peer, &json, config.connect_timeout, config.read_timeout) {
            Ok(Some(crate::query::QueryResult::ProcessInstances(mut more))) => {
                let remaining = query.limit.value().saturating_sub(all_instances.len());
                all_instances.extend(more.drain(..remaining.min(more.len())));
            }
            Ok(_) => {}
            Err(_) => {
                failed.push(peer.clone());
            }
        }
    }

    all_instances.truncate(query.limit.value());

    ClusterQueryResult {
        peer_count: config.peers.len(),
        failed_peers: failed,
        merged: crate::query::QueryResult::ProcessInstances(all_instances),
    }
}

/// Fans out a status query to peer nodes.
pub fn fanout_process_status_query(
    service: &Arc<Mutex<VannakService>>,
    query: &crate::query::ProcessStatusQuery,
    config: &ClusterQueryConfig,
) -> ClusterQueryResult {
    let local = service.lock().unwrap().process_instances_by_status(query);
    let mut all_instances = match local {
        crate::query::QueryResult::ProcessInstances(instances) => instances,
        _ => Vec::new(),
    };
    let mut failed = Vec::new();

    for peer in &config.peers {
        let json = format!(
            r#"{{"type":"ProcessStatus","status":"{:?}","limit":{}}}"#,
            query.status,
            query.limit.value(),
        );
        match send_query_to_peer(peer, &json, config.connect_timeout, config.read_timeout) {
            Ok(Some(crate::query::QueryResult::ProcessInstances(mut more))) => {
                let remaining = query.limit.value().saturating_sub(all_instances.len());
                all_instances.extend(more.drain(..remaining.min(more.len())));
            }
            Ok(_) => {}
            Err(_) => {
                failed.push(peer.clone());
            }
        }
    }

    all_instances.truncate(query.limit.value());

    ClusterQueryResult {
        peer_count: config.peers.len(),
        failed_peers: failed,
        merged: crate::query::QueryResult::ProcessInstances(all_instances),
    }
}

fn send_query_to_peer(
    peer: &str,
    json_body: &str,
    connect_timeout: Duration,
    read_timeout: Duration,
) -> Result<Option<crate::query::QueryResult>, String> {
    let mut stream = TcpStream::connect_timeout(
        &peer
            .parse::<std::net::SocketAddr>()
            .map_err(|e| e.to_string())?,
        connect_timeout,
    )
    .map_err(|e| e.to_string())?;

    let request = format!(
        "POST /query HTTP/1.1\r\nHost: {peer}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{json_body}",
        json_body.len(),
    );
    stream
        .write_all(request.as_bytes())
        .map_err(|e| e.to_string())?;
    stream.flush().map_err(|e| e.to_string())?;

    stream
        .set_read_timeout(Some(read_timeout))
        .map_err(|e| e.to_string())?;

    let mut reader = BufReader::new(&stream);
    let mut response = String::new();
    let mut empty_line_count = 0u32;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).map_err(|e| e.to_string())? == 0 {
            break;
        }
        if line == "\r\n" {
            empty_line_count += 1;
            if empty_line_count >= 1 {
                break;
            }
            continue;
        }
        response.push_str(&line);
    }

    if response.is_empty() {
        return Ok(None);
    }

    let mut body = String::new();
    reader
        .read_to_string(&mut body)
        .map_err(|e| e.to_string())?;

    Ok(parse_query_result_json(&body))
}

fn parse_query_result_json(json: &str) -> Option<crate::query::QueryResult> {
    let body = json.trim();

    if body.contains("\"type\":\"ProcessInstances\"") {
        let mut instances = Vec::new();
        let mut rest = body;
        while let Some(start) = rest.find("\"process_instance_id\":\"") {
            let after_field = &rest[start + "\"process_instance_id\":\"".len()..];
            let id_end = after_field.find('"').unwrap_or(0);
            let pi_id = &after_field[..id_end];

            let mut pipeline_id = "";
            if let Some(pipe_start) = after_field.find("\"pipeline_id\":\"") {
                let after_pipe = &after_field[pipe_start + "\"pipeline_id\":\"".len()..];
                let pipe_end = after_pipe.find('"').unwrap_or(0);
                pipeline_id = &after_pipe[..pipe_end];
            }

            let mut status = crate::process::ProcessStatus::Active;
            if let Some(stat_start) = after_field.find("\"status\":\"") {
                let after_stat = &after_field[stat_start + "\"status\":\"".len()..];
                let stat_end = after_stat.find('"').unwrap_or(0);
                let stat_str = &after_stat[..stat_end];
                status = match stat_str {
                    "Active" => crate::process::ProcessStatus::Active,
                    "Completed" => crate::process::ProcessStatus::Completed,
                    "Failed" => crate::process::ProcessStatus::Failed,
                    "Cancelled" => crate::process::ProcessStatus::Cancelled,
                    _ => crate::process::ProcessStatus::Active,
                };
            }

            instances.push(crate::process::ProcessInstanceSnapshot {
                process_instance_id: crate::process::ProcessInstanceId::from(pi_id),
                pipeline_id: crate::process::PipelineId::from(pipeline_id),
                tenant_id: crate::process::TenantId::from(""),
                environment_id: crate::process::EnvironmentId::from(""),
                process_definition_id: crate::process::ProcessDefinitionId::from(""),
                process_version: None,
                current_activity_id: None,
                status,
                started_at: None,
                last_updated_at: crate::ingest::EventTimestamp::from("1970-01-01T00:00:00Z"),
                completed_at: None,
                correlation_id: None,
                business_key: None,
                token_id: None,
                activities: std::collections::BTreeMap::new(),
                activity_entered_at: std::collections::BTreeMap::new(),
                activity_durations: std::collections::BTreeMap::new(),
                metadata_refs: Vec::new(),
                retry_count: 0,
                last_error: None,
            });

            rest = &after_field[id_end + 1..];
        }
        if instances.is_empty() && body.contains("\"error\"") {
            return None;
        }
        Some(crate::query::QueryResult::ProcessInstances(instances))
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// Rebalancing orchestration
// ---------------------------------------------------------------------------

/// Detects placement map changes between consecutive epochs and returns
/// the affected shard ranges that need rebalancing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacementChange {
    /// The previous placement epoch.
    pub from_epoch: crate::cluster::PlacementEpoch,
    /// The new placement epoch.
    pub to_epoch: crate::cluster::PlacementEpoch,
    /// Shard ranges that changed owner — (shard start, shard end, old target, new target).
    pub moved_ranges: Vec<(
        crate::data::DataIndividualShardId,
        crate::data::DataIndividualShardId,
        crate::ipto::IptoInstanceId,
        crate::ipto::IptoInstanceId,
    )>,
}

impl PlacementChange {
    pub fn is_empty(&self) -> bool {
        self.moved_ranges.is_empty()
    }
}

/// Detects placement changes by comparing two consecutive placement maps.
///
/// Returns affected shard ranges that changed target instance between the
/// two epochs. If the ring-based placement changed, shards that map to a
/// different instance are grouped into contiguous ranges.
pub fn detect_placement_change(
    from_map: &crate::cluster::IptoPlacementMap,
    to_map: &crate::cluster::IptoPlacementMap,
) -> PlacementChange {
    let mut moved_ranges = Vec::new();

    // Scan through the shard space and find contiguous ranges where the
    // target instance changed. We sample at a reasonable granularity.
    const SAMPLE_STEP: u64 = 1;
    let mut range_start: Option<(
        crate::data::DataIndividualShardId,
        &crate::ipto::IptoInstanceId,
        &crate::ipto::IptoInstanceId,
    )> = None;

    // Scan from 0 upward, looking for changes.
    let mut shard = 0u64;
    while shard < u64::MAX.saturating_sub(SAMPLE_STEP) {
        let sid = crate::data::DataIndividualShardId(shard);
        let from_target = from_map.resolve(sid);
        let to_target = to_map.resolve(sid);

        match (from_target, to_target) {
            (Some(from), Some(to)) if from != to => {
                if let Some((start, _, new_t)) = &range_start {
                    if *new_t != to {
                        // Different new target — close the previous range
                        moved_ranges.push((
                            *start,
                            crate::data::DataIndividualShardId(shard.saturating_sub(1)),
                            (*from).clone(),
                            (*new_t).clone(),
                        ));
                        range_start = Some((sid, from, to));
                    }
                } else {
                    range_start = Some((sid, from, to));
                }
            }
            _ => {
                if let Some((start, old_t, new_t)) = range_start.take() {
                    moved_ranges.push((
                        start,
                        crate::data::DataIndividualShardId(shard.saturating_sub(1)),
                        old_t.clone(),
                        new_t.clone(),
                    ));
                }
            }
        }
        shard = shard.saturating_add(SAMPLE_STEP);
    }

    PlacementChange {
        from_epoch: from_map.epoch,
        to_epoch: to_map.epoch,
        moved_ranges,
    }
}

/// Result of an automated rebalancing operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RebalanceOutcome {
    pub placement_change: PlacementChange,
    pub drain_summary: crate::ipto::MetadataOutboxRebalanceSummary,
}

/// Runs automated rebalancing for a detected placement change.
///
/// For each moved shard range:
/// 1. Replays the outbox segment for the affected shard range
/// 2. Drains the extracted entries through the writer (new target)
/// 3. The idempotent writer handles duplicates if entries already exist on
///    the new target
///
/// Returns the rebalancing outcome for each moved range.
pub fn rebalance_after_placement_change(
    outbox_segment_path: impl AsRef<std::path::Path>,
    change: &PlacementChange,
    writer: &mut (impl IptoWriter + ?Sized),
    max_attempts: usize,
) -> Result<Vec<RebalanceOutcome>, crate::ipto::MetadataOutboxStorageError> {
    let mut outcomes = Vec::new();

    for &(start, end, _, _) in &change.moved_ranges {
        let summary = crate::ipto::rebalance_shard_range_to(
            outbox_segment_path.as_ref(),
            start,
            end,
            writer,
            max_attempts,
        )?;

        outcomes.push(RebalanceOutcome {
            placement_change: change.clone(),
            drain_summary: summary,
        });
    }

    Ok(outcomes)
}

/// Configuration for the automated rebalancing monitor.
#[derive(Debug, Clone)]
pub struct RebalanceMonitorConfig {
    /// Interval between placement-change checks.
    pub check_interval: Duration,
    /// Maximum delivery attempts per rebalance drain.
    pub max_drain_attempts: usize,
    /// Path to the metadata outbox segment for replay.
    pub outbox_segment_path: std::path::PathBuf,
}

impl Default for RebalanceMonitorConfig {
    fn default() -> Self {
        Self {
            check_interval: Duration::from_secs(30),
            max_drain_attempts: 100,
            outbox_segment_path: std::path::PathBuf::from("outbox.seg"),
        }
    }
}

/// Runs an automated rebalancing monitor loop.
///
/// Periodically checks the Raft-replicated control state for placement map
/// changes. When a change is detected, replays the outbox segment for
/// affected shard ranges and delivers the entries to the new Ipto target
/// through the provided writer.
pub fn run_rebalance_monitor(
    control_state: Arc<Mutex<ClusterControlState>>,
    writer: Arc<Mutex<dyn IptoWriter + Send>>,
    config: RebalanceMonitorConfig,
    stop_token: Arc<AtomicBool>,
) -> Vec<RebalanceOutcome> {
    let mut outcomes = Vec::new();
    let mut last_epoch: Option<crate::cluster::PlacementEpoch> = None;

    while !stop_token.load(Ordering::Relaxed) {
        let current_map = {
            let state = control_state.lock().unwrap();
            state.placement_map().cloned()
        };

        if let Some(ref map) = current_map {
            match last_epoch {
                Some(prev) if prev != map.epoch => {
                    let prev_map = {
                        let state = control_state.lock().unwrap();
                        state
                            .placement_map_history()
                            .find(|(epoch, _)| **epoch == prev)
                            .map(|(_, m)| m.clone())
                    };

                    if let Some(prev_map) = prev_map {
                        let change = detect_placement_change(&prev_map, map);
                        if !change.is_empty() {
                            let mut w = writer.lock().unwrap();
                            match rebalance_after_placement_change(
                                &config.outbox_segment_path,
                                &change,
                                &mut *w,
                                config.max_drain_attempts,
                            ) {
                                Ok(mut result) => outcomes.append(&mut result),
                                Err(_) => { /* retry next cycle */ }
                            }
                        }
                    }
                    last_epoch = Some(map.epoch);
                }
                None => {
                    last_epoch = Some(map.epoch);
                }
                Some(_) => { /* same epoch, no change */ }
            }
        }

        std::thread::sleep(config.check_interval);
    }

    outcomes
}

// ---------------------------------------------------------------------------
// Daemon struct
// ---------------------------------------------------------------------------

/// Orchestrates the full daemon: segment discovery, recovery, background
/// writer, and health endpoint.
///
/// Wraps a [`VannakService`] behind `Arc<Mutex<...>>` for concurrent access
/// from the health endpoint and the background writer.
pub struct Daemon {
    pub service: Arc<Mutex<VannakService>>,
    config: DaemonConfig,
    writer_config: BackgroundWriterConfig,
    start_time: Instant,
    stop_token: Arc<AtomicBool>,
}

impl Daemon {
    pub fn new(
        service: VannakService,
        config: DaemonConfig,
        writer_config: BackgroundWriterConfig,
    ) -> Self {
        Self {
            service: Arc::new(Mutex::new(service)),
            config,
            writer_config,
            start_time: Instant::now(),
            stop_token: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Returns the service for external query access.
    pub fn service(&self) -> &Arc<Mutex<VannakService>> {
        &self.service
    }

    /// Starts the background writer loop on a new thread with a real
    /// `IptoWriter` for draining entries.
    ///
    /// Returns an `Arc<Mutex<Option<BackgroundWriterSnapshot>>>` that is
    /// periodically updated by the writer thread.
    pub fn start_background_writer(
        &self,
        control_state: Arc<Mutex<ClusterControlState>>,
        node_id: NodeId,
        writer: Arc<Mutex<dyn IptoWriter + Send>>,
    ) -> Arc<Mutex<Option<BackgroundWriterSnapshot>>> {
        let bw_snapshot = Arc::new(Mutex::new(None::<BackgroundWriterSnapshot>));
        let bw_snapshot_clone = bw_snapshot.clone();
        let service = self.service.clone();
        let config = self.writer_config.clone();
        let stop_token = self.stop_token.clone();

        std::thread::spawn(move || {
            let snapshot =
                run_background_writer(service, control_state, node_id, writer, config, stop_token);
            *bw_snapshot_clone.lock().unwrap() = Some(snapshot);
        });

        bw_snapshot
    }

    /// Blocks the calling thread serving HTTP health requests.
    ///
    /// Returns when `stop()` is called from another thread.
    pub fn serve_health(
        &self,
        background_writer_snapshot: Arc<Mutex<Option<BackgroundWriterSnapshot>>>,
    ) {
        start_health_server(
            self.service.clone(),
            background_writer_snapshot,
            self.config.clone(),
            self.start_time,
            self.stop_token.clone(),
        );
    }

    /// Signals the daemon to stop all background loops.
    pub fn stop(&self) {
        self.stop_token.store(true, Ordering::Relaxed);
    }

    /// Returns `true` if stop has been requested.
    pub fn is_stopping(&self) -> bool {
        self.stop_token.load(Ordering::Relaxed)
    }

    /// Takes an owned snapshot of the full daemon state.
    pub fn snapshot(
        &self,
        background_writer: &Arc<Mutex<Option<BackgroundWriterSnapshot>>>,
    ) -> DaemonSnapshot {
        let svc = self.service.lock().unwrap();
        DaemonSnapshot {
            service: svc.snapshot(),
            background_writer: background_writer
                .lock()
                .ok()
                .and_then(|guard| guard.clone()),
            segment_discovery: None,
            uptime_secs: self.start_time.elapsed().as_secs(),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::{
        ClusterControlCommand, IptoPlacementMap, IptoPlacementSlot, LeaseEpoch, PlacementEpoch,
        WriterLease,
    };
    use crate::data::{
        DataIndividualId, DataIndividualMetadataEvent, MetadataEventId, MetadataOperation,
        MetadataValue, PassiveMetadata,
    };
    use crate::ingest::EventTimestamp;
    use crate::ipto::{IptoInstanceId, IptoMapping};
    use crate::process::{EnvironmentId, PipelineId, ProcessInstanceId, TenantId};
    use crate::storage::{SegmentId, SegmentWriter};
    use std::time::{SystemTime, UNIX_EPOCH};

    // -- segment discovery --

    #[test]
    fn discovers_valid_segments_in_directory() {
        let dir = tempdir();
        let path1 = dir.join("outbox.seg");
        let path2 = dir.join("process.seg");
        let bad_path = dir.join("corrupt.seg");
        let _other = dir.join("notes.txt");

        let mut writer1 =
            SegmentWriter::create(&path1, SegmentId::from("outbox"), NodeId::from("node-a"))
                .unwrap();
        writer1.append_record(b"first").unwrap();
        writer1.sync().unwrap();
        drop(writer1);

        let mut writer2 =
            SegmentWriter::create(&path2, SegmentId::from("process"), NodeId::from("node-a"))
                .unwrap();
        writer2.append_record(b"process-event").unwrap();
        writer2.sync().unwrap();
        drop(writer2);

        fs::write(&bad_path, b"not a vannak segment").unwrap();
        fs::write(&_other, b"not a segment file").unwrap();

        let result = discover_segments(&dir).unwrap();
        assert_eq!(result.segments.len(), 2);
        assert_eq!(result.invalid.len(), 1);
        assert!(result.io_errors.is_empty());
    }

    #[test]
    fn discovers_nothing_in_nonexistent_directory() {
        let dir = tempdir();
        let nonexistent = dir.join("nonexistent");
        let result = discover_segments(&nonexistent).unwrap();
        assert!(result.segments.is_empty());
    }

    // -- background writer snapshot --

    #[test]
    fn background_writer_snapshot_defaults_are_sensible() {
        let snap = BackgroundWriterSnapshot::default();
        assert_eq!(snap.cycles, 0);
        assert_eq!(snap.total_acknowledged, 0);
        assert_eq!(snap.total_failed, 0);
        assert!(snap.current_cycle.is_none());
        assert!(snap.last_summary.is_none());
    }

    // -- health JSON --

    #[test]
    fn health_json_contains_required_fields() {
        let dir = tempdir();
        let path = dir.join("health-test.seg");
        let svc = VannakService::create(
            IptoPlacementMap::new(
                PlacementEpoch(1),
                vec![IptoPlacementSlot::new(IptoInstanceId::from("ipto-a"), 1).unwrap()],
                vec![],
            )
            .unwrap(),
            IptoMapping::new("v1").map_field("vannak:dataIndividualId", "vannak:dataIndividualId"),
            &path,
            SegmentId::from("health-seg"),
            NodeId::from("node-a"),
        )
        .unwrap();

        let service = Arc::new(Mutex::new(svc));
        let bw: Arc<Mutex<Option<BackgroundWriterSnapshot>>> = Arc::new(Mutex::new(None));
        let start = Instant::now();

        let json = build_health_json(&service, &bw, start);
        assert!(json.contains("\"uptime_secs\""));
        assert!(json.contains("\"hot_index\""));
        assert!(json.contains("\"event_count\""));
        assert!(json.contains("\"outbox\""));
        assert!(json.contains("\"provenance\""));
        assert!(json.contains("\"placement_epoch\""));
    }

    // -- daemon lifecycle --

    #[test]
    fn daemon_starts_and_stops_cleanly() {
        let dir = tempdir();
        let path = dir.join("daemon.seg");
        let svc = VannakService::create(
            IptoPlacementMap::new(
                PlacementEpoch(1),
                vec![IptoPlacementSlot::new(IptoInstanceId::from("ipto-a"), 1).unwrap()],
                vec![],
            )
            .unwrap(),
            IptoMapping::new("v1").map_field("vannak:dataIndividualId", "vannak:dataIndividualId"),
            &path,
            SegmentId::from("daemon-seg"),
            NodeId::from("node-a"),
        )
        .unwrap();

        let daemon = Daemon::new(
            svc,
            DaemonConfig::default(),
            BackgroundWriterConfig::default(),
        );
        assert!(!daemon.is_stopping());

        let bw_snap = Arc::new(Mutex::new(None::<BackgroundWriterSnapshot>));
        let snap = daemon.snapshot(&bw_snap);
        assert_eq!(snap.uptime_secs, 0);
        assert!(snap.service.hot_index.event_count == 0);

        daemon.stop();
        assert!(daemon.is_stopping());
    }

    #[test]
    fn background_writer_drains_outbox_with_writer() {
        let dir = tempdir();
        let path = dir.join("bg-writer.seg");
        let target = IptoInstanceId::from("ipto-a");
        let mut svc = VannakService::create(
            IptoPlacementMap::new(
                PlacementEpoch(1),
                vec![IptoPlacementSlot::new(target.clone(), 1).unwrap()],
                vec![],
            )
            .unwrap(),
            IptoMapping::new("v1").map_field("vannak:dataIndividualId", "vannak:dataIndividualId"),
            &path,
            SegmentId::from("bw-seg"),
            NodeId::from("node-a"),
        )
        .unwrap();

        let event = DataIndividualMetadataEvent::new(
            MetadataEventId::from("meta-1"),
            DataIndividualId::from("data-1"),
            crate::data::DataIndividualShardId::from_data_individual(&DataIndividualId::from(
                "data-1",
            )),
            TenantId::from("tenant-a"),
            EnvironmentId::from("prod"),
            PipelineId::from("pipeline-a"),
            ProcessInstanceId::from("instance-a"),
            EventTimestamp::from("2026-06-30T12:00:00Z"),
            MetadataOperation::Received,
        )
        .with_passive_metadata(
            PassiveMetadata::new()
                .insert("vannak:dataIndividualId", MetadataValue::string("data-1")),
        );

        svc.capture_metadata_event(&event).unwrap();
        assert_eq!(svc.snapshot().metadata_outbox.outbox.pending, 1);

        let mut control = ClusterControlState::new();
        control
            .apply(ClusterControlCommand::AddNode(NodeId::from("node-a")))
            .unwrap();
        control
            .apply(ClusterControlCommand::GrantWriterLease(WriterLease {
                target: target.clone(),
                holder: NodeId::from("node-a"),
                epoch: LeaseEpoch(1),
            }))
            .unwrap();

        let mut recording = RecordingWriter::default();
        let summary = svc
            .drain_metadata_for_target_if_lease_held(
                &control,
                &NodeId::from("node-a"),
                &target,
                &mut recording,
                10,
            )
            .unwrap();

        assert_eq!(summary.acknowledged, 1);
        assert_eq!(recording.written, 1);
    }

    // -- helpers --

    fn tempdir() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir =
            std::env::temp_dir().join(format!("vannak-daemon-{}-{}", std::process::id(), nanos));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[derive(Default)]
    struct RecordingWriter {
        written: usize,
    }

    impl IptoWriter for RecordingWriter {
        fn write(
            &mut self,
            _payload: &crate::ipto::IptoWritePayload,
        ) -> Result<(), crate::ipto::IptoWriteError> {
            self.written += 1;
            Ok(())
        }
    }
}
