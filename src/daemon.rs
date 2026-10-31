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

use crate::cluster::{CheckpointEpoch, ClusterControlCommand, ClusterControlState, NodeId};
use crate::ipto::{IptoWriter, MetadataOutboxDrainSummary};
use crate::service::{VannakService, VannakServiceError, VannakServiceSnapshot};
use crate::storage::{SegmentError, SegmentManifest};
use serde_json::Value;
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
    pub last_control_error: Option<String>,
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
    control_state: Arc<dyn crate::control::ControlPlane>,
    node_id: NodeId,
    writer: Arc<Mutex<dyn IptoWriter + Send>>,
    config: BackgroundWriterConfig,
    stop_token: Arc<AtomicBool>,
) -> BackgroundWriterSnapshot {
    run_background_writer_with_updates(
        service,
        control_state,
        node_id,
        writer,
        config,
        stop_token,
        None,
    )
}

fn run_background_writer_with_updates(
    service: Arc<Mutex<VannakService>>,
    control_state: Arc<dyn crate::control::ControlPlane>,
    node_id: NodeId,
    writer: Arc<Mutex<dyn IptoWriter + Send>>,
    config: BackgroundWriterConfig,
    stop_token: Arc<AtomicBool>,
    live_snapshot: Option<Arc<Mutex<Option<BackgroundWriterSnapshot>>>>,
) -> BackgroundWriterSnapshot {
    let mut snapshot = BackgroundWriterSnapshot::default();

    while !stop_token.load(Ordering::Relaxed) {
        snapshot.cycles += 1;
        snapshot.current_cycle = Some(snapshot.cycles);

        let control = match control_state.read() {
            Ok(control) => {
                snapshot.last_control_error = None;
                control
            }
            Err(error) => {
                snapshot.last_control_error = Some(error);
                if let Some(live) = &live_snapshot {
                    *live.lock().unwrap() = Some(snapshot.clone());
                }
                wait_for_writer_cycle(&stop_token, config.interval.min(Duration::from_secs(1)));
                continue;
            }
        };
        let targets = control
            .placement_map()
            .map(|map| map.instances())
            .unwrap_or_default();
        let mut cycle_summaries = Vec::new();
        let checkpoint_epoch = CheckpointEpoch(
            control
                .outbox_checkpoints()
                .values()
                .map(|checkpoint| checkpoint.epoch.0)
                .max()
                .unwrap_or(0)
                .saturating_add(snapshot.cycles),
        );
        for target in &targets {
            if control
                .writer_lease(target)
                .is_none_or(|lease| lease.holder != node_id)
            {
                continue;
            }
            let (result, checkpoints) = {
                let mut svc = service.lock().unwrap();
                let mut writer = writer.lock().unwrap();
                svc.metadata_outbox_mut()
                    .outbox_mut()
                    .retry_failed_for_target(target);
                let result = svc.drain_metadata_for_target_if_lease_held(
                    &control,
                    &node_id,
                    target,
                    &mut *writer,
                    config.max_drain_attempts_per_target,
                );
                let checkpoints = svc.acknowledged_checkpoints_for_target(target, checkpoint_epoch);
                (result, checkpoints)
            };
            if let Ok(summary) = result
                && !summary.is_empty()
            {
                snapshot.total_acknowledged += summary.acknowledged;
                snapshot.total_failed += summary.failed;
                cycle_summaries.push((target.clone(), summary));
            }
            // Retry uncommitted checkpoints even when this cycle had no writes.
            // Do not hold the service lock while waiting for consensus.
            for checkpoint in checkpoints {
                if control
                    .outbox_checkpoint(checkpoint.data_individual_shard_id, target)
                    .is_some_and(|committed| {
                        committed.segment_id == checkpoint.segment_id
                            && committed.last_acknowledged_offset
                                >= checkpoint.last_acknowledged_offset
                    })
                {
                    continue;
                }
                if let Err(error) =
                    control_state.propose(ClusterControlCommand::RecordOutboxCheckpoint(checkpoint))
                {
                    snapshot.last_control_error = Some(error);
                }
            }
        }

        if !cycle_summaries.is_empty() {
            snapshot.last_summary = Some(cycle_summaries);
        }
        if let Some(ref live_snapshot) = live_snapshot {
            *live_snapshot.lock().unwrap() = Some(snapshot.clone());
        }
        wait_for_writer_cycle(&stop_token, config.interval);
    }

    snapshot.current_cycle = Some(snapshot.cycles);
    if let Some(ref live_snapshot) = live_snapshot {
        *live_snapshot.lock().unwrap() = Some(snapshot.clone());
    }
    snapshot
}

fn wait_for_writer_cycle(stop: &AtomicBool, interval: Duration) {
    let deadline = Instant::now() + interval;
    while !stop.load(Ordering::Relaxed) && Instant::now() < deadline {
        std::thread::sleep(
            deadline
                .saturating_duration_since(Instant::now())
                .min(Duration::from_millis(50)),
        );
    }
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
    /// Maximum accepted request body size for POST endpoints.
    pub max_request_body_bytes: usize,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            bind_address: String::from("127.0.0.1:9090"),
            read_timeout: Duration::from_secs(10),
            max_request_body_bytes: 1024 * 1024,
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
/// Returns an I/O error if the listener cannot bind or cannot be configured as
/// nonblocking.
pub fn start_health_server(
    service: Arc<Mutex<VannakService>>,
    background_writer_snapshot: Arc<Mutex<Option<BackgroundWriterSnapshot>>>,
    config: DaemonConfig,
    start_time: Instant,
    stop_token: Arc<AtomicBool>,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(&config.bind_address)?;
    listener.set_nonblocking(true)?;

    let poll_interval = Duration::from_millis(100);

    while !stop_token.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((stream, _addr)) => {
                let svc = service.clone();
                let bw = background_writer_snapshot.clone();
                let uptime = start_time;
                let request_config = config.clone();
                std::thread::spawn(move || {
                    handle_health_request(stream, &svc, &bw, &request_config, uptime);
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
    Ok(())
}

fn handle_health_request(
    mut stream: TcpStream,
    service: &Arc<Mutex<VannakService>>,
    background_writer: &Arc<Mutex<Option<BackgroundWriterSnapshot>>>,
    config: &DaemonConfig,
    start_time: Instant,
) {
    let _ = stream.set_read_timeout(Some(config.read_timeout));
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

    let method = request_line.split_whitespace().next().unwrap_or_default();
    let path = request_line.split_whitespace().nth(1).unwrap_or_default();

    if method == "POST" && path == "/ingest/metadata" {
        let body = match read_request_body(&mut reader, content_length, config) {
            Ok(body) => body,
            Err((status, body)) => {
                write_json_response(&mut stream, status, &body);
                return;
            }
        };
        let (status, result) = handle_metadata_ingest_request(service, &body);
        write_json_response(&mut stream, status, &result);
        return;
    }

    if method == "POST" && path == "/ingest" {
        let body = match read_request_body(&mut reader, content_length, config) {
            Ok(body) => body,
            Err((status, body)) => {
                write_json_response(&mut stream, status, &body);
                return;
            }
        };

        let (status, result_json) = handle_ingest_request(service, &body);
        write_json_response(&mut stream, status, &result_json);
        return;
    }

    if method == "POST" && path == "/query" {
        let body = match read_request_body(&mut reader, content_length, config) {
            Ok(body) => body,
            Err((status, body)) => {
                write_json_response(&mut stream, status, &body);
                return;
            }
        };

        let (status, result_json) = handle_query_request(service, &body);
        write_json_response(&mut stream, status, &result_json);
        return;
    }

    let status = if method == "GET" && (path == "/" || path == "/health") {
        "200 OK"
    } else {
        "404 Not Found"
    };

    let body = if status == "200 OK" {
        build_health_json(service, background_writer, start_time)
    } else {
        r#"{"error":"not found"}"#.to_string()
    };

    write_json_response(&mut stream, status, &body);
}

fn read_request_body(
    reader: &mut BufReader<TcpStream>,
    content_length: Option<usize>,
    config: &DaemonConfig,
) -> Result<String, (&'static str, String)> {
    let Some(len) = content_length else {
        return Ok(String::new());
    };
    if len > config.max_request_body_bytes {
        return Err((
            "413 Payload Too Large",
            r#"{"error":"request body too large"}"#.to_string(),
        ));
    }
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf).map_err(|_| {
        (
            "400 Bad Request",
            r#"{"error":"failed to read request body"}"#.to_string(),
        )
    })?;
    Ok(String::from_utf8_lossy(&buf).to_string())
}

fn write_json_response(stream: &mut TcpStream, status: &str, body: &str) {
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

    match svc.ingest_process_event_configured(event) {
        Ok(crate::index::IngestOutcome::Accepted) => {
            ("200 OK", r#"{"status":"accepted"}"#.to_string())
        }
        Ok(crate::index::IngestOutcome::Duplicate) => {
            ("200 OK", r#"{"status":"duplicate"}"#.to_string())
        }
        Err(VannakServiceError::ProcessIngest(error)) => (
            "400 Bad Request",
            serde_json::json!({"error": error.to_string()}).to_string(),
        ),
        Err(error) => service_error_response(error),
    }
}

fn handle_metadata_ingest_request(
    service: &Arc<Mutex<VannakService>>,
    body: &str,
) -> (&'static str, String) {
    let event = match parse_metadata_event_json(body) {
        Ok(event) => event,
        Err(error) => {
            return (
                "400 Bad Request",
                serde_json::json!({"error": error}).to_string(),
            );
        }
    };
    let Ok(mut service) = service.lock() else {
        return (
            "503 Service Unavailable",
            r#"{"error":"service unavailable"}"#.into(),
        );
    };
    match service.capture_metadata_event(&event) {
        Ok(result) => ("200 OK", serde_json::json!({
            "status": if matches!(result.result, crate::ipto::DurableOutboxEnqueueResult::Duplicate) { "duplicate" } else { "accepted" },
            "target": result.target.as_str(),
        }).to_string()),
        Err(error) => service_error_response(error),
    }
}

fn parse_metadata_event_json(
    body: &str,
) -> Result<crate::data::DataIndividualMetadataEvent, String> {
    use crate::data::*;
    let value: Value = serde_json::from_str(body).map_err(|error| error.to_string())?;
    let required = |field| {
        json_string_field(&value, field)
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| format!("missing or empty {field}"))
    };
    let data_id = DataIndividualId::from(required("data_individual_id")?);
    let shard = match value.get("data_individual_shard_id") {
        Some(value) => {
            DataIndividualShardId(value.as_u64().ok_or("invalid data_individual_shard_id")?)
        }
        None => DataIndividualShardId::from_data_individual(&data_id),
    };
    let operation = match required("operation")?.as_str() {
        "Created" => MetadataOperation::Created,
        "Received" => MetadataOperation::Received,
        "Transformed" => MetadataOperation::Transformed {
            plugin_name: json_string_field(&value, "plugin_name").map(PluginName::from),
            plugin_version: json_string_field(&value, "plugin_version").map(PluginVersion::from),
        },
        "Masked" => MetadataOperation::Masked {
            fields: value
                .get("fields")
                .and_then(Value::as_array)
                .ok_or("Masked requires fields")?
                .iter()
                .map(|field| {
                    field
                        .as_str()
                        .map(String::from)
                        .ok_or("invalid masked field")
                })
                .collect::<Result<_, _>>()?,
        },
        "Validated" => MetadataOperation::Validated {
            passed: value
                .get("passed")
                .and_then(Value::as_bool)
                .ok_or("Validated requires passed")?,
        },
        "Enriched" => MetadataOperation::Enriched {
            source: json_string_field(&value, "source"),
        },
        "Routed" => MetadataOperation::Routed,
        "Persisted" => MetadataOperation::Persisted,
        _ => return Err("invalid metadata operation".into()),
    };
    let mut event = DataIndividualMetadataEvent::new(
        MetadataEventId::from(required("metadata_event_id")?),
        data_id,
        shard,
        crate::process::TenantId::from(required("tenant_id")?),
        crate::process::EnvironmentId::from(required("environment_id")?),
        crate::process::PipelineId::from(required("pipeline_id")?),
        crate::process::ProcessInstanceId::from(required("process_instance_id")?),
        crate::ingest::EventTimestamp::from(required("timestamp")?),
        operation,
    );
    if let Some(activity) = json_string_field(&value, "activity_id") {
        event = event.with_activity_id(crate::process::ActivityId::from(activity));
    }
    if let Some(payload) = json_string_field(&value, "source_payload_ref") {
        event = event.with_source_payload_ref(PayloadRef::from(payload));
    }
    let mut passive = PassiveMetadata::new();
    let mut active = ActiveMetadata::new();
    for (field, is_passive) in [("passive_metadata", true), ("active_metadata", false)] {
        if let Some(fields) = value.get(field) {
            for (name, value) in fields
                .as_object()
                .ok_or_else(|| format!("{field} must be an object"))?
            {
                let metadata_value = match value {
                    Value::String(value) => MetadataValue::string(value),
                    Value::Bool(value) => MetadataValue::Boolean(*value),
                    Value::Number(value) => MetadataValue::Integer(
                        value
                            .as_i64()
                            .ok_or("metadata numbers must be signed integers")?,
                    ),
                    Value::Array(values) => MetadataValue::StringList(
                        values
                            .iter()
                            .map(|value| {
                                value
                                    .as_str()
                                    .map(String::from)
                                    .ok_or("metadata lists must contain strings")
                            })
                            .collect::<Result<_, _>>()?,
                    ),
                    Value::Object(value) if value.len() == 1 => {
                        MetadataValue::Timestamp(crate::ingest::EventTimestamp::from(
                            value
                                .get("timestamp")
                                .and_then(Value::as_str)
                                .ok_or("expected timestamp object")?,
                        ))
                    }
                    _ => return Err("unsupported metadata value".into()),
                };
                if is_passive {
                    passive = passive.insert(name.as_str(), metadata_value);
                } else {
                    active = active.insert(name.as_str(), metadata_value);
                }
            }
        }
    }
    event = event
        .with_passive_metadata(passive)
        .with_active_metadata(active);
    event.validate().map_err(|error| error.to_string())?;
    Ok(event)
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
    let value: Value = serde_json::from_str(json).ok()?;
    let pi_id = json_string_field(&value, "process_instance_id")?;
    let pipeline_id = json_string_field(&value, "pipeline_id")?;
    let kind_str = json_string_field(&value, "kind")?;

    let kind = match kind_str.as_str() {
        "ProcessStarted" => crate::process::EventKind::ProcessStarted,
        "ProcessCompleted" => crate::process::EventKind::ProcessCompleted,
        "ProcessFailed" => crate::process::EventKind::ProcessFailed,
        "ActivityEntered" => crate::process::EventKind::ActivityEntered,
        "ActivityCompleted" => crate::process::EventKind::ActivityCompleted,
        "ActivityCancelled" => crate::process::EventKind::ActivityCancelled,
        "ActivityEscalated" => crate::process::EventKind::ActivityEscalated,
        "GatewayTaken" => crate::process::EventKind::GatewayTaken,
        _ => return None,
    };

    let event_id = json_string_field(&value, "event_id")
        .map(crate::ingest::EventId::from)
        .unwrap_or_else(|| {
            use std::sync::atomic::AtomicU64;
            static COUNTER: AtomicU64 = AtomicU64::new(1);
            crate::ingest::EventId::from(format!(
                "ingest-{}-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos(),
                COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            ))
        });

    let timestamp = json_string_field(&value, "timestamp")
        .map(crate::ingest::EventTimestamp::from)
        .unwrap_or_else(|| crate::ingest::EventTimestamp::from("unknown"));

    let tenant_id = json_string_field(&value, "tenant_id")
        .map(crate::process::TenantId::from)
        .unwrap_or_else(|| crate::process::TenantId::from("demo"));

    let environment_id = json_string_field(&value, "environment_id")
        .map(crate::process::EnvironmentId::from)
        .unwrap_or_else(|| crate::process::EnvironmentId::from("demo"));

    let activity_id =
        json_string_field(&value, "activity_id").map(crate::process::ActivityId::from);

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

    let Ok(value) = serde_json::from_str::<Value>(body) else {
        return (
            "400 Bad Request",
            r#"{"error":"invalid query JSON"}"#.to_string(),
        );
    };
    let Some(query_type) = json_string_field(&value, "type") else {
        return (
            "400 Bad Request",
            r#"{"error":"invalid query"}"#.to_string(),
        );
    };

    if query_type == "ProcessInstance"
        && let Some(id) = json_string_field(&value, "process_instance_id")
    {
        let result = svc.process_instance(&crate::query::ProcessInstanceQuery {
            process_instance_id: crate::process::ProcessInstanceId::from(id),
        });
        match result {
            Ok(Some(snap)) => {
                return (
                    "200 OK",
                    serialize_query_result(&crate::query::QueryResult::ProcessInstances(vec![
                        snap,
                    ])),
                );
            }
            Ok(None) => {
                return (
                    "200 OK",
                    r#"{"type":"ProcessInstances","instance":[]}"#.to_string(),
                );
            }
            Err(error) => return service_error_response(error),
        }
    }

    if query_type == "Pipeline"
        && let Some(id) = json_string_field(&value, "pipeline_id")
    {
        let limit_val = json_u64_field(&value, "limit").unwrap_or(100);
        let result = svc.pipeline_instances(&crate::query::PipelineQuery {
            pipeline_id: crate::process::PipelineId::from(id),
            limit: crate::query::QueryLimit::new(limit_val as usize),
        });
        return match result {
            Ok(result) => ("200 OK", serialize_query_result(&result)),
            Err(error) => service_error_response(error),
        };
    }

    if query_type == "ProcessStatus"
        && let Some(status_str) = json_string_field(&value, "status")
    {
        let status = match status_str.as_str() {
            "Active" => crate::process::ProcessStatus::Active,
            "Completed" => crate::process::ProcessStatus::Completed,
            "Failed" => crate::process::ProcessStatus::Failed,
            "Cancelled" => crate::process::ProcessStatus::Cancelled,
            _ => {
                return (
                    "400 Bad Request",
                    r#"{"error":"invalid process status"}"#.to_string(),
                );
            }
        };
        let limit_val = json_u64_field(&value, "limit").unwrap_or(100);
        let result = svc.process_instances_by_status(&crate::query::ProcessStatusQuery {
            status,
            limit: crate::query::QueryLimit::new(limit_val as usize),
        });
        return match result {
            Ok(result) => ("200 OK", serialize_query_result(&result)),
            Err(error) => service_error_response(error),
        };
    }

    let limit =
        crate::query::QueryLimit::new(json_u64_field(&value, "limit").unwrap_or(100) as usize);
    if query_type == "DataIndividualMetadata"
        && let Some(id) = json_string_field(&value, "data_individual_id")
    {
        let result = svc.metadata_for_data_individual(&crate::query::DataIndividualMetadataQuery {
            data_individual_id: crate::data::DataIndividualId::from(id),
            limit,
        });
        return ("200 OK", serialize_query_result(&result));
    }
    if query_type == "ProcessMetadata"
        && let Some(id) = json_string_field(&value, "process_instance_id")
    {
        let result = svc.metadata_for_process(&crate::query::ProcessMetadataQuery {
            process_instance_id: crate::process::ProcessInstanceId::from(id),
            limit,
        });
        return ("200 OK", serialize_query_result(&result));
    }
    if query_type == "ActivityMetadata"
        && let (Some(process), Some(activity)) = (
            json_string_field(&value, "process_instance_id"),
            json_string_field(&value, "activity_id"),
        )
    {
        let result = svc.metadata_for_activity(&crate::query::ActivityMetadataQuery {
            process_instance_id: crate::process::ProcessInstanceId::from(process),
            activity_id: crate::process::ActivityId::from(activity),
            limit,
        });
        return ("200 OK", serialize_query_result(&result));
    }

    (
        "400 Bad Request",
        r#"{"error":"invalid query"}"#.to_string(),
    )
}

fn service_error_response(error: VannakServiceError) -> (&'static str, String) {
    (
        "503 Service Unavailable",
        serde_json::json!({"error": error.to_string()}).to_string(),
    )
}

fn serialize_query_result(result: &crate::query::QueryResult) -> String {
    use crate::query::QueryResult;
    match result {
        QueryResult::ProcessInstances(instances) => serde_json::json!({"type":"ProcessInstances", "instance": instances.iter().map(|snapshot|
            serde_json::json!({"process_instance_id": snapshot.process_instance_id.as_str(), "pipeline_id": snapshot.pipeline_id.as_str(), "status": format!("{:?}", snapshot.status)})
        ).collect::<Vec<_>>()}),
        QueryResult::Events(events) => serde_json::json!({"type":"Events", "events": events.iter().map(|event|
            serde_json::json!({"event_id": event.event_id().as_str(), "pipeline_id": event.pipeline_id().as_str(), "process_instance_id": event.process_instance_id().as_str(), "kind": format!("{:?}", event.kind())})
        ).collect::<Vec<_>>()}),
        QueryResult::MetadataEvents(events) => serde_json::json!({"type":"MetadataEvents", "events": events.iter().map(|event|
            serde_json::json!({"metadata_event_id": event.metadata_event_id().as_str(), "data_individual_id": event.data_individual_id().as_str(), "operation": format!("{:?}", event.operation())})
        ).collect::<Vec<_>>()}),
    }.to_string()
}

fn json_string_field(value: &Value, field: &str) -> Option<String> {
    value.get(field)?.as_str().map(ToOwned::to_owned)
}

fn json_u64_field(value: &Value, field: &str) -> Option<u64> {
    value.get(field)?.as_u64()
}

fn build_health_json(
    service: &Arc<Mutex<VannakService>>,
    background_writer: &Arc<Mutex<Option<BackgroundWriterSnapshot>>>,
    start_time: Instant,
) -> String {
    let svc_snapshot = service.lock().ok().and_then(|svc| svc.snapshot().ok());
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
        if let Some(error) = &bw.last_control_error {
            bw_parts.push(format!(
                "\"last_control_error\":{}",
                serde_json::json!(error)
            ));
        }
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
    let mut instances: Vec<crate::process::ProcessInstanceSnapshot> = local
        .as_ref()
        .ok()
        .and_then(|value| value.clone())
        .into_iter()
        .collect();
    let mut failed = if local.is_err() {
        vec!["local".to_string()]
    } else {
        Vec::new()
    };
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
        Ok(crate::query::QueryResult::ProcessInstances(instances)) => instances,
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
        Ok(crate::query::QueryResult::ProcessInstances(instances)) => instances,
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
    let mut boundaries = from_map.interval_starts();
    boundaries.extend(to_map.interval_starts());
    let boundaries: Vec<_> = boundaries.into_iter().collect();
    let mut moved_ranges: Vec<(
        crate::data::DataIndividualShardId,
        crate::data::DataIndividualShardId,
        crate::ipto::IptoInstanceId,
        crate::ipto::IptoInstanceId,
    )> = Vec::new();
    for (index, &start) in boundaries.iter().enumerate() {
        let end = boundaries.get(index + 1).map_or(u64::MAX, |next| next - 1);
        let sid = crate::data::DataIndividualShardId(start);
        let (Some(from), Some(to)) = (from_map.resolve(sid), to_map.resolve(sid)) else {
            continue;
        };
        if from == to {
            continue;
        }
        if let Some((_, previous_end, previous_from, previous_to)) = moved_ranges.last_mut()
            && previous_end.0.checked_add(1) == Some(start)
            && previous_from == from
            && previous_to == to
        {
            *previous_end = crate::data::DataIndividualShardId(end);
        } else {
            moved_ranges.push((
                sid,
                crate::data::DataIndividualShardId(end),
                from.clone(),
                to.clone(),
            ));
        }
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
    background_threads: Vec<std::thread::JoinHandle<()>>,
}

impl Daemon {
    pub fn new(
        service: VannakService,
        config: DaemonConfig,
        writer_config: BackgroundWriterConfig,
    ) -> Self {
        Self::new_with_stop_token(
            service,
            config,
            writer_config,
            Arc::new(AtomicBool::new(false)),
        )
    }

    pub fn new_with_stop_token(
        service: VannakService,
        config: DaemonConfig,
        writer_config: BackgroundWriterConfig,
        stop_token: Arc<AtomicBool>,
    ) -> Self {
        Self {
            service: Arc::new(Mutex::new(service)),
            config,
            writer_config,
            start_time: Instant::now(),
            stop_token,
            background_threads: Vec::new(),
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
        &mut self,
        control_state: Arc<dyn crate::control::ControlPlane>,
        node_id: NodeId,
        writer: Arc<Mutex<dyn IptoWriter + Send>>,
    ) -> Arc<Mutex<Option<BackgroundWriterSnapshot>>> {
        let bw_snapshot = Arc::new(Mutex::new(None::<BackgroundWriterSnapshot>));
        let bw_snapshot_clone = bw_snapshot.clone();
        let service = self.service.clone();
        let config = self.writer_config.clone();
        let stop_token = self.stop_token.clone();

        let thread = std::thread::spawn(move || {
            let snapshot = run_background_writer_with_updates(
                service,
                control_state,
                node_id,
                writer,
                config,
                stop_token,
                Some(bw_snapshot_clone.clone()),
            );
            *bw_snapshot_clone.lock().unwrap() = Some(snapshot);
        });

        self.background_threads.push(thread);
        bw_snapshot
    }

    pub fn join_background_writers(&mut self) {
        for thread in self.background_threads.drain(..) {
            let _ = thread.join();
        }
    }

    /// Blocks the calling thread serving HTTP health requests.
    ///
    /// Returns when `stop()` is called from another thread.
    pub fn serve_health(
        &self,
        background_writer_snapshot: Arc<Mutex<Option<BackgroundWriterSnapshot>>>,
    ) -> std::io::Result<()> {
        start_health_server(
            self.service.clone(),
            background_writer_snapshot,
            self.config.clone(),
            self.start_time,
            self.stop_token.clone(),
        )
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
    ) -> Result<DaemonSnapshot, VannakServiceError> {
        let svc = self.service.lock().unwrap();
        Ok(DaemonSnapshot {
            service: svc.snapshot()?,
            background_writer: background_writer
                .lock()
                .ok()
                .and_then(|guard| guard.clone()),
            segment_discovery: None,
            uptime_secs: self.start_time.elapsed().as_secs(),
        })
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

    #[test]
    fn placement_change_covers_the_full_ring_without_scanning_shards() {
        let map = |epoch, target| {
            IptoPlacementMap::new(
                PlacementEpoch(epoch),
                vec![IptoPlacementSlot::new(IptoInstanceId::from(target), 16).unwrap()],
                vec![],
            )
            .unwrap()
        };
        let change = detect_placement_change(&map(1, "old"), &map(2, "new"));
        assert_eq!(
            change.moved_ranges,
            vec![(
                crate::data::DataIndividualShardId(0),
                crate::data::DataIndividualShardId(u64::MAX),
                IptoInstanceId::from("old"),
                IptoInstanceId::from("new")
            )]
        );
    }

    #[test]
    fn placement_change_preserves_source_target_pairs_and_inclusive_endpoints() {
        use crate::cluster::IptoPlacementRange;
        use crate::data::DataIndividualShardId as Shard;
        let range = |start, end, target| {
            IptoPlacementRange::new(Shard(start), Shard(end), IptoInstanceId::from(target)).unwrap()
        };
        let map = |epoch, overrides| {
            IptoPlacementMap::new(
                PlacementEpoch(epoch),
                vec![IptoPlacementSlot::new(IptoInstanceId::from("base"), 1).unwrap()],
                overrides,
            )
            .unwrap()
        };
        let old = map(1, vec![range(0, 9, "a"), range(10, 19, "b")]);
        let new = map(2, vec![range(0, 19, "c"), range(u64::MAX, u64::MAX, "d")]);
        assert_eq!(
            detect_placement_change(&old, &new).moved_ranges,
            vec![
                (
                    Shard(0),
                    Shard(9),
                    IptoInstanceId::from("a"),
                    IptoInstanceId::from("c")
                ),
                (
                    Shard(10),
                    Shard(19),
                    IptoInstanceId::from("b"),
                    IptoInstanceId::from("c")
                ),
                (
                    Shard(u64::MAX),
                    Shard(u64::MAX),
                    IptoInstanceId::from("base"),
                    IptoInstanceId::from("d")
                ),
            ]
        );
    }

    fn post_to_service(
        service: &Arc<Mutex<VannakService>>,
        path: &str,
        body: &str,
    ) -> (String, Value) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let service = service.clone();
        let thread = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            handle_health_request(
                stream,
                &service,
                &Arc::new(Mutex::new(None)),
                &DaemonConfig::default(),
                Instant::now(),
            );
        });
        let mut stream = TcpStream::connect(addr).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        write!(
            stream,
            "POST {path} HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
        .unwrap();
        stream.shutdown(std::net::Shutdown::Write).unwrap();
        let mut reply = String::new();
        stream.read_to_string(&mut reply).unwrap();
        thread.join().unwrap();
        let (headers, body) = reply.split_once("\r\n\r\n").unwrap();
        (
            headers.lines().next().unwrap().into(),
            serde_json::from_str(body).unwrap(),
        )
    }

    #[cfg(feature = "sitas-runtime")]
    #[test]
    fn http_and_kafka_share_journaled_shard_state_and_recover_metadata() {
        let dir = tempdir();
        let outbox = dir.join("outbox.seg");
        let journal = dir.join("process.seg");
        let placement = IptoPlacementMap::new(
            PlacementEpoch(1),
            vec![IptoPlacementSlot::new(IptoInstanceId::from("ipto-a"), 1).unwrap()],
            vec![],
        )
        .unwrap();
        let mut runtime =
            crate::SitasShardRuntime::start(crate::SitasRuntimeConfig::new(2, 16)).unwrap();
        runtime.start_mailbox_workers().unwrap();
        let runtime = Arc::new(runtime);
        let svc = VannakService::create(
            placement.clone(),
            IptoMapping::new("v1"),
            &outbox,
            SegmentId::from("outbox"),
            NodeId::from("node-a"),
        )
        .unwrap()
        .with_sitas_runtime(runtime.clone())
        .unwrap()
        .with_process_event_journal(
            crate::ProcessEventJournal::create(
                &journal,
                SegmentId::from("journal"),
                NodeId::from("node-a"),
            )
            .unwrap(),
        );
        let service = Arc::new(Mutex::new(svc));
        let body = r#"{"event_id":"http-1","process_instance_id":"http-instance","pipeline_id":"pipe","kind":"ProcessStarted","timestamp":"2026-07-01T10:00:00Z"}"#;
        assert!(post_to_service(&service, "/ingest", body).0.contains("200"));
        let event = parse_ingest_event_json(r#"{"event_id":"kafka-1","process_instance_id":"kafka-instance","pipeline_id":"pipe","kind":"ProcessStarted","timestamp":"2026-07-01T10:00:01Z"}"#).unwrap();
        let mut pending = crate::KafkaPendingRecord::new(crate::KafkaProcessRecord::new(
            crate::KafkaTopicPartition::new("process-events-pipe", 0),
            crate::KafkaOffset(7),
            event,
        ));
        let admitted = service
            .lock()
            .unwrap()
            .submit_kafka_record(&mut pending)
            .unwrap()
            .unwrap();
        assert_eq!(admitted.next_commit_offset, crate::KafkaOffset(8));
        assert!(admitted.journal_offset.is_some());
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let (_, result) = post_to_service(
                &service,
                "/query",
                r#"{"type":"ProcessInstance","process_instance_id":"kafka-instance"}"#,
            );
            if result["instance"].as_array().unwrap().len() == 1 {
                break;
            }
            assert!(Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(
            crate::replay_process_event_segment(&journal)
                .unwrap()
                .events
                .len(),
            2
        );
        let metadata = r#"{"metadata_event_id":"m1","data_individual_id":"data-1","tenant_id":"t","environment_id":"e","pipeline_id":"pipe","process_instance_id":"http-instance","timestamp":"2026-07-01T10:00:02Z","operation":"Received","passive_metadata":{"unmapped":"preserve me"},"active_metadata":{"validated":true}}"#;
        assert_eq!(
            post_to_service(&service, "/ingest/metadata", metadata).1["status"],
            "accepted"
        );
        assert_eq!(
            post_to_service(&service, "/ingest/metadata", metadata).1["status"],
            "duplicate"
        );
        assert!(
            post_to_service(&service, "/ingest/metadata", "{}")
                .0
                .contains("400")
        );
        drop(service);
        Arc::try_unwrap(runtime).ok().unwrap().stop().unwrap();
        let mut recovered = VannakService::recover_after(
            placement,
            IptoMapping::new("v1"),
            &outbox,
            SegmentId::from("outbox"),
            NodeId::from("node-a"),
            None,
        )
        .unwrap()
        .service;
        recovered
            .recover_process_events_into_hot_index(&journal)
            .unwrap();
        let service = Arc::new(Mutex::new(recovered));
        let (_, result) = post_to_service(
            &service,
            "/query",
            r#"{"type":"Pipeline","pipeline_id":"pipe"}"#,
        );
        assert_eq!(result["instance"].as_array().unwrap().len(), 2);
        let (_, result) = post_to_service(
            &service,
            "/query",
            r#"{"type":"DataIndividualMetadata","data_individual_id":"data-1"}"#,
        );
        assert_eq!(result["events"].as_array().unwrap().len(), 1);
        let recovered = service.lock().unwrap();
        let crate::QueryResult::MetadataEvents(events) =
            recovered.metadata_for_data_individual(&crate::DataIndividualMetadataQuery {
                data_individual_id: DataIndividualId::from("data-1"),
                limit: crate::QueryLimit::DEFAULT,
            })
        else {
            panic!("metadata query");
        };
        assert_eq!(
            events[0]
                .passive_metadata()
                .fields()
                .get(&crate::MetadataFieldName::from("unmapped")),
            Some(&MetadataValue::string("preserve me"))
        );
    }

    #[test]
    fn background_writer_retries_transient_delivery_and_failed_checkpoint_proposal() {
        use crate::control::ControlPlane;
        #[derive(Debug)]
        struct FlakyControl {
            state: Arc<Mutex<ClusterControlState>>,
            reject_checkpoint: AtomicBool,
        }
        impl ControlPlane for FlakyControl {
            fn read(&self) -> Result<ClusterControlState, String> {
                Ok(self.state.lock().unwrap().clone())
            }
            fn propose(&self, command: ClusterControlCommand) -> Result<(), String> {
                if matches!(&command, ClusterControlCommand::RecordOutboxCheckpoint(_))
                    && self.reject_checkpoint.swap(false, Ordering::Relaxed)
                {
                    return Err("temporary quorum failure".into());
                }
                self.state
                    .lock()
                    .unwrap()
                    .apply(command)
                    .map_err(|error| error.to_string())
            }
        }
        struct FlakyWriter {
            attempts: usize,
        }
        impl IptoWriter for FlakyWriter {
            fn write(&mut self, _: &crate::IptoWritePayload) -> Result<(), crate::IptoWriteError> {
                self.attempts += 1;
                if self.attempts == 1 {
                    Err(crate::IptoWriteError::retryable("offline"))
                } else {
                    Ok(())
                }
            }
        }
        let dir = tempdir();
        let target = IptoInstanceId::from("ipto-a");
        let node = NodeId::from("node-a");
        let placement = IptoPlacementMap::new(
            PlacementEpoch(1),
            vec![IptoPlacementSlot::new(target.clone(), 1).unwrap()],
            vec![],
        )
        .unwrap();
        let mut svc = VannakService::create(
            placement.clone(),
            IptoMapping::new("v1"),
            dir.join("outbox.seg"),
            SegmentId::from("outbox"),
            node.clone(),
        )
        .unwrap();
        let event = parse_metadata_event_json(r#"{"metadata_event_id":"retry","data_individual_id":"d","tenant_id":"t","environment_id":"e","pipeline_id":"p","process_instance_id":"i","timestamp":"2026-07-01T10:00:00Z","operation":"Received"}"#).unwrap();
        svc.capture_metadata_event(&event).unwrap();
        let state = Arc::new(Mutex::new(ClusterControlState::new()));
        {
            let mut state = state.lock().unwrap();
            state
                .apply(ClusterControlCommand::AddNode(node.clone()))
                .unwrap();
            state
                .apply(ClusterControlCommand::SetIptoPlacementMap(placement))
                .unwrap();
            state
                .apply(ClusterControlCommand::GrantWriterLease(WriterLease {
                    target: target.clone(),
                    holder: node.clone(),
                    epoch: LeaseEpoch(1),
                }))
                .unwrap();
        }
        let control = Arc::new(FlakyControl {
            state: state.clone(),
            reject_checkpoint: AtomicBool::new(true),
        });
        let writer = Arc::new(Mutex::new(FlakyWriter { attempts: 0 }));
        let mut daemon = Daemon::new(
            svc,
            DaemonConfig::default(),
            BackgroundWriterConfig {
                interval: Duration::from_millis(10),
                ..Default::default()
            },
        );
        let snapshot = daemon.start_background_writer(control, node, writer.clone());
        let deadline = Instant::now() + Duration::from_secs(5);
        while state
            .lock()
            .unwrap()
            .outbox_checkpoint(event.data_individual_shard_id(), &target)
            .is_none()
        {
            assert!(Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(5));
        }
        daemon.stop();
        daemon.join_background_writers();
        assert_eq!(writer.lock().unwrap().attempts, 2);
        let snapshot = snapshot.lock().unwrap().clone().unwrap();
        assert_eq!(snapshot.total_failed, 1);
        assert_eq!(snapshot.total_acknowledged, 1);
    }

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

    #[test]
    fn health_server_rejects_oversized_post_body() {
        let dir = tempdir();
        let path = dir.join("health-body-limit.seg");
        let svc = VannakService::create(
            IptoPlacementMap::new(
                PlacementEpoch(1),
                vec![IptoPlacementSlot::new(IptoInstanceId::from("ipto-a"), 1).unwrap()],
                vec![],
            )
            .unwrap(),
            IptoMapping::new("v1"),
            &path,
            SegmentId::from("health-body-limit-seg"),
            NodeId::from("node-a"),
        )
        .unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let service = Arc::new(Mutex::new(svc));
        let bw: Arc<Mutex<Option<BackgroundWriterSnapshot>>> = Arc::new(Mutex::new(None));
        let config = DaemonConfig {
            max_request_body_bytes: 8,
            ..DaemonConfig::default()
        };

        let server = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            handle_health_request(stream, &service, &bw, &config, Instant::now());
        });

        let mut client = TcpStream::connect(addr).unwrap();
        client
            .write_all(
                b"POST /ingest HTTP/1.1\r\nHost: localhost\r\nContent-Length: 16\r\n\r\n0123456789abcdef",
            )
            .unwrap();
        client.shutdown(std::net::Shutdown::Write).unwrap();

        let mut response = String::new();
        client.read_to_string(&mut response).unwrap();
        server.join().unwrap();

        assert!(response.starts_with("HTTP/1.1 413 Payload Too Large"));
        assert!(response.contains("request body too large"));
    }

    #[test]
    fn health_server_does_not_treat_every_get_as_health() {
        let dir = tempdir();
        let path = dir.join("health-route-test.seg");
        let svc = VannakService::create(
            IptoPlacementMap::new(
                PlacementEpoch(1),
                vec![IptoPlacementSlot::new(IptoInstanceId::from("ipto-a"), 1).unwrap()],
                vec![],
            )
            .unwrap(),
            IptoMapping::new("v1"),
            &path,
            SegmentId::from("health-route-test-seg"),
            NodeId::from("node-a"),
        )
        .unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let service = Arc::new(Mutex::new(svc));
        let bw: Arc<Mutex<Option<BackgroundWriterSnapshot>>> = Arc::new(Mutex::new(None));
        let config = DaemonConfig::default();

        let server = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            handle_health_request(stream, &service, &bw, &config, Instant::now());
        });

        let mut client = TcpStream::connect(addr).unwrap();
        client
            .write_all(b"GET /not-health HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .unwrap();
        client.shutdown(std::net::Shutdown::Write).unwrap();

        let mut response = String::new();
        client.read_to_string(&mut response).unwrap();
        server.join().unwrap();

        assert!(response.starts_with("HTTP/1.1 404 Not Found"));
        assert!(response.contains("not found"));
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
        let snap = daemon.snapshot(&bw_snap).unwrap();
        assert_eq!(snap.uptime_secs, 0);
        assert!(snap.service.hot_index.event_count == 0);

        daemon.stop();
        assert!(daemon.is_stopping());
    }

    #[test]
    fn daemon_can_share_external_stop_token() {
        let dir = tempdir();
        let path = dir.join("daemon-stop-token.seg");
        let svc = VannakService::create(
            IptoPlacementMap::new(
                PlacementEpoch(1),
                vec![IptoPlacementSlot::new(IptoInstanceId::from("ipto-a"), 1).unwrap()],
                vec![],
            )
            .unwrap(),
            IptoMapping::new("v1"),
            &path,
            SegmentId::from("daemon-stop-token-seg"),
            NodeId::from("node-a"),
        )
        .unwrap();
        let stop_token = Arc::new(AtomicBool::new(false));
        let daemon = Daemon::new_with_stop_token(
            svc,
            DaemonConfig::default(),
            BackgroundWriterConfig::default(),
            stop_token.clone(),
        );

        stop_token.store(true, Ordering::Relaxed);

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
        assert_eq!(svc.snapshot().unwrap().metadata_outbox.outbox.pending, 1);

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

    #[test]
    fn background_writer_publishes_live_snapshot() {
        let dir = tempdir();
        let path = dir.join("bg-live-snapshot.seg");
        let svc = VannakService::create(
            IptoPlacementMap::new(
                PlacementEpoch(1),
                vec![IptoPlacementSlot::new(IptoInstanceId::from("ipto-a"), 1).unwrap()],
                vec![],
            )
            .unwrap(),
            IptoMapping::new("v1"),
            &path,
            SegmentId::from("bg-live-snapshot-seg"),
            NodeId::from("node-a"),
        )
        .unwrap();
        let mut daemon = Daemon::new(
            svc,
            DaemonConfig::default(),
            BackgroundWriterConfig {
                interval: Duration::from_millis(10),
                max_drain_attempts_per_target: 1,
                verbose: false,
            },
        );
        let control_state = Arc::new(Mutex::new(ClusterControlState::new()));
        let writer: Arc<Mutex<dyn IptoWriter + Send>> =
            Arc::new(Mutex::new(RecordingWriter::default()));
        let snapshot =
            daemon.start_background_writer(control_state, NodeId::from("node-a"), writer);

        for _ in 0..50 {
            if snapshot.lock().unwrap().is_some() {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }

        let live = snapshot.lock().unwrap().clone();
        daemon.stop();

        assert!(live.is_some());
        assert!(live.unwrap().cycles > 0);
    }

    #[test]
    fn background_writer_records_acknowledged_checkpoints() {
        let dir = tempdir();
        let path = dir.join("bg-checkpoint.seg");
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
            SegmentId::from("bg-checkpoint-seg"),
            NodeId::from("node-a"),
        )
        .unwrap();

        let event = DataIndividualMetadataEvent::new(
            MetadataEventId::from("meta-checkpoint"),
            DataIndividualId::from("data-checkpoint"),
            crate::data::DataIndividualShardId::from_data_individual(&DataIndividualId::from(
                "data-checkpoint",
            )),
            TenantId::from("tenant-a"),
            EnvironmentId::from("prod"),
            PipelineId::from("pipeline-a"),
            ProcessInstanceId::from("instance-a"),
            EventTimestamp::from("2026-06-30T12:00:00Z"),
            MetadataOperation::Received,
        )
        .with_passive_metadata(PassiveMetadata::new().insert(
            "vannak:dataIndividualId",
            MetadataValue::string("data-checkpoint"),
        ));
        let shard_id = event.data_individual_shard_id();
        svc.capture_metadata_event(&event).unwrap();

        let mut daemon = Daemon::new(
            svc,
            DaemonConfig::default(),
            BackgroundWriterConfig {
                interval: Duration::from_millis(10),
                max_drain_attempts_per_target: 10,
                verbose: false,
            },
        );
        let control_state = Arc::new(Mutex::new(ClusterControlState::new()));
        {
            let mut control = control_state.lock().unwrap();
            control
                .apply(ClusterControlCommand::AddNode(NodeId::from("node-a")))
                .unwrap();
            control
                .apply(ClusterControlCommand::SetIptoPlacementMap(
                    IptoPlacementMap::new(
                        PlacementEpoch(1),
                        vec![IptoPlacementSlot::new(target.clone(), 1).unwrap()],
                        vec![],
                    )
                    .unwrap(),
                ))
                .unwrap();
            control
                .apply(ClusterControlCommand::GrantWriterLease(WriterLease {
                    target: target.clone(),
                    holder: NodeId::from("node-a"),
                    epoch: LeaseEpoch(1),
                }))
                .unwrap();
        }
        let writer: Arc<Mutex<dyn IptoWriter + Send>> =
            Arc::new(Mutex::new(RecordingWriter::default()));
        let snapshot =
            daemon.start_background_writer(control_state.clone(), NodeId::from("node-a"), writer);

        for _ in 0..50 {
            if snapshot
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|snapshot| snapshot.total_acknowledged > 0)
            {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        daemon.stop();

        let control = control_state.lock().unwrap();
        assert!(
            control
                .outbox_checkpoint(shard_id, &target)
                .is_some_and(|checkpoint| checkpoint.last_acknowledged_offset.0 > 0)
        );
    }

    #[test]
    fn ingest_json_parser_rejects_unknown_event_kind() {
        let event = parse_ingest_event_json(
            r#"{"process_instance_id":"p1","pipeline_id":"pipe","kind":"Bogus"}"#,
        );
        assert!(event.is_none());
    }

    #[test]
    fn query_json_parser_rejects_unknown_status() {
        let dir = tempdir();
        let path = dir.join("query-parser.seg");
        let svc = VannakService::create(
            IptoPlacementMap::new(
                PlacementEpoch(1),
                vec![IptoPlacementSlot::new(IptoInstanceId::from("ipto-a"), 1).unwrap()],
                vec![],
            )
            .unwrap(),
            IptoMapping::new("v1"),
            &path,
            SegmentId::from("query-parser-seg"),
            NodeId::from("node-a"),
        )
        .unwrap();
        let service = Arc::new(Mutex::new(svc));

        let (status, body) =
            handle_query_request(&service, r#"{"type":"ProcessStatus","status":"Bogus"}"#);

        assert_eq!(status, "400 Bad Request");
        assert!(body.contains("invalid process status"));
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
