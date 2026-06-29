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

//! Append-only event segment storage.
//!
//! Segments are a dependency-free durability substrate for raw process events
//! and metadata outbox entries. The format is intentionally small:
//!
//! ```text
//! magic[8]
//! repeated:
//!   len: u32 little endian
//!   checksum: u64 little endian
//!   payload: [u8; len]
//! ```
//!
//! The checksum is a stable non-cryptographic checksum over the payload. It is
//! for corruption detection, not adversarial tamper resistance.

use crate::cluster::NodeId;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, Read, Write};
use std::path::{Path, PathBuf};

const SEGMENT_MAGIC: &[u8; 8] = b"VANNAK01";
const MAX_RECORD_LEN: usize = u32::MAX as usize;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SegmentId(String);

impl SegmentId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for SegmentId {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for SegmentId {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentManifest {
    pub segment_id: SegmentId,
    pub node_id: NodeId,
    pub path: PathBuf,
    pub record_count: u64,
    pub byte_len: u64,
    pub checksum: u64,
}

#[derive(Debug)]
pub struct SegmentWriter {
    segment_id: SegmentId,
    node_id: NodeId,
    path: PathBuf,
    file: File,
    record_count: u64,
    byte_len: u64,
    checksum: u64,
}

impl SegmentWriter {
    pub fn create(
        path: impl AsRef<Path>,
        segment_id: SegmentId,
        node_id: NodeId,
    ) -> Result<Self, SegmentError> {
        let path = path.as_ref().to_path_buf();
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        file.write_all(SEGMENT_MAGIC)?;
        Ok(Self {
            segment_id,
            node_id,
            path,
            file,
            record_count: 0,
            byte_len: SEGMENT_MAGIC.len() as u64,
            checksum: 0,
        })
    }

    pub fn append_record(&mut self, payload: &[u8]) -> Result<RecordOffset, SegmentError> {
        if payload.len() > MAX_RECORD_LEN {
            return Err(SegmentError::RecordTooLarge { len: payload.len() });
        }

        let offset = self.byte_len;
        let record_checksum = checksum(payload);
        let len = payload.len() as u32;

        self.file.write_all(&len.to_le_bytes())?;
        self.file.write_all(&record_checksum.to_le_bytes())?;
        self.file.write_all(payload)?;

        self.record_count += 1;
        self.byte_len += 4 + 8 + payload.len() as u64;
        self.checksum = combine_checksum(self.checksum, record_checksum);

        Ok(RecordOffset(offset))
    }

    pub fn flush(&mut self) -> Result<(), SegmentError> {
        self.file.flush()?;
        Ok(())
    }

    pub fn sync(&mut self) -> Result<(), SegmentError> {
        self.file.sync_all()?;
        Ok(())
    }

    pub fn manifest(&self) -> SegmentManifest {
        SegmentManifest {
            segment_id: self.segment_id.clone(),
            node_id: self.node_id.clone(),
            path: self.path.clone(),
            record_count: self.record_count,
            byte_len: self.byte_len,
            checksum: self.checksum,
        }
    }

    pub fn seal(mut self) -> Result<SegmentManifest, SegmentError> {
        self.flush()?;
        self.sync()?;
        Ok(self.manifest())
    }
}

#[derive(Debug)]
pub struct SegmentReader {
    reader: BufReader<File>,
    offset: u64,
}

impl SegmentReader {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, SegmentError> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut magic = [0u8; SEGMENT_MAGIC.len()];
        reader.read_exact(&mut magic)?;
        if &magic != SEGMENT_MAGIC {
            return Err(SegmentError::InvalidMagic);
        }
        Ok(Self {
            reader,
            offset: SEGMENT_MAGIC.len() as u64,
        })
    }

    pub fn read_next(&mut self) -> Result<Option<SegmentRecord>, SegmentError> {
        let mut len_buf = [0u8; 4];
        match self.reader.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(error) => return Err(error.into()),
        }

        let mut checksum_buf = [0u8; 8];
        self.reader.read_exact(&mut checksum_buf)?;

        let len = u32::from_le_bytes(len_buf) as usize;
        let expected_checksum = u64::from_le_bytes(checksum_buf);
        let mut payload = vec![0u8; len];
        self.reader.read_exact(&mut payload)?;

        let offset = self.offset;
        self.offset += 4 + 8 + len as u64;

        let actual_checksum = checksum(&payload);
        if actual_checksum != expected_checksum {
            return Err(SegmentError::ChecksumMismatch {
                offset,
                expected: expected_checksum,
                actual: actual_checksum,
            });
        }

        Ok(Some(SegmentRecord {
            offset: RecordOffset(offset),
            checksum: actual_checksum,
            payload,
        }))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RecordOffset(pub u64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentRecord {
    pub offset: RecordOffset,
    pub checksum: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug)]
pub enum SegmentError {
    Io(io::Error),
    InvalidMagic,
    RecordTooLarge {
        len: usize,
    },
    ChecksumMismatch {
        offset: u64,
        expected: u64,
        actual: u64,
    },
}

impl fmt::Display for SegmentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "segment I/O error: {error}"),
            Self::InvalidMagic => f.write_str("invalid Vannak segment magic"),
            Self::RecordTooLarge { len } => {
                write!(f, "segment record length {len} exceeds u32::MAX")
            }
            Self::ChecksumMismatch {
                offset,
                expected,
                actual,
            } => write!(
                f,
                "segment record checksum mismatch at offset {offset}: expected {expected:#x}, got {actual:#x}"
            ),
        }
    }
}

impl std::error::Error for SegmentError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::InvalidMagic | Self::RecordTooLarge { .. } | Self::ChecksumMismatch { .. } => {
                None
            }
        }
    }
}

impl From<io::Error> for SegmentError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

fn checksum(payload: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in payload {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn combine_checksum(current: u64, next: u64) -> u64 {
    current.rotate_left(7) ^ next
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn segment_round_trips_records_and_manifest() {
        let path = temp_segment_path("round-trip");
        let mut writer =
            SegmentWriter::create(&path, SegmentId::from("segment-a"), NodeId::new("node-a"))
                .unwrap();

        let first_offset = writer.append_record(b"first").unwrap();
        let second_offset = writer.append_record(b"second").unwrap();
        assert!(second_offset > first_offset);
        let manifest = writer.seal().unwrap();

        assert_eq!(manifest.record_count, 2);
        assert!(manifest.byte_len > SEGMENT_MAGIC.len() as u64);
        assert_ne!(manifest.checksum, 0);

        let mut reader = SegmentReader::open(&path).unwrap();
        let first = reader.read_next().unwrap().unwrap();
        let second = reader.read_next().unwrap().unwrap();
        assert_eq!(first.payload, b"first");
        assert_eq!(second.payload, b"second");
        assert!(reader.read_next().unwrap().is_none());

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn segment_rejects_invalid_magic() {
        let path = temp_segment_path("bad-magic");
        fs::write(&path, b"not-a-vannak-segment").unwrap();

        let error = SegmentReader::open(&path).unwrap_err();
        assert!(matches!(error, SegmentError::InvalidMagic));

        fs::remove_file(path).unwrap();
    }

    fn temp_segment_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("vannak-{name}-{nanos}.seg"))
    }
}
