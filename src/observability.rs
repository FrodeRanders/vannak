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

/// Owned point-in-time snapshot of a hot index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HotIndexSnapshot {
    pub process_instance_count: usize,
    pub event_count: usize,
    pub pipeline_count: usize,
    pub metadata_ref_count: usize,
    pub duplicate_events: u64,
    pub rejected_events: u64,
}

/// Live Durga schema compatibility tracking.
///
/// Accumulated over one consumer session; reported alongside consumer
/// snapshots so operators can detect schema drift before a new Durga version
/// introduces a breaking contract change.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DurgaCompatibilitySnapshot {
    /// Unknown status values encountered, with occurrence counts.
    pub unknown_status_values: Vec<(String, u64)>,
    /// Unknown event-type values encountered, with occurrence counts.
    pub unknown_event_type_values: Vec<(String, u64)>,
}
