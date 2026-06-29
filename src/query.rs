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

use crate::ingest::PipelineEvent;
use crate::metadata::MetadataRef;
use crate::process::{PipelineId, ProcessInstanceId, ProcessInstanceSnapshot};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessInstanceQuery {
    pub process_instance_id: ProcessInstanceId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineQuery {
    pub pipeline_id: PipelineId,
    pub limit: QueryLimit,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventQuery {
    pub process_instance_id: ProcessInstanceId,
    pub limit: QueryLimit,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImpactQuery {
    pub metadata_ref: MetadataRef,
    pub limit: QueryLimit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryLimit(usize);

impl QueryLimit {
    pub const DEFAULT: Self = Self(100);

    pub fn new(limit: usize) -> Self {
        Self(limit.max(1))
    }

    pub fn value(self) -> usize {
        self.0
    }

    pub(crate) fn reached(self, count: usize) -> bool {
        count >= self.0
    }
}

impl Default for QueryLimit {
    fn default() -> Self {
        Self::DEFAULT
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryResult {
    ProcessInstances(Vec<ProcessInstanceSnapshot>),
    Events(Vec<PipelineEvent>),
}
