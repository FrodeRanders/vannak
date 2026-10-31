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

//! Process-index ownership boundary shared by service ingest and queries.

use crate::index::{HotIndex, IngestOutcome};
use crate::ingest::PipelineEvent;
use crate::observability::HotIndexSnapshot;
use crate::process::ProcessInstanceSnapshot;
use crate::query::*;
use crate::service::VannakServiceError;

#[derive(Debug)]
pub enum ProcessIndex {
    Local(HotIndex),
    #[cfg(feature = "sitas-runtime")]
    Sitas(SharedSitasIndex),
}

#[cfg(feature = "sitas-runtime")]
pub struct SharedSitasIndex(pub std::sync::Arc<crate::sitas_runtime::SitasShardRuntime>);

#[cfg(feature = "sitas-runtime")]
impl std::fmt::Debug for SharedSitasIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedSitasIndex").finish_non_exhaustive()
    }
}

impl ProcessIndex {
    pub fn ingest(&mut self, event: PipelineEvent) -> Result<IngestOutcome, VannakServiceError> {
        match self {
            Self::Local(index) => Ok(index.ingest(event)?),
            #[cfg(feature = "sitas-runtime")]
            Self::Sitas(index) => Ok(index.0.ingest(event)?.outcome),
        }
    }

    pub fn contains_event(&self, event: &PipelineEvent) -> Result<bool, VannakServiceError> {
        match self {
            Self::Local(index) => Ok(index.contains_event(event.event_id())),
            #[cfg(feature = "sitas-runtime")]
            Self::Sitas(index) => Ok(index
                .0
                .contains_event(event.process_instance_id(), event.event_id())?),
        }
    }

    pub fn snapshot(&self) -> Result<HotIndexSnapshot, VannakServiceError> {
        match self {
            Self::Local(index) => Ok(index.snapshot()),
            #[cfg(feature = "sitas-runtime")]
            Self::Sitas(index) => Ok(index.0.snapshot()?.runtime.totals),
        }
    }

    pub fn process_instance(
        &self,
        query: &ProcessInstanceQuery,
    ) -> Result<Option<ProcessInstanceSnapshot>, VannakServiceError> {
        match self {
            Self::Local(index) => Ok(index.process_instance(query)),
            #[cfg(feature = "sitas-runtime")]
            Self::Sitas(index) => Ok(index.0.process_instance(query)?),
        }
    }
    pub fn pipeline_instances(
        &self,
        query: &PipelineQuery,
    ) -> Result<QueryResult, VannakServiceError> {
        match self {
            Self::Local(index) => Ok(index.pipeline_instances(query)),
            #[cfg(feature = "sitas-runtime")]
            Self::Sitas(index) => Ok(index.0.pipeline_instances(query)?),
        }
    }
    pub fn events(&self, query: &EventQuery) -> Result<QueryResult, VannakServiceError> {
        match self {
            Self::Local(index) => Ok(index.events(query)),
            #[cfg(feature = "sitas-runtime")]
            Self::Sitas(index) => Ok(index.0.events(query)?),
        }
    }
    pub fn impact(&self, query: &ImpactQuery) -> Result<QueryResult, VannakServiceError> {
        match self {
            Self::Local(index) => Ok(index.impact(query)),
            #[cfg(feature = "sitas-runtime")]
            Self::Sitas(index) => Ok(index.0.impact(query)?),
        }
    }
    pub fn process_instances_by_status(
        &self,
        query: &ProcessStatusQuery,
    ) -> Result<QueryResult, VannakServiceError> {
        match self {
            Self::Local(index) => Ok(index.process_instances_by_status(query)),
            #[cfg(feature = "sitas-runtime")]
            Self::Sitas(index) => Ok(index.0.process_instances_by_status(query)?),
        }
    }
    pub fn events_in_time_range(
        &self,
        query: &TimeRangeQuery,
    ) -> Result<QueryResult, VannakServiceError> {
        match self {
            Self::Local(index) => Ok(index.events_in_time_range(query)),
            #[cfg(feature = "sitas-runtime")]
            Self::Sitas(index) => Ok(index.0.events_in_time_range(query)?),
        }
    }
}
