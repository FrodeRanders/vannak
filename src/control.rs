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

//! Compact control-plane access. Raw events never cross this boundary.

use crate::cluster::{ClusterControlCommand, ClusterControlState};

pub trait ControlPlane: Send + Sync + std::fmt::Debug {
    /// A current control view. Raft adapters must pass a linearizable read
    /// barrier; an unavailable quorum must return an error, never local leases.
    fn read(&self) -> Result<ClusterControlState, String>;
    /// Return only once the command is committed and applied.
    fn propose(&self, command: ClusterControlCommand) -> Result<(), String>;
}

impl ControlPlane for std::sync::Mutex<ClusterControlState> {
    fn read(&self) -> Result<ClusterControlState, String> {
        self.lock()
            .map(|state| state.clone())
            .map_err(|error| error.to_string())
    }
    fn propose(&self, command: ClusterControlCommand) -> Result<(), String> {
        self.lock()
            .map_err(|error| error.to_string())?
            .apply(command)
            .map_err(|error| error.to_string())
    }
}
