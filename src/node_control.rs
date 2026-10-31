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

//! Committed Raft control access for the supervisor's metadata writer.

use crate::cluster::{ClusterControlCommand, ClusterControlState};
use crate::control::ControlPlane;
use crate::raft_sm::{ClusterQuery, decode_snapshot, encode_command};
use graft_proto::raft;
use graft_runtime::handlers::RaftHandler;
use prost::Message;
use std::sync::Arc;

pub struct RaftControlPlane {
    handle: tokio::runtime::Handle,
    handler: Arc<RaftHandler>,
}

impl std::fmt::Debug for RaftControlPlane {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RaftControlPlane").finish_non_exhaustive()
    }
}

impl RaftControlPlane {
    pub fn new(handle: tokio::runtime::Handle, handler: Arc<RaftHandler>) -> Self {
        Self { handle, handler }
    }

    async fn request(&self, kind: &str, payload: Vec<u8>) -> Result<Vec<u8>, String> {
        let mut reply = self.handler.dispatch(kind, &payload).await;
        // Followers redirect through the existing peer transport. Never use a
        // follower's local lease cache as authorization to drain an outbox.
        for _ in 0..2 {
            let (success, leader) = if kind == "ClientQueryRequest" {
                let response = raft::ClientQueryResponse::decode(reply.as_slice())
                    .map_err(|e| e.to_string())?;
                (response.success, response.leader_id)
            } else {
                let response = raft::ClientCommandResponse::decode(reply.as_slice())
                    .map_err(|e| e.to_string())?;
                (response.success, response.leader_id)
            };
            if success || leader.is_empty() || leader == self.handler.node.lock().me.id {
                break;
            }
            let address = self
                .handler
                .client
                .peer_addr(&leader)
                .map_err(|error| error.to_string())?;
            reply = self
                .handler
                .client
                .send_rpc(address, kind, payload.clone())
                .await
                .map_err(|error| error.to_string())?;
        }
        Ok(reply)
    }
}

impl ControlPlane for RaftControlPlane {
    fn read(&self) -> Result<ClusterControlState, String> {
        self.handle.block_on(async {
            let request = raft::ClientQueryRequest {
                query: serde_json::to_vec(&ClusterQuery::ControlSnapshot)
                    .map_err(|e| e.to_string())?,
                ..Default::default()
            };
            let reply = self
                .request("ClientQueryRequest", request.encode_to_vec())
                .await?;
            let reply =
                raft::ClientQueryResponse::decode(reply.as_slice()).map_err(|e| e.to_string())?;
            if !reply.success {
                return Err(format!("control read: {} {}", reply.status, reply.message));
            }
            decode_snapshot(&reply.result)
        })
    }

    fn propose(&self, command: ClusterControlCommand) -> Result<(), String> {
        self.handle.block_on(async {
            let request = raft::ClientCommandRequest {
                command: encode_command(&command).map_err(|error| error.to_string())?,
                ..Default::default()
            };
            let reply = self
                .request("ClientCommandRequest", request.encode_to_vec())
                .await?;
            let reply =
                raft::ClientCommandResponse::decode(reply.as_slice()).map_err(|e| e.to_string())?;
            if !reply.success {
                return Err(format!(
                    "control proposal: {} {}",
                    reply.status, reply.message
                ));
            }
            if !reply.result.is_empty() {
                return Err(String::from_utf8_lossy(&reply.result).into_owned());
            }
            Ok(())
        })
    }
}
