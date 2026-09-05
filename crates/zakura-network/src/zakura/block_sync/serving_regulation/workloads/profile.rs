//! Explicit capture policy and checked dependency timings for resource replay.

use serde::{Deserialize, Serialize};

use super::super::*;

/// Only fields read by serving admission and cost calculation belong here.
/// Requiring them prevents old captures from inheriting new production defaults.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Policy {
    pub max_blocks_per_response: u32,
    pub max_inflight_requests: u32,
    pub max_response_bytes: u32,
    pub max_inbound_peers: usize,
    pub max_outbound_peers: usize,
    pub request_overhead_bytes: u64,
    pub peer_rate_bytes_per_second: u64,
    pub peer_rate_capacity_bytes: u64,
    pub peer_outstanding_bytes: u64,
    pub node_rate_bytes_per_second: u64,
    pub node_rate_capacity_bytes: u64,
    pub node_outstanding_bytes: u64,
    pub peer_pending_requests: usize,
    pub node_pending_requests: usize,
    pub node_active_requests: usize,
    pub query_timeout_ms: u64,
}

impl Policy {
    pub fn config(&self) -> Result<ZakuraBlockSyncConfig, String> {
        let mut config = ZakuraBlockSyncConfig {
            max_blocks_per_response: self.max_blocks_per_response,
            max_inflight_requests: self.max_inflight_requests,
            max_response_bytes: self.max_response_bytes,
            get_blocks_regulation: GetBlocksRegulationConfig {
                request_overhead_bytes: self.request_overhead_bytes,
                peer_rate_bytes_per_second: self.peer_rate_bytes_per_second,
                peer_rate_capacity_bytes: self.peer_rate_capacity_bytes,
                peer_outstanding_bytes: self.peer_outstanding_bytes,
                node_rate_bytes_per_second: self.node_rate_bytes_per_second,
                node_rate_capacity_bytes: self.node_rate_capacity_bytes,
                node_outstanding_bytes: self.node_outstanding_bytes,
                peer_pending_requests: self.peer_pending_requests,
                node_pending_requests: self.node_pending_requests,
                node_active_requests: self.node_active_requests,
                query_timeout: Duration::from_millis(self.query_timeout_ms),
            },
            // Remaining fields govern downloading, not serving admission.
            ..ZakuraBlockSyncConfig::default()
        };
        config.peer_limits.max_inbound_peers = self.max_inbound_peers;
        config.peer_limits.max_outbound_peers = self.max_outbound_peers;
        validate_config(&config).map_err(str::to_owned)?;
        Ok(config)
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct Profile {
    pub version: u32,
    pub profile: String,
    pub time_unit: String,
    pub observation_boundary: String,
    pub all_observation_counters_reconciled: bool,
    pub completed_request_profiles_verified: bool,
    pub instantaneous_global_balances_reconstructed: bool,
    pub write_return_semantics: String,
    pub peers: usize,
    pub sessions: Vec<Session>,
    pub requests: Vec<Request>,
}

#[derive(Debug, Deserialize)]
pub(super) struct Session {
    pub peer: usize,
    pub session: usize,
    pub start_us: u64,
    pub end_us: u64,
}

#[derive(Debug, Deserialize)]
pub(super) struct Request {
    pub peer: usize,
    pub session: usize,
    pub message_sequence: u64,
    pub decoded_us: u64,
    pub retained_us: u64,
    pub admitted_us: u64,
    pub pending_release_us: [u64; 2],
    pub committed_us: u64,
    pub bound_us: u64,
    pub query_us: [u64; 2],
    pub settlement_us: [u64; 2],
    pub start_height: u32,
    pub count: u32,
    pub request_overhead: u64,
    pub response_cap: u64,
    pub frames: Vec<Frame>,
    pub waits: Vec<Wait>,
}

#[derive(Debug, Deserialize)]
pub(super) struct Frame {
    pub payload_bytes: u64,
    pub queued_us: u64,
    pub write_started_us: u64,
    pub write_returned_us: u64,
    pub release_us: [u64; 2],
}

#[derive(Debug, Deserialize)]
pub(super) struct Wait {
    pub stage: String,
    pub interval_us: [u64; 2],
}

impl Request {
    /// The admission loop can take its next input once its send becomes ready.
    pub fn forwarded_us(&self) -> u64 {
        self.waits
            .iter()
            .find(|wait| wait.stage == "reactor_queue")
            .expect("profile validation requires one completed reactor queue wait")
            .interval_us[1]
    }
}

fn ordered(times: &[u64]) -> bool {
    times.windows(2).all(|pair| pair[0] <= pair[1])
}

impl Profile {
    pub fn validate(&self, captured: &Policy, candidate: &Policy) -> Result<(), String> {
        if self.version != 1
            || self.profile != "completed_getblocks_application_lifetimes"
            || self.time_unit != "microseconds"
            || self.observation_boundary != "peer_routine_decode"
            || !self.all_observation_counters_reconciled
            || !self.completed_request_profiles_verified
            || self.instantaneous_global_balances_reconstructed
            || self.write_return_semantics != "success_or_error_not_peer_receipt"
        {
            return Err("unsupported or unreconciled workload profile".into());
        }
        let captured = captured.config()?;
        let candidate = candidate.config()?;
        if self.peers == 0 || self.sessions.is_empty() || self.requests.is_empty() {
            return Err("workload must contain peers, sessions and requests".into());
        }
        if captured.peer_limits.max_inbound_peers != candidate.peer_limits.max_inbound_peers
            || captured.peer_limits.max_outbound_peers != candidate.peer_limits.max_outbound_peers
        {
            return Err("peer connection admission is outside this resource profile".into());
        }
        // This bounded runner starts with a new process and full rate accounts.
        // Arbitrary slices with hidden initial owners are not valid episodes.
        const MAX_TIMESTAMP_US: u64 = 24 * 60 * 60 * 1_000_000;
        for (index, session) in self.sessions.iter().enumerate() {
            if session.session != index
                || session.peer >= self.peers
                || !ordered(&[session.start_us, session.end_us, MAX_TIMESTAMP_US])
            {
                return Err(format!("invalid session {index}"));
            }
        }
        let mut previous = vec![(0, 0); self.sessions.len()];
        for (index, request) in self.requests.iter().enumerate() {
            let invalid = || format!("invalid request dependency profile {index}");
            let session = self.sessions.get(request.session).ok_or_else(invalid)?;
            if request.peer != session.peer
                || request.message_sequence == 0
                || request.message_sequence <= previous[request.session].0
                || request.decoded_us < previous[request.session].1
                || request.count == 0
                || request.count > MAX_BS_BLOCKS_PER_REQUEST
                || !ordered(&[
                    session.start_us,
                    request.decoded_us,
                    request.retained_us,
                    request.admitted_us,
                    request.pending_release_us[0],
                    request.pending_release_us[1],
                    request.committed_us,
                    request.bound_us,
                    request.query_us[0],
                    request.query_us[1],
                    request.settlement_us[0],
                    request.settlement_us[1],
                    MAX_TIMESTAMP_US,
                ])
                || request.decoded_us > session.end_us
            {
                return Err(invalid());
            }
            previous[request.session] = (request.message_sequence, request.decoded_us);
            let cost = serving_cost(&captured, request.count).map_err(str::to_owned)?;
            let alternative = serving_cost(&candidate, request.count).map_err(str::to_owned)?;
            if cost.response_cap != request.response_cap
                || captured.get_blocks_regulation.request_overhead_bytes != request.request_overhead
                || alternative.response_cap != cost.response_cap
                || alternative.count != cost.count
            {
                return Err(format!(
                    "request {index}: captured response shape does not fit the policy"
                ));
            }
            let mut bytes = 0u64;
            for frame in &request.frames {
                if frame.payload_bytes == 0
                    || !ordered(&[
                        request.query_us[1],
                        frame.queued_us,
                        frame.write_started_us,
                        frame.write_returned_us,
                        frame.release_us[0],
                        frame.release_us[1],
                        MAX_TIMESTAMP_US,
                    ])
                    || frame.queued_us > request.settlement_us[0]
                {
                    return Err(invalid());
                }
                bytes = bytes.checked_add(frame.payload_bytes).ok_or_else(invalid)?;
            }
            if bytes > cost.response_cap || request.frames.is_empty() {
                return Err(invalid());
            }
            let queues: Vec<_> = request
                .waits
                .iter()
                .filter(|wait| wait.stage == "reactor_queue")
                .collect();
            if queues.len() != 1
                || !ordered(&[
                    request.admitted_us,
                    queues[0].interval_us[0],
                    queues[0].interval_us[1],
                    request.pending_release_us[0],
                ])
            {
                return Err(invalid());
            }
        }
        Ok(())
    }
}
