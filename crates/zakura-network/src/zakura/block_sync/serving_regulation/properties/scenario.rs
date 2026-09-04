//! Stable replay inputs. These are ownership events, not peer wire operations.

use serde::{Deserialize, Serialize};

use super::super::*;

pub(super) const REQUEST_SLOTS: usize = 4;
pub(super) const INPUT_SLOTS: usize = 4;
pub(super) const QUEUE_DEPTH: usize = 2;
pub(super) const RESPONSE_CAP: u64 = 2_000_010;
pub(super) const OVERHEAD: u64 = 65_536;
pub(super) const CHARGE: u64 = RESPONSE_CAP + OVERHEAD;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Limit {
    PeerRate,
    NodeRate,
    PeerActive,
    NodeActive,
    PeerBytes,
    NodeBytes,
}

impl Limit {
    pub(super) const ALL: [Self; 6] = [
        Self::PeerRate,
        Self::NodeRate,
        Self::PeerActive,
        Self::NodeActive,
        Self::PeerBytes,
        Self::NodeBytes,
    ];

    pub(super) fn config(self) -> ZakuraBlockSyncConfig {
        let mut config = ZakuraBlockSyncConfig {
            max_blocks_per_response: 1,
            max_inflight_requests: if self == Self::PeerActive { 1 } else { 4 },
            ..Default::default()
        };
        let policy = &mut config.get_blocks_regulation;
        policy.request_overhead_bytes = OVERHEAD;
        policy.peer_rate_capacity_bytes = CHARGE * if self == Self::PeerRate { 1 } else { 8 };
        policy.node_rate_capacity_bytes = CHARGE * if self == Self::NodeRate { 1 } else { 16 };
        policy.peer_rate_bytes_per_second = CHARGE;
        policy.node_rate_bytes_per_second = CHARGE * 2;
        policy.peer_outstanding_bytes = RESPONSE_CAP * if self == Self::PeerBytes { 1 } else { 8 };
        policy.node_outstanding_bytes = RESPONSE_CAP * if self == Self::NodeBytes { 1 } else { 16 };
        policy.node_active_requests = if self == Self::NodeActive { 1 } else { 8 };
        policy.peer_pending_requests = 2;
        policy.node_pending_requests = 3;
        config
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum WriteEnd {
    Complete,
    Fail,
    Cancel,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum Action {
    Admit { peer: usize, request: usize },
    Commit { request: usize },
    ClaimQuery { request: usize },
    CloneQueryLease { request: usize },
    DropQueryLease { request: usize },
    DropLedger { request: usize },
    QueueBlock { request: usize },
    QueueTerminal { request: usize },
    BeginWrite { session: usize },
    EndWrite { session: usize, outcome: WriteEnd },
    RetainInput { peer: usize, input: usize },
    DropInput { input: usize },
    Reconnect { peer: usize },
    Advance { millis: u64 },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Scenario {
    pub(super) version: u8,
    pub(super) limit: Limit,
    pub(super) actions: Vec<Action>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum Outcome {
    Done,
    Admission(Option<Limit>),
    Started(bool),
    Queued(bool),
    Retained(bool),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Snapshot {
    pub(super) node_rate: u64,
    pub(super) peer_rates: [u64; 2],
    pub(super) node_bytes: u64,
    pub(super) session_bytes: Vec<u64>,
    pub(super) node_active: usize,
    pub(super) session_active: Vec<usize>,
    pub(super) node_pending: usize,
    pub(super) session_pending: Vec<usize>,
}

/// One semantic result and its live resource accounting at a replay checkpoint.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Observation {
    pub(super) outcome: Outcome,
    pub(super) resources: Snapshot,
}
