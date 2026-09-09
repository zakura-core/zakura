//! Stable replay inputs. These are ownership events, not peer wire operations.

use serde::{Deserialize, Serialize};

use super::super::*;

pub(super) const REQUEST_SLOTS: usize = 4;
pub(super) const QUEUE_DEPTH: usize = 1;
pub(super) const RESPONSE_CAP: u64 = 2_000_010;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Limit {
    PeerActive,
    NodeActive,
}

impl Limit {
    pub(super) const ALL: [Self; 2] = [Self::PeerActive, Self::NodeActive];

    pub(super) fn config(self) -> ZakuraBlockSyncConfig {
        let mut config = ZakuraBlockSyncConfig {
            max_blocks_per_response: 1,
            max_inflight_requests: if self == Self::PeerActive { 1 } else { 4 },
            ..Default::default()
        };
        let policy = &mut config.get_blocks_regulation;
        policy.node_active_requests = if self == Self::NodeActive { 1 } else { 8 };
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
    Admit {
        peer: usize,
        request: usize,
    },
    Commit {
        request: usize,
    },
    ClaimQuery {
        request: usize,
    },
    CloneQueryLease {
        request: usize,
    },
    DropQueryLease {
        request: usize,
    },
    #[serde(alias = "drop_ledger")]
    DropProducer {
        request: usize,
    },
    QueueBlock {
        request: usize,
    },
    QueueTerminal {
        request: usize,
    },
    BeginWrite {
        session: usize,
    },
    EndWrite {
        session: usize,
        outcome: WriteEnd,
    },
    Reconnect {
        peer: usize,
    },
    Advance {
        millis: u64,
    },
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
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Snapshot {
    pub(super) node_active: usize,
    pub(super) peer_active: [usize; 2],
    pub(super) session_active: Vec<usize>,
}

/// One semantic result and its live resource accounting at a replay checkpoint.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Observation {
    pub(super) outcome: Outcome,
    pub(super) resources: Snapshot,
}
