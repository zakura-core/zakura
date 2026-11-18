//! The hostile block-sync scenarios: a probe peer sends one frame that
//! stream 6's table refuses, or a burst its cadence allows, and the node's
//! close must discharge the frame's `<label>-closes` ID (mutation plan,
//! mut-6). The `block_table` tests run each one against its ID, and
//! [`super::block_sync::Storage`]'s `draw` picks each by its tag, so the
//! generator reaches stream 6's refusal IDs.
//!
//! Each tag is `ff` then [`HOSTILE_TAG_BASE`] plus the scenario's index in
//! [`hostile_scenarios`]. Those prefixes drew the short budget before, and
//! no committed input starts with one.

use super::{
    block_node::{
        control::{ControlAction, ControlStep},
        policy::{BlockAllow, BlockKind, Hold, Select, Trigger},
        probe::ProbeFrame,
        Policy, PROBE,
    },
    block_sync::BlockScenario,
    ScenarioFamily,
};

/// The second tag byte of the first hostile scenario.
pub const HOSTILE_TAG_BASE: u8 = 0x20;

/// One hostile scenario: its family name, the stream-6 ID suffix it reaches,
/// and whether the probe's frame lands inside an exchange the node opened.
#[derive(Clone, Debug)]
pub struct Hostile {
    /// The scenario family's name.
    pub name: &'static str,
    /// The ID suffix under `block-sync/s6v3/` the scenario reaches.
    pub reaches: &'static str,
    /// True if the probe first advertises the chain, so the node asks it for
    /// bodies, and the faulty frame answers that request.
    pub in_exchange: bool,
    /// The scenario.
    pub scenario: BlockScenario,
}

impl BlockScenario {
    /// A node holding every fixture body, with no mirrors, and a probe that
    /// sends `frames` once the node's status has reached it.
    pub fn probe_sends(frames: Vec<ProbeFrame>) -> Self {
        BlockScenario {
            node_bodies: 8,
            mirrors: Vec::new(),
            probe: true,
            control: vec![ControlStep {
                action: ControlAction::ProbeSend { frames },
                when: Some(Trigger::VirtualTime(1_000)),
            }],
            ..BlockScenario::default()
        }
    }

    /// A node holding no body, with no mirrors, and a probe that advertises
    /// the whole chain with `max_response_bytes`, so the node asks it for
    /// bodies, then sends `frames` while that request is open.
    pub fn probe_answers(max_response_bytes: u32, frames: Vec<ProbeFrame>) -> Self {
        BlockScenario {
            node_bodies: 0,
            mirrors: Vec::new(),
            probe: true,
            control: vec![
                ControlStep {
                    action: ControlAction::ProbeSend {
                        frames: vec![ProbeFrame::LimitedStatus {
                            servable_low: 0,
                            servable_high: 8,
                            max_inflight_requests: 1,
                            max_response_bytes,
                        }],
                    },
                    when: None,
                },
                ControlStep {
                    action: ControlAction::ProbeSend { frames },
                    when: Some(Trigger::VirtualTime(2_000)),
                },
            ],
            ..BlockScenario::default()
        }
    }

    /// The node holds every fixture body and serves one request at a time.
    /// The probe sends three requests while the node's first two reads are
    /// held, which is past twice its limit.
    pub fn over_margin() -> Self {
        BlockScenario {
            serving_limit: Some(1),
            policy: Policy {
                holds: vec![held_read(1), held_read(2)],
                ..Policy::converging()
            },
            ..BlockScenario::probe_sends(vec![get_blocks(1, 1), get_blocks(2, 1), get_blocks(3, 1)])
        }
    }

    /// The tagged hostile scenarios [`BlockScenario::hostile_drawn`] picks.
    pub fn hostile_families() -> Vec<ScenarioFamily> {
        hostile_scenarios()
            .iter()
            .zip(HOSTILE_TAG_BASE..)
            .map(|(hostile, tag)| ScenarioFamily::new(hostile.name, [0xFF, tag]))
            .collect()
    }

    /// The hostile scenario a fuzz input's 2-byte prefix names, if it starts
    /// with one of [`BlockScenario::hostile_families`]' tags.
    pub fn hostile_drawn(prefix: &[u8]) -> Option<Self> {
        let [0xFF, tag, ..] = prefix else {
            return None;
        };
        let index = usize::from(tag.checked_sub(HOSTILE_TAG_BASE)?);
        hostile_scenarios()
            .into_iter()
            .nth(index)
            .map(|hostile| hostile.scenario)
    }
}

/// A hold on the node's `nth` range read until the probe's connection
/// closes.
pub fn held_read(nth: u32) -> Hold {
    Hold {
        select: Select::nth(BlockKind::Read, nth),
        until: Trigger::Closed(PROBE.to_owned()),
        then: vec![BlockAllow::Serve, BlockAllow::Error],
        deadline_ms: None,
    }
}

/// A request for `count` bodies from `start_height`.
pub fn get_blocks(start_height: u32, count: u32) -> ProbeFrame {
    ProbeFrame::GetBlocks {
        start_height,
        count,
    }
}

/// A raw frame of `message_type` with `flags` and a `len`-byte payload.
pub fn raw(message_type: u16, flags: u16, len: u32) -> ProbeFrame {
    ProbeFrame::Raw {
        message_type,
        flags,
        len,
    }
}

/// The raw frames whose header or length breaks a stream-6 rule: each
/// family's name, the ID suffix it reaches, and the frame's message type,
/// flags and payload length. Each flagged frame also breaks a content or
/// solicitation rule, which closes the connection without the flag check.
const RAW_FAULTS: [(&str, &str, u16, u16, u32); 15] = [
    (
        "status-reserved-flags",
        "status/reserved-flags-closes",
        1,
        1,
        53,
    ),
    (
        "status-short-payload",
        "status/short-payload-closes",
        1,
        0,
        52,
    ),
    (
        "status-long-payload",
        "status/long-payload-closes",
        1,
        0,
        54,
    ),
    (
        "get-blocks-reserved-flags",
        "get-blocks/reserved-flags-closes",
        2,
        1,
        9,
    ),
    (
        "get-blocks-short-payload",
        "get-blocks/short-payload-closes",
        2,
        0,
        8,
    ),
    (
        "get-blocks-long-payload",
        "get-blocks/long-payload-closes",
        2,
        0,
        10,
    ),
    (
        "block-reserved-flags",
        "block/reserved-flags-closes",
        3,
        1,
        2,
    ),
    ("block-short-payload", "block/short-payload-closes", 3, 0, 1),
    (
        "block-long-payload",
        "block/long-payload-closes",
        3,
        0,
        2_000_002,
    ),
    (
        "blocks-done-reserved-flags",
        "blocks-done/reserved-flags-closes",
        4,
        1,
        9,
    ),
    (
        "blocks-done-short-payload",
        "blocks-done/short-payload-closes",
        4,
        0,
        8,
    ),
    (
        "blocks-done-long-payload",
        "blocks-done/long-payload-closes",
        4,
        0,
        10,
    ),
    (
        "range-unavailable-reserved-flags",
        "range-unavailable/reserved-flags-closes",
        5,
        1,
        9,
    ),
    (
        "range-unavailable-short-payload",
        "range-unavailable/short-payload-closes",
        5,
        0,
        8,
    ),
    (
        "range-unavailable-long-payload",
        "range-unavailable/long-payload-closes",
        5,
        0,
        10,
    ),
];

/// Every hostile scenario, in tag order: the faults the node judges outside
/// any exchange of its own, then those inside its own exchange, then
/// requests past twice its serving limit. Appending keeps every earlier
/// tag's meaning.
pub fn hostile_scenarios() -> Vec<Hostile> {
    let outside = |name, reaches, frames| Hostile {
        name,
        reaches,
        in_exchange: false,
        scenario: BlockScenario::probe_sends(frames),
    };
    let inside = |name, reaches, max_response_bytes, frames| Hostile {
        name,
        reaches,
        in_exchange: true,
        scenario: BlockScenario::probe_answers(max_response_bytes, frames),
    };
    let mut scenarios = vec![
        outside(
            "undeclared-type",
            "undeclared-type-closes",
            vec![raw(9, 0, 1)],
        ),
        outside(
            "status-bad-content",
            "status/bad-content-closes",
            vec![raw(1, 0, 53)],
        ),
        outside(
            "get-blocks-bad-content",
            "get-blocks/bad-content-closes",
            vec![raw(2, 0, 9)],
        ),
        outside(
            "block-unsolicited",
            "block/unsolicited-closes",
            vec![raw(3, 0, 2)],
        ),
        outside(
            "blocks-done-unsolicited",
            "blocks-done/unsolicited-closes",
            vec![ProbeFrame::BlocksDone {
                start_height: 1,
                returned: 1,
            }],
        ),
        outside(
            "range-unavailable-unsolicited",
            "range-unavailable/unsolicited-closes",
            vec![ProbeFrame::RangeUnavailable {
                start_height: 1,
                count: 1,
            }],
        ),
        outside(
            "status-cadence-burst",
            "status/cadence-never-closes",
            (0..30)
                .map(|_| ProbeFrame::Status {
                    servable_low: 0,
                    servable_high: 0,
                })
                .collect(),
        ),
    ];
    scenarios.extend(
        RAW_FAULTS
            .into_iter()
            .map(|(name, reaches, message_type, flags, len)| {
                outside(name, reaches, vec![raw(message_type, flags, len)])
            }),
    );
    scenarios.extend([
        // A status that breaks the flag rule only.
        outside(
            "status-flag-only",
            "status/reserved-flags-closes",
            vec![ProbeFrame::FlaggedStatus {
                servable_low: 0,
                servable_high: 0,
                flags: 1,
            }],
        ),
        // The node's read for the first request stays held until the
        // probe's connection closes, so the overlap is certain.
        Hostile {
            name: "get-blocks-overlap",
            reaches: "get-blocks/bad-content-closes",
            in_exchange: false,
            scenario: BlockScenario {
                policy: Policy {
                    holds: vec![held_read(1)],
                    ..Policy::converging()
                },
                ..BlockScenario::probe_sends(vec![get_blocks(1, 1), get_blocks(1, 1)])
            },
        },
        inside(
            "block-over-owed",
            "block/over-owed-closes",
            1_000,
            vec![ProbeFrame::Block { height: 1 }],
        ),
        inside(
            "block-bad-content",
            "block/bad-content-closes",
            2_000_000,
            vec![raw(3, 0, 2)],
        ),
        inside(
            "block-tampered",
            "block/bad-content-closes",
            2_000_000,
            vec![ProbeFrame::TamperedBlock { height: 1 }],
        ),
        inside(
            "blocks-done-bad-content",
            "blocks-done/bad-content-closes",
            2_000_000,
            vec![ProbeFrame::BlocksDone {
                start_height: 1,
                returned: 2,
            }],
        ),
        inside(
            "range-unavailable-bad-content",
            "range-unavailable/bad-content-closes",
            2_000_000,
            vec![
                ProbeFrame::Block { height: 1 },
                ProbeFrame::RangeUnavailable {
                    start_height: 1,
                    count: 1,
                },
            ],
        ),
        Hostile {
            name: "get-blocks-over-margin",
            reaches: "get-blocks/over-margin-closes",
            in_exchange: false,
            scenario: BlockScenario::over_margin(),
        },
    ]);
    scenarios
}
