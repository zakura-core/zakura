//! The block-sync scenarios P6b's semantic properties need, and the fuzz
//! tags that draw them (see [`super::block_sync::Storage`]'s `draw`).
//!
//! - [`BlockScenario::reset_mid_flight`]: a chain reset while two mirrors
//!   serve, so block sync submits bodies from both the old and the new
//!   body-work epoch, and some through first-completion-wins.
//! - [`BlockScenario::stale_beside_current`]: a reset detaches a held
//!   submission whose late verdict reaches block sync while a current
//!   submission waits beside it.
//! - [`BlockScenario::capacity_release`]: a reset detaches a full
//!   submission window, and one detached submission's commit frees room
//!   while a fork body waits for it.
//! - [`BlockScenario::stalled_reset`]: storage never answers a
//!   submission, and a reset detaches the first one.
//! - [`BlockScenario::decoded_input_stall`]: the sequencer stalls, as a
//!   slow verifier stalls it, and the mirror keeps delivering bodies after
//!   the input queue's decoded bytes fill the decoded-byte cap.

use super::{
    block_node::{
        control::{ControlAction, ControlStep, Fork},
        policy::{BlockAllow, BlockKind, Hold, Select, Trigger},
        Policy,
    },
    block_sync::{BlockScenario, MirrorSpec, SequencerPause},
    header_sync::Dialer,
};

/// The fuzz prefix that draws [`BlockScenario::reset_mid_flight`].
pub const RESET_MID_FLIGHT_TAG: [u8; 2] = [0xFF, 0x0C];

/// The fuzz prefix that draws [`BlockScenario::stale_beside_current`].
pub const STALE_BESIDE_CURRENT_TAG: [u8; 2] = [0xFF, 0x06];

/// The fuzz prefix that draws [`BlockScenario::capacity_release`].
pub const CAPACITY_RELEASE_TAG: [u8; 2] = [0xFF, 0x08];

/// The fuzz prefix that draws [`BlockScenario::stalled_reset`].
pub const STALLED_RESET_TAG: [u8; 2] = [0xFF, 0x0A];

/// The fuzz prefix that draws [`BlockScenario::decoded_input_stall`].
pub const DECODED_INPUT_STALL_TAG: [u8; 2] = [0xFF, 0x10];

/// Holds the operations `select` picks until `until`, then offers `then`.
fn hold(select: Select, until: Trigger, then: Vec<BlockAllow>) -> Hold {
    Hold {
        select,
        until,
        then,
        deadline_ms: None,
    }
}

/// Every operation of `kind`.
fn every(kind: BlockKind) -> Select {
    Select {
        kind: Some(kind),
        ..Select::default()
    }
}

/// Relinks `peer` once its connection closes, as a reset closes it.
fn relink(peer: &str) -> ControlStep {
    ControlStep {
        action: ControlAction::Relink {
            peer: peer.to_owned(),
        },
        when: Some(Trigger::Closed(peer.to_owned())),
    }
}

impl BlockScenario {
    /// Sixteen blocks from two mirrors, one dialing each way. Once the node
    /// submits its third body, every actor switches to a fork from height
    /// 8, the node first. The new epoch's work asks for heights whose
    /// old-epoch bodies may still be on the wire.
    pub fn reset_mid_flight() -> Self {
        BlockScenario {
            blocks: 16,
            mirrors: vec![
                MirrorSpec::new(Dialer::Mirror),
                MirrorSpec::new(Dialer::Node),
            ],
            control: vec![
                ControlStep {
                    action: ControlAction::Reset {
                        fork: Fork {
                            branch: 1,
                            at: 8,
                            len: 10,
                        },
                        node_first: true,
                        apply_when: None,
                    },
                    when: Some(Trigger::Submitted(Select::nth(BlockKind::Apply, 3))),
                },
                relink("mirror-0"),
            ],
            ..BlockScenario::default()
        }
    }

    /// Eight blocks, the first two already the node's. Storage holds the
    /// first submission's verdict for 5 s and every other for 20 s. Once
    /// the first is submitted, every actor switches to a fork from height
    /// 2, which detaches it. Its verdict then reaches block sync as a stale
    /// completion while the fork's first submission waits in storage.
    pub fn stale_beside_current() -> Self {
        let verdicts = vec![
            BlockAllow::Committed,
            BlockAllow::Duplicate,
            BlockAllow::Retryable,
        ];
        BlockScenario {
            blocks: 8,
            node_bodies: 2,
            policy: Policy {
                holds: vec![
                    hold(
                        Select::nth(BlockKind::Apply, 1),
                        Trigger::VirtualTime(5_000),
                        verdicts.clone(),
                    ),
                    hold(
                        Select {
                            kind: Some(BlockKind::Apply),
                            ..Select::default()
                        },
                        Trigger::VirtualTime(20_000),
                        verdicts,
                    ),
                ],
                ..Policy::converging()
            },
            control: vec![
                ControlStep {
                    action: ControlAction::Reset {
                        fork: Fork {
                            branch: 1,
                            at: 2,
                            len: 8,
                        },
                        node_first: false,
                        apply_when: None,
                    },
                    when: Some(Trigger::Submitted(Select::nth(BlockKind::Apply, 1))),
                },
                relink("mirror-0"),
            ],
            ..BlockScenario::default()
        }
    }

    /// 410 blocks, so the node fills its 401-submission window, which
    /// storage holds for 200 s. The 401st submission resets every actor to
    /// a fork from height 1, which detaches the whole window, so the fork's
    /// bodies wait for room. Mirror reads wait out the first 50 ms. The mirror's
    /// link closes after its 455th read, relinks, and closes again after its
    /// 470th. Once it has closed twice and 100 s have
    /// passed, storage commits the first submission, height 1, which the
    /// fork shares. That frees room beside the waiting fork body at height
    /// 2. Storage fails the 21st admission as retryable, and its verdict
    /// tears the node down.
    ///
    /// The commit both advances the view's tip and replies. When block sync
    /// handles the tip first, the frontier releases height 1, and the stale
    /// reply that follows frees room that must wake the waiting body
    /// (#1523).
    ///
    /// The teardown ends by step 478,000 on both committed inputs, after
    /// which only the mirror's metrics timer runs, so the run stops at
    /// 520,000 steps instead of the default budget's 837,000. #1522's
    /// decoded-budget reads and view publishes add preemption points, which
    /// moved the mirror's second close past the earlier 470,000-step budget.
    pub fn capacity_release() -> Self {
        let mirror = "mirror-0";
        let reads = |count: u32| {
            Trigger::Count(
                count,
                Box::new(Trigger::Completed(every(BlockKind::MirrorRead))),
            )
        };
        BlockScenario {
            blocks: 410,
            policy: Policy {
                holds: vec![
                    hold(
                        Select::nth(BlockKind::Apply, 1),
                        Trigger::All(vec![
                            Trigger::Count(2, Box::new(Trigger::Closed(mirror.to_owned()))),
                            Trigger::VirtualTime(100_000),
                        ]),
                        vec![BlockAllow::Committed],
                    ),
                    hold(
                        Select::nth(BlockKind::Apply, 21),
                        Trigger::VirtualTime(10),
                        vec![BlockAllow::Retryable],
                    ),
                    hold(
                        every(BlockKind::Apply),
                        Trigger::VirtualTime(200_000),
                        vec![BlockAllow::Committed, BlockAllow::Duplicate],
                    ),
                    hold(
                        every(BlockKind::MirrorRead),
                        Trigger::VirtualTime(50),
                        vec![BlockAllow::Serve],
                    ),
                ],
                ..Policy::converging()
            },
            control: vec![
                ControlStep {
                    action: ControlAction::Reset {
                        fork: Fork {
                            branch: 1,
                            at: 1,
                            len: 419,
                        },
                        node_first: false,
                        apply_when: None,
                    },
                    when: Some(Trigger::Submitted(Select::nth(BlockKind::Admit, 401))),
                },
                ControlStep {
                    action: ControlAction::Close {
                        peer: mirror.to_owned(),
                    },
                    when: Some(reads(455)),
                },
                relink(mirror),
                ControlStep {
                    action: ControlAction::Close {
                        peer: mirror.to_owned(),
                    },
                    when: Some(reads(470)),
                },
                ControlStep {
                    action: ControlAction::Teardown,
                    when: Some(Trigger::Completed(Select::nth(BlockKind::Apply, 21))),
                },
            ],
            steps: Some(520_000),
            ..BlockScenario::default()
        }
    }

    /// Eight blocks, the first two already the node's. Storage stalls: it
    /// never answers a submission. Once the first is submitted, every actor
    /// switches to a fork from height 2, which detaches it.
    pub fn stalled_reset() -> Self {
        BlockScenario {
            blocks: 8,
            node_bodies: 2,
            policy: Policy {
                holds: vec![hold(
                    every(BlockKind::Apply),
                    Trigger::VirtualTime(u64::MAX),
                    vec![BlockAllow::Committed],
                )],
                ..Policy::converging()
            },
            control: vec![
                ControlStep {
                    action: ControlAction::Reset {
                        fork: Fork {
                            branch: 1,
                            at: 2,
                            len: 8,
                        },
                        node_first: false,
                        apply_when: None,
                    },
                    when: Some(Trigger::Submitted(Select::nth(BlockKind::Apply, 1))),
                },
                relink("mirror-0"),
            ],
            ..BlockScenario::default()
        }
    }

    /// 320 blocks of 2,000,000 bytes from one mirror, the first two
    /// already the node's. Storage never answers a submission, and the
    /// node's sequencer task pauses once it publishes its first snapshot, as
    /// a slow verifier would stall it. The routine keeps handing bodies to
    /// the sequencer's input queue, which has room for every one. These
    /// padded bodies decode to about their wire size, so the queue's
    /// decoded bodies fill the 512 MiB decoded-byte cap after about 265
    /// bodies, and the mirror keeps delivering the rest. The node's
    /// look-ahead limit is 4 GiB: production's 1.5 GiB would stop downloads
    /// before the cap. The pause lasts 150,000 steps, which outlasts the
    /// downloads.
    pub fn decoded_input_stall() -> Self {
        BlockScenario {
            blocks: 320,
            body_bytes: 2_000_000,
            node_bodies: 2,
            max_reorder_lookahead_bytes: Some(4 << 30),
            policy: Policy {
                holds: vec![hold(
                    every(BlockKind::Apply),
                    Trigger::VirtualTime(u64::MAX),
                    vec![BlockAllow::Committed],
                )],
                ..Policy::converging()
            },
            sequencer_pause: Some(SequencerPause { steps: 150_000 }),
            ..BlockScenario::default()
        }
    }

    /// The semantic scenario a fuzz input's 2-byte prefix names, if it
    /// starts with one of the tags above.
    pub fn semantic_drawn(prefix: &[u8]) -> Option<Self> {
        match prefix.get(..2)? {
            tag if tag == RESET_MID_FLIGHT_TAG => Some(BlockScenario::reset_mid_flight()),
            tag if tag == STALE_BESIDE_CURRENT_TAG => Some(BlockScenario::stale_beside_current()),
            tag if tag == CAPACITY_RELEASE_TAG => Some(BlockScenario::capacity_release()),
            tag if tag == STALLED_RESET_TAG => Some(BlockScenario::stalled_reset()),
            tag if tag == DECODED_INPUT_STALL_TAG => Some(BlockScenario::decoded_input_stall()),
            _ => None,
        }
    }
}
