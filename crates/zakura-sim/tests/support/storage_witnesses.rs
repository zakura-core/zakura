//! `header-sync/storage` witness scenarios that both the committed seeds and
//! the tests that read them build: each is a typed scenario generator 3 can
//! encode, so the fuzzer can draw it from its seed.

use zakura_sim::harness::{
    header_node::{
        policy::{Allow, Hold, OpKind, Select, Trigger},
        Chain, ControlAction, ControlStep, Fork, Policy,
    },
    header_sync::{Dialer, MirrorSpec, StorageScenario},
};

/// The node's header applies attributed to `session`.
pub fn header_apply(session: u64) -> Select {
    Select {
        kind: Some(OpKind::Apply),
        session: Some(session),
        ..Select::default()
    }
}

/// The node at genesis, `mirror-0` at A5, and `mirror-1` on a heavier
/// branch from A1.
pub fn competing_mirrors() -> StorageScenario {
    StorageScenario {
        mirrors: vec![
            MirrorSpec {
                chain: Chain::main(5),
                dialer: Dialer::Mirror,
            },
            MirrorSpec {
                chain: Chain {
                    main: 1,
                    fork: Some(Fork { branch: 1, len: 6 }),
                },
                dialer: Dialer::Node,
            },
        ],
        ..StorageScenario::default()
    }
}

/// The node's first locator, held until the reactor's request timeout drops
/// its call, then completed.
pub fn dropped_locator() -> StorageScenario {
    StorageScenario {
        policy: Policy {
            holds: vec![Hold {
                select: Select::nth(OpKind::Locator, 1),
                until: Trigger::CallerDropped(Select::held()),
                then: vec![Allow::Engine],
                deadline_ms: None,
            }],
            ..Policy::converging()
        },
        ..StorageScenario::default()
    }
}

/// The node's first apply for `mirror-0`, held until the relinked
/// session's locator is submitted, then committed. The close retires the
/// applying request before its apply completes, as
/// `tests/header_node_late.rs`'s `apply_pending_at_close_still_commits`
/// does, with the triggers generator 3 has: the relink comes after the
/// close and the growth, so the second locator's submission implies both.
pub fn apply_at_close() -> StorageScenario {
    let control = |action, when| ControlStep {
        peer: "mirror-0".to_owned(),
        action,
        when: Some(when),
    };
    StorageScenario {
        policy: Policy {
            holds: vec![
                Hold {
                    select: header_apply(1),
                    until: Trigger::Submitted(Select::nth(OpKind::Locator, 2)),
                    then: vec![Allow::Engine],
                    deadline_ms: None,
                },
                Hold {
                    select: header_apply(2),
                    until: Trigger::Completed(header_apply(1)),
                    then: vec![Allow::Engine],
                    deadline_ms: None,
                },
            ],
            ..Policy::converging()
        },
        controls: vec![
            control(ControlAction::Close, Trigger::Submitted(header_apply(1))),
            control(
                ControlAction::Grow { len: 1 },
                Trigger::Closed("mirror-0".to_owned()),
            ),
            control(ControlAction::Relink, Trigger::Grown("mirror-0".to_owned())),
        ],
        ..StorageScenario::default()
    }
}
