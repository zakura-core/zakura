//! `header-sync/storage` witness scenarios that both the committed seeds and
//! the tests that read them build: each is a typed scenario generator 3 can
//! encode, so the fuzzer can draw it from its seed.

// Not every test that shares this module names them.
#[allow(unused_imports)]
pub use zakura_sim::harness::header_sync::liveness::{OWNER, TAKER};
use zakura_sim::harness::{
    header_node::{
        policy::{Allow, Hold, OpKind, ReplyHold, Select, Trigger},
        Chain, ControlAction, ControlStep, Fork, Policy, Reset,
    },
    header_sync::liveness::RecoveryFixture,
    header_sync::{Dialer, MirrorSpec, StorageScenario, Weights},
};

/// The node under test's name.
pub const NODE: &str = "node";

/// The mirror most witnesses sync from.
pub const SOURCE: &str = "mirror-0";

/// `action` on `peer`, offered once `when` holds, or from the start.
pub fn step(peer: &str, action: ControlAction, when: Option<Trigger>) -> ControlStep {
    ControlStep {
        peer: peer.to_owned(),
        action,
        when,
    }
}

/// A mirror on `chain` that `dialer` links.
pub fn mirror(chain: Chain, dialer: Dialer) -> MirrorSpec {
    MirrorSpec {
        chain,
        dialer,
        max_headers_per_response: None,
    }
}

/// A hold of `select` until `until`, offering `then`.
pub fn hold(select: Select, until: Trigger, then: Allow) -> Hold {
    Hold {
        select,
        until,
        then: vec![then],
        deadline_ms: None,
    }
}

/// The node at `node` syncing from one mirror at `source` that dials it,
/// under the converging policy with `holds` and `controls`.
pub fn syncing(
    node: u32,
    source: u32,
    holds: Vec<Hold>,
    controls: Vec<ControlStep>,
) -> StorageScenario {
    StorageScenario {
        node: Chain::main(node),
        mirrors: vec![mirror(Chain::main(source), Dialer::Mirror)],
        policy: Policy {
            holds,
            ..Policy::converging()
        },
        controls,
        weights: Weights::default(),
    }
}

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
                max_headers_per_response: None,
            },
            MirrorSpec {
                chain: Chain {
                    main: 1,
                    fork: Some(Fork { branch: 1, len: 6 }),
                },
                dialer: Dialer::Node,
                max_headers_per_response: None,
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

/// A reset of the node, offered once `when` holds.
pub fn reset(reset: Reset, when: Trigger) -> ControlStep {
    step(NODE, ControlAction::Reset(reset), Some(when))
}

/// The node at `node` syncing from `mirror-0` at A6, with the first `kind`
/// operation held until the node resets. The reset invalidates the node's
/// tip once that operation is submitted, and a reconsider restores it once
/// the operation completes.
fn reset_held(kind: OpKind, node: u32) -> StorageScenario {
    syncing(
        node,
        6,
        vec![hold(
            Select::nth(kind, 1),
            Trigger::Reset(NODE.to_owned()),
            Allow::Engine,
        )],
        vec![
            reset(
                Reset::Invalidate { depth: 0 },
                Trigger::Submitted(Select::nth(kind, 1)),
            ),
            reset(Reset::Reconsider, Trigger::Completed(Select::nth(kind, 1))),
        ],
    )
}

/// [`reset_held`] for the node's first locator, from A3.
pub fn reset_held_locator() -> StorageScenario {
    reset_held(OpKind::Locator, 3)
}

/// [`reset_held`] for the node's first prepare, from A3.
pub fn reset_held_prepare() -> StorageScenario {
    reset_held(OpKind::Prepare, 3)
}

/// [`reset_held`] for the node's first apply, from A2, so the held apply's
/// headers extend the invalidated header.
pub fn reset_held_apply() -> StorageScenario {
    reset_held(OpKind::Apply, 2)
}

/// The node at A2 syncing from A6, its first apply's reply held until the
/// node resets, and the reset made once that apply commits: commit,
/// invalidate, deliver.
pub fn reset_reply_hold() -> StorageScenario {
    let mut scenario = syncing(
        2,
        6,
        vec![],
        vec![reset(
            Reset::Invalidate { depth: 0 },
            Trigger::Committed(Select::nth(OpKind::Apply, 1)),
        )],
    );
    scenario.policy.reply_holds = vec![ReplyHold {
        select: Select::nth(OpKind::Apply, 1),
        until: Trigger::Reset(NODE.to_owned()),
    }];
    scenario
}

/// The node at A3 syncing from A6: an invalidation once the first apply
/// completes, and a reconsider once the next prepare is submitted, while
/// that round runs.
pub fn invalidate_reconsider() -> StorageScenario {
    syncing(
        3,
        6,
        vec![],
        vec![
            reset(
                Reset::Invalidate { depth: 0 },
                Trigger::Completed(Select::nth(OpKind::Apply, 1)),
            ),
            reset(
                Reset::Reconsider,
                Trigger::Submitted(Select::nth(OpKind::Prepare, 2)),
            ),
        ],
    )
}

/// The node at A5 serving `mirror-0` at genesis and syncing from
/// `mirror-1` at A7. Its first page read is held until a `Release`, which
/// the Plan places once the node's first apply, which the read doesn't
/// depend on, has completed.
pub fn release_after_apply() -> StorageScenario {
    StorageScenario {
        node: Chain::main(5),
        mirrors: vec![
            mirror(Chain::default(), Dialer::Node),
            mirror(Chain::main(7), Dialer::Mirror),
        ],
        policy: Policy {
            holds: vec![hold(
                Select::nth(OpKind::Read, 1),
                Trigger::Released,
                Allow::Engine,
            )],
            ..Policy::converging()
        },
        controls: vec![step(
            NODE,
            ControlAction::Release { hold: 0 },
            Some(Trigger::Completed(Select::nth(OpKind::Apply, 1))),
        )],
        weights: Weights::default(),
    }
}

/// The node at genesis syncing from `mirror-0` at A5 and serving
/// `mirror-1`, which starts at genesis too. The first apply's reply is held
/// until the node has served `mirror-1` its first page, which the node can
/// do only from the committed headers.
pub fn reply_hold() -> StorageScenario {
    StorageScenario {
        node: Chain::default(),
        mirrors: vec![
            mirror(Chain::main(5), Dialer::Mirror),
            mirror(Chain::default(), Dialer::Node),
        ],
        policy: Policy {
            reply_holds: vec![ReplyHold {
                select: Select::nth(OpKind::Apply, 1),
                until: Trigger::Completed(Select::nth(OpKind::Read, 1)),
            }],
            ..Policy::converging()
        },
        controls: Vec::new(),
        weights: Weights::default(),
    }
}

/// The node at genesis syncing from A5, its first apply's reply held until
/// `until`, and `action` on `peer` once that apply commits.
fn reply_then(peer: &str, action: ControlAction, until: Trigger) -> StorageScenario {
    let mut scenario = syncing(
        0,
        5,
        vec![],
        vec![step(
            peer,
            action,
            Some(Trigger::Committed(Select::nth(OpKind::Apply, 1))),
        )],
    );
    scenario.policy.reply_holds = vec![ReplyHold {
        select: Select::nth(OpKind::Apply, 1),
        until,
    }];
    scenario
}

/// Commit, then a shutdown drops the reactor's apply future, then deliver
/// 2 s past the commit: the reply reaches no waiter.
pub fn reply_after_shutdown() -> StorageScenario {
    reply_then(NODE, ControlAction::Shutdown, Trigger::VirtualTime(2_000))
}

/// The passing control for [`reply_after_shutdown`]: closing the link
/// retires the apply's owner but leaves its future waiting, so the reply,
/// held until the link has closed, is delivered.
pub fn reply_after_close() -> StorageScenario {
    reply_then(
        SOURCE,
        ControlAction::Close,
        Trigger::Closed(SOURCE.to_owned()),
    )
}

/// The node at genesis syncing from A5, its first apply held until storage
/// stops, then a shutdown once that apply is submitted and a storage stop.
pub fn stop_with_held_apply() -> StorageScenario {
    syncing(
        0,
        5,
        vec![hold(
            Select::nth(OpKind::Apply, 1),
            Trigger::StorageStopped(NODE.to_owned()),
            Allow::Engine,
        )],
        vec![
            step(
                NODE,
                ControlAction::Shutdown,
                Some(Trigger::Submitted(Select::nth(OpKind::Apply, 1))),
            ),
            step(NODE, ControlAction::StopStorage, None),
        ],
    )
}

/// The node at A5 serving `mirror-0` at genesis, its first page read held
/// for good, so that session's peer lease stays held. A publisher page job
/// outlives its session, so a close and relink of `mirror-0` start a new
/// session whose page job acquires for the same peer, at the peer class's
/// limit of one lease, and is refused `CapacityBusy`.
pub fn peer_lease_denied() -> StorageScenario {
    let read = || Select::nth(OpKind::Read, 1);
    StorageScenario {
        node: Chain::main(5),
        mirrors: vec![mirror(Chain::default(), Dialer::Mirror)],
        policy: Policy {
            holds: vec![hold(read(), Trigger::Released, Allow::Engine)],
            ..Policy::converging()
        },
        controls: vec![
            step(
                SOURCE,
                ControlAction::Close,
                Some(Trigger::Submitted(read())),
            ),
            step(
                SOURCE,
                ControlAction::Relink,
                Some(Trigger::Closed(SOURCE.to_owned())),
            ),
        ],
        weights: Weights::default(),
    }
}

/// The node at genesis syncing from A5 and serving a second mirror at
/// genesis, with the first `kind` operation completing with `fault` once
/// `after` milliseconds pass.
pub fn fault(kind: OpKind, fault: Allow, after: u64) -> StorageScenario {
    StorageScenario {
        node: Chain::default(),
        mirrors: vec![
            mirror(Chain::main(5), Dialer::Mirror),
            mirror(Chain::default(), Dialer::Node),
        ],
        policy: Policy {
            holds: vec![hold(
                Select::nth(kind, 1),
                Trigger::VirtualTime(after),
                fault,
            )],
            ..Policy::converging()
        },
        controls: Vec::new(),
        weights: Weights::default(),
    }
}

/// A mirror holding `len` headers on `branch`, forked from genesis.
fn fork(branch: u8, len: u32) -> MirrorSpec {
    mirror(
        Chain {
            main: 0,
            fork: Some(Fork { branch, len }),
        },
        Dialer::Mirror,
    )
}

/// The node at genesis, `mirror-0` on `len_1` headers of branch 1 and
/// `mirror-1` on `len_2` headers of branch 2, under `weights`.
fn forks(len_1: u32, len_2: u32, weights: Weights) -> StorageScenario {
    StorageScenario {
        node: Chain::default(),
        mirrors: vec![fork(1, len_1), fork(2, len_2)],
        weights,
        policy: Policy::converging(),
        controls: Vec::new(),
    }
}

/// Equal work at equal length: four headers on each branch, both at
/// weight 1.
pub fn equal_forks() -> StorageScenario {
    forks(
        4,
        4,
        Weights {
            branch_1: 1,
            branch_2: 1,
        },
    )
}

/// A longer branch with more work: five unweighted headers on branch 1,
/// three on branch 2.
pub fn longer_fork() -> StorageScenario {
    forks(5, 3, Weights::default())
}

/// A shorter weighted branch with more work than a longer one: five
/// unweighted headers on branch 1, three at weight 2 on branch 2.
pub fn weighted_fork() -> StorageScenario {
    forks(
        5,
        3,
        Weights {
            branch_1: 0,
            branch_2: 2,
        },
    )
}

/// A recover of [`SOURCE`], offered once `when` holds.
pub fn recover(when: Option<Trigger>) -> ControlStep {
    step(SOURCE, ControlAction::Recover, when)
}

/// The node at A2 and the source at A8, the heaviest chain in the run,
/// dialing the node, with `controls`.
pub fn recovering(controls: Vec<ControlStep>) -> StorageScenario {
    syncing(2, 8, vec![], controls)
}

/// A recovery witness for each hostile prefix class: a close, a held
/// prepare, failed prepares, an invalidation and a held reply.
pub fn recovery_witnesses() -> Vec<(&'static str, StorageScenario)> {
    let mut holds = recovering(vec![recover(Some(Trigger::Submitted(Select::nth(
        OpKind::Prepare,
        1,
    ))))]);
    holds.policy.holds.push(hold(
        Select::nth(OpKind::Prepare, 1),
        Trigger::Released,
        Allow::Engine,
    ));
    // Every prepare fails until the recover, a minute in.
    let mut faults = recovering(vec![recover(Some(Trigger::VirtualTime(60_000)))]);
    faults.policy.holds.push(hold(
        Select {
            kind: Some(OpKind::Prepare),
            ..Select::default()
        },
        Trigger::VirtualTime(0),
        Allow::LocalResource,
    ));
    let invalidations = recovering(vec![
        reset(
            Reset::Invalidate { depth: 0 },
            Trigger::Completed(Select::nth(OpKind::Apply, 1)),
        ),
        recover(None),
    ]);
    let mut reply_holds = recovering(vec![recover(Some(Trigger::Committed(Select::nth(
        OpKind::Apply,
        1,
    ))))]);
    reply_holds.policy.reply_holds.push(ReplyHold {
        select: Select::nth(OpKind::Apply, 1),
        until: Trigger::VirtualTime(3_600_000),
    });
    vec![
        (
            "closes",
            recovering(vec![
                step(SOURCE, ControlAction::Close, None),
                recover(None),
            ]),
        ),
        ("holds", holds),
        ("faults", faults),
        ("invalidations", invalidations),
        ("reply holds", reply_holds),
    ]
}

/// hdr-13's recovery witnesses for the classes whose fuzz-12 witness owes
/// the node nothing at its `recovered` pick, so
/// `header-sync/bounded-adversary-progress` doesn't apply there. Each owes a
/// header past the node's baseline.
///
/// - **Reply holds.** fuzz-12's recovers once its held apply commits, which
///   leaves the baseline at the source's tip. Here the source grows by two
///   headers once the apply commits, and the recover comes once it has
///   grown, with the reply still held. The grown tip reaches the node only
///   behind the source's one-second status floor, so the node meets it
///   after the `recovered` pick.
/// - **Departure.** The close class as hdr-10's departure fixture
///   completes it ([`departure_recovery`]).
pub fn owed_recovery_witnesses() -> Vec<(&'static str, StorageScenario)> {
    let mut reply_holds = recovering(vec![
        step(
            SOURCE,
            ControlAction::Grow { len: 2 },
            Some(Trigger::Committed(Select::nth(OpKind::Apply, 1))),
        ),
        recover(Some(Trigger::Grown(SOURCE.to_owned()))),
    ]);
    reply_holds.policy.reply_holds.push(ReplyHold {
        select: Select::nth(OpKind::Apply, 1),
        until: Trigger::VirtualTime(3_600_000),
    });
    vec![
        ("reply holds", reply_holds),
        ("departure", departure_recovery()),
    ]
}

/// The node at A2 and the source at A8, which grows by two headers once the
/// node's first apply completes, with the recover once it has grown. The
/// grown tip reaches the node only behind the source's one-second status
/// floor, so at the `recovered` pick the node still owes it: hdr-13's
/// declared stall.
pub fn grown_source() -> StorageScenario {
    recovering(vec![
        step(
            SOURCE,
            ControlAction::Grow { len: 2 },
            Some(Trigger::Completed(Select::nth(OpKind::Apply, 1))),
        ),
        recover(Some(Trigger::Grown(SOURCE.to_owned()))),
    ])
}

/// hdr-13's close class as the departure's completion
/// ([`RecoveryFixture::Departure`]).
pub fn departure_recovery() -> StorageScenario {
    RecoveryFixture::Departure.storage()
}

/// [`StorageScenario::holds`], the scenario whose node releases a header
/// path, with its first release completing with `fault` once `after`
/// milliseconds pass.
pub fn release_fault(fault: Allow, after: u64) -> StorageScenario {
    let mut scenario = StorageScenario::holds();
    scenario.policy.holds.insert(
        0,
        hold(
            Select::nth(OpKind::Release, 1),
            Trigger::VirtualTime(after),
            fault,
        ),
    );
    scenario
}

/// Each fault witness: the kind, the fault, its outcome's name, and when
/// it's offered. A port fault is legal at once and a timeout once the
/// adapter's 30 s deadline passes. Prepare and apply have only
/// `local-resource`. Membership has none: no scenario probed submitted a
/// membership read, among the default, close-relink, grow, holds and
/// triggers scenarios, competing mirrors, and five layouts of a node
/// serving a caught-up mirror while syncing from another.
pub fn fault_witnesses() -> Vec<(OpKind, Allow, &'static str, u64)> {
    let mut faults = Vec::new();
    for kind in [
        OpKind::Locator,
        OpKind::Acquire,
        OpKind::Read,
        OpKind::Release,
    ] {
        faults.push((kind, Allow::Unavailable, "unavailable", 0));
        faults.push((kind, Allow::Timeout, "timeout", 30_000));
    }
    faults.push((OpKind::Prepare, Allow::LocalResource, "local-resource", 0));
    faults.push((OpKind::Apply, Allow::LocalResource, "local-resource", 0));
    faults
}

/// A fault witness's scenario: [`release_fault`] for a release, else
/// [`fault`].
pub fn fault_witness(kind: OpKind, allow: Allow, after: u64) -> StorageScenario {
    match kind {
        OpKind::Release => release_fault(allow, after),
        _ => fault(kind, allow, after),
    }
}

/// A fault witness seed's file stem.
pub fn fault_stem(kind: OpKind, outcome: &str) -> String {
    format!(
        "header-storage-w-fault-{}-{outcome}",
        format!("{kind:?}").to_lowercase()
    )
}

/// A witness's name and its scenario's constructor.
type Named = (&'static str, fn() -> StorageScenario);

/// Every witness seed's stem and scenario, as `tests/fixtures` holds them.
pub fn witness_seeds() -> Vec<(String, StorageScenario)> {
    let named: [Named; 15] = [
        ("hold-succeeds", StorageScenario::holds),
        ("reset-locator", reset_held_locator),
        ("reset-prepare", reset_held_prepare),
        ("reset-apply", reset_held_apply),
        ("reset-reply", reset_reply_hold),
        ("reconsider", invalidate_reconsider),
        ("release", release_after_apply),
        ("reply-hold", reply_hold),
        ("reply-after-shutdown", reply_after_shutdown),
        ("reply-after-close", reply_after_close),
        ("stop-storage", stop_with_held_apply),
        ("peer-lease-denied", peer_lease_denied),
        ("equal-forks", equal_forks),
        ("longer-fork", longer_fork),
        ("weighted-fork", weighted_fork),
    ];
    let mut seeds: Vec<(String, StorageScenario)> = named
        .into_iter()
        .map(|(name, scenario)| (format!("header-storage-w-{name}"), scenario()))
        .collect();
    for (kind, allow, outcome, after) in fault_witnesses() {
        seeds.push((fault_stem(kind, outcome), fault_witness(kind, allow, after)));
    }
    for (class, scenario) in recovery_witnesses() {
        seeds.push((
            format!("header-storage-w-recover-{}", class.replace(' ', "-")),
            scenario,
        ));
    }
    for (class, scenario) in owed_recovery_witnesses() {
        seeds.push((
            format!("header-storage-w-owed-{}", class.replace(' ', "-")),
            scenario,
        ));
    }
    seeds
}
