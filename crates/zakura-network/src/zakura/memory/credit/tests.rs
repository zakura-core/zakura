//! Tests drive [`CreditMachine`] directly: inputs in, effects out, no sink.

use std::collections::HashMap;

use proptest::prelude::*;

use super::*;

const MIB: u64 = 1024 * 1024;
const BASE_RECEIVE: u64 = 4 * MIB;
const BASE_SEND: u64 = 2 * MIB;
const PER_PEER_CAP: u64 = 16 * MIB;
const PEER_FLOOR: u64 = 2 * MIB;

/// A machine and the `set_limits` calls the test answered for it.
struct Harness {
    machine: CreditMachine,
    seq: u64,
}

impl Harness {
    fn new() -> Self {
        Self {
            machine: CreditMachine::new(BASE_RECEIVE, BASE_SEND),
            seq: 0,
        }
    }

    /// Steps `input`, answers each `SetLimits` with the next sequence number,
    /// and returns every effect.
    fn step(&mut self, input: CreditInput) -> Vec<CreditEffect> {
        let mut effects = self.machine.step(input);
        if effects
            .iter()
            .any(|effect| matches!(effect, CreditEffect::SetLimits(_)))
        {
            self.seq += 1;
            effects.extend(self.machine.step(CreditInput::LimitsSent { seq: self.seq }));
        }
        effects
    }

    fn finish(&mut self, total: u64, unspent: u64) -> CreditLeaseId {
        let id = self.machine.lease_id();
        self.step(CreditInput::FinishReceive {
            total,
            unspent: Some((id, unspent)),
        });
        id
    }

    fn tick(&mut self, load: CreditLoad) -> Vec<CreditEffect> {
        self.step(CreditInput::Tick {
            load,
            per_peer_cap: PER_PEER_CAP,
            peer_floor: PEER_FLOOR,
        })
    }
}

fn released(effects: &[CreditEffect]) -> Vec<CreditLeaseId> {
    effects
        .iter()
        .filter_map(|effect| match effect {
            CreditEffect::Release(id) => Some(*id),
            _ => None,
        })
        .collect()
}

fn snapshot(limits_applied: u64, receive_credit_excess: u64) -> CreditLoad {
    CreditLoad {
        limits_applied,
        receive_credit_excess,
        ..CreditLoad::default()
    }
}

#[test]
fn receive_credit_raises_the_window_and_drains_unspent_credit_against_the_excess() {
    let mut harness = Harness::new();
    let effects = harness.step(CreditInput::AddReceive { bytes: 2 * MIB });
    assert_eq!(
        effects,
        vec![CreditEffect::SetLimits(
            ConnLimits::default()
                .with_receive_window(BASE_RECEIVE + 2 * MIB)
                .with_send_window(BASE_SEND)
        )]
    );

    // RangeUnavailable: the window drops at once, the lease drains.
    let first = harness.finish(2 * MIB, 2 * MIB);
    assert_eq!(harness.machine.receive_window(), BASE_RECEIVE);
    let first_seq = harness.seq;

    harness.step(CreditInput::AddReceive { bytes: MIB });
    let second = harness.finish(MIB, MIB);

    // The peer may still spend all 3 MiB, then 2 MiB: only both drains
    // together cover that.
    assert!(released(&harness.step(CreditInput::Drain(snapshot(harness.seq, 3 * MIB)))).is_empty());
    assert!(released(&harness.step(CreditInput::Drain(snapshot(harness.seq, 2 * MIB)))).is_empty());
    assert!(released(&harness.step(CreditInput::Drain(snapshot(harness.seq, MIB + 1)))).is_empty());
    // 1 MiB remains: the later drain covers it, so the oldest releases.
    assert_eq!(
        released(&harness.step(CreditInput::Drain(snapshot(harness.seq, MIB)))),
        vec![first]
    );
    // A snapshot of the first shrink alone can't release the second.
    assert!(released(&harness.step(CreditInput::Drain(snapshot(first_seq, 0)))).is_empty());
    assert_eq!(
        released(&harness.step(CreditInput::Drain(snapshot(harness.seq, 0)))),
        vec![second]
    );
    assert!(!harness.machine.has_drains());
}

/// fix-11: a snapshot older than the shrink shows the excess before the
/// shrink, so it releases nothing.
#[test]
fn a_snapshot_from_before_the_shrink_releases_nothing() {
    let mut harness = Harness::new();
    harness.step(CreditInput::AddReceive { bytes: MIB });
    let before = harness.seq;
    let id = harness.finish(MIB, MIB);
    assert!(released(&harness.step(CreditInput::Drain(snapshot(before, 0)))).is_empty());
    assert_eq!(
        released(&harness.step(CreditInput::Drain(snapshot(harness.seq, 0)))),
        vec![id]
    );
}

#[test]
fn close_releases_every_lease_and_every_later_one() {
    let mut harness = Harness::new();
    harness.step(CreditInput::AddReceive { bytes: MIB });
    let id = harness.finish(MIB, MIB);
    harness.tick(CreditLoad {
        cwnd: 4 * MIB,
        queued_send_bytes: 1,
        ..CreditLoad::default()
    });
    let grown = harness.machine.lease_id();
    harness.step(CreditInput::SendGrown {
        id: grown,
        bytes: 6 * MIB,
    });

    let effects = harness.step(CreditInput::Closed);
    assert_eq!(released(&effects), vec![id]);
    assert!(effects.contains(&CreditEffect::ReleaseActiveSend));
    assert!(!harness.machine.has_drains());

    // After close, a handed lease releases at once and credit changes
    // nothing.
    assert!(harness
        .step(CreditInput::AddReceive { bytes: MIB })
        .is_empty());
    let late = harness.machine.lease_id();
    assert_eq!(
        harness.step(CreditInput::FinishReceive {
            total: MIB,
            unspent: Some((late, MIB)),
        }),
        vec![CreditEffect::Release(late)]
    );
}

#[test]
fn send_window_grows_to_two_cwnd_and_drains_after_an_accepted_byte() {
    let mut harness = Harness::new();
    let waiting = CreditLoad {
        cwnd: 5 * MIB,
        queued_send_bytes: 64 * 1024,
        send_blocked_streams: 1,
        ..CreditLoad::default()
    };
    let effects = harness.tick(waiting);
    // 2 × cwnd = 10 MiB within the 16 MiB cap: 8 MiB above the base.
    assert!(effects.contains(&CreditEffect::GrowSend(8 * MIB)));
    assert!(effects.contains(&CreditEffect::SetPeerCap(PER_PEER_CAP)));
    // A second tick before the answer asks for nothing more.
    assert!(!harness
        .tick(waiting)
        .iter()
        .any(|effect| matches!(effect, CreditEffect::GrowSend(_))));
    let grown = harness.machine.lease_id();
    let effects = harness.step(CreditInput::SendGrown {
        id: grown,
        bytes: 8 * MIB,
    });
    assert_eq!(effects[0], CreditEffect::MergeSend(grown));
    assert_eq!(harness.machine.send_window(), 10 * MIB);

    // Idle for SEND_IDLE_TICKS ticks: the window returns to the base.
    let idle = CreditLoad {
        cwnd: 5 * MIB,
        limits_applied: harness.seq,
        ..CreditLoad::default()
    };
    let mut drained = None;
    for _ in 0..SEND_IDLE_TICKS {
        for effect in harness.tick(idle) {
            if let CreditEffect::DrainSend { id, bytes } = effect {
                assert_eq!(bytes, 8 * MIB);
                drained = Some(id);
            }
        }
    }
    let drained = drained.expect("the idle window shrinks");
    assert_eq!(harness.machine.send_window(), BASE_SEND);

    // The snapshot that reflects the shrink sets its baseline; a byte
    // accepted after it releases the lease.
    let reflected = CreditLoad {
        limits_applied: harness.seq,
        ..idle
    };
    assert!(released(&harness.tick(reflected)).is_empty());
    assert_eq!(
        released(&harness.tick(CreditLoad {
            accepted_send_bytes: 1,
            ..reflected
        })),
        vec![drained]
    );
}

/// fix-13: output that drains between ticks doesn't shrink the window, while
/// bytes keep flowing or for fewer than [`SEND_IDLE_TICKS`] idle ticks.
#[test]
fn a_short_pause_keeps_the_send_window() {
    let mut harness = Harness::new();
    harness.tick(CreditLoad {
        cwnd: 4 * MIB,
        queued_send_bytes: 1,
        ..CreditLoad::default()
    });
    let grown = harness.machine.lease_id();
    harness.step(CreditInput::SendGrown {
        id: grown,
        bytes: 6 * MIB,
    });
    let window = harness.machine.send_window();

    // The queue reads empty while the connection keeps accepting bytes.
    for accepted in 1..=20 {
        harness.tick(CreditLoad {
            cwnd: 4 * MIB,
            accepted_send_bytes: accepted * MIB,
            ..CreditLoad::default()
        });
        assert_eq!(harness.machine.send_window(), window);
    }
    // A pause shorter than SEND_IDLE_TICKS keeps it too.
    for _ in 1..SEND_IDLE_TICKS {
        harness.tick(CreditLoad {
            cwnd: 4 * MIB,
            accepted_send_bytes: 20 * MIB,
            ..CreditLoad::default()
        });
        assert_eq!(harness.machine.send_window(), window);
    }
}

/// fix-14: an idle connection doesn't tick. It ticks while its send window is
/// above the base, watches snapshots while a lease drains, and sleeps again
/// once nothing is left.
#[test]
fn the_machine_ticks_only_while_it_has_credit_work() {
    let mut harness = Harness::new();
    let idle = CreditWake::Load { drains_seen: None };
    assert_eq!(harness.machine.wake(), idle);
    // A live request needs no snapshot.
    harness.step(CreditInput::AddReceive { bytes: MIB });
    assert_eq!(harness.machine.wake(), idle);
    let quiet = snapshot(harness.seq, 0);
    assert!(!idle.wakes_on(&quiet));
    assert!(idle.wakes_on(&CreditLoad {
        queued_send_bytes: 1,
        ..quiet
    }));

    // A drain watches for a snapshot that could release it.
    harness.finish(MIB, MIB);
    let drain_wake = harness.machine.wake();
    assert!(matches!(
        drain_wake,
        CreditWake::Load {
            drains_seen: Some(_)
        }
    ));
    assert!(drain_wake.wakes_on(&snapshot(harness.seq, 0)));
    harness.step(CreditInput::Drain(snapshot(harness.seq, 0)));
    assert_eq!(harness.machine.wake(), idle);

    // Waiting output grows the window, which ticks until it drains back.
    harness.tick(CreditLoad {
        cwnd: 4 * MIB,
        queued_send_bytes: 1,
        ..quiet
    });
    assert_eq!(harness.machine.wake(), CreditWake::Tick);
    let grown = harness.machine.lease_id();
    harness.step(CreditInput::SendGrown {
        id: grown,
        bytes: 6 * MIB,
    });
    let idle_load = CreditLoad {
        cwnd: 4 * MIB,
        ..snapshot(harness.seq, 0)
    };
    for _ in 0..SEND_IDLE_TICKS {
        assert_eq!(harness.machine.wake(), CreditWake::Tick);
        harness.tick(idle_load);
    }
    let shrunk = CreditLoad {
        limits_applied: harness.seq,
        ..idle_load
    };
    harness.tick(shrunk);
    let drain_wake = harness.machine.wake();
    assert!(matches!(
        drain_wake,
        CreditWake::Load {
            drains_seen: Some(_)
        }
    ));
    // Only an accepted byte releases the send drain.
    assert!(!drain_wake.wakes_on(&shrunk));
    let accepted = CreditLoad {
        accepted_send_bytes: 1,
        ..shrunk
    };
    assert!(drain_wake.wakes_on(&accepted));
    harness.tick(accepted);
    assert_eq!(harness.machine.wake(), idle);

    harness.step(CreditInput::Closed);
    assert_eq!(harness.machine.wake(), CreditWake::Never);
}

#[derive(Clone, Debug)]
enum Op {
    Add(u64),
    /// Finish the oldest live request, with this part unspent (per mille).
    Finish(u64),
    /// A snapshot `lag` set_limits calls old, with this excess.
    Drain {
        lag: u64,
        excess: u64,
    },
    Tick {
        lag: u64,
        excess: u64,
        cwnd: u64,
        queued: u64,
        accepted: u64,
    },
    /// Answer an outstanding growth request.
    Grow(bool),
    Close,
}

/// An excess that is often zero, so drains release.
fn excess() -> impl Strategy<Value = u64> {
    prop_oneof![Just(0), 0..8 * MIB]
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        3 => (1..4 * MIB).prop_map(Op::Add),
        3 => (0..=1000u64).prop_map(Op::Finish),
        3 => (0..3u64, excess()).prop_map(|(lag, excess)| Op::Drain { lag, excess }),
        3 => (0..3u64, excess(), 0..8 * MIB, 0..2u64, 0..2u64).prop_map(
            |(lag, excess, cwnd, queued, accepted)| Op::Tick { lag, excess, cwnd, queued, accepted }
        ),
        2 => any::<bool>().prop_map(Op::Grow),
        1 => Just(Op::Close),
    ]
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Kind {
    /// An unspent receive lease; `Some(seq)` once a `set_limits` call
    /// applied its shrink.
    Receive(Option<u64>),
    Growth,
    SendDrain,
}

/// The leases the test handed the machine, as a shell would hold them.
#[derive(Default)]
struct Ledger {
    held: HashMap<CreditLeaseId, (Kind, u64)>,
    active_send: u64,
    requests: Vec<u64>,
    growth_asked: bool,
    accepted: u64,
}

impl Ledger {
    /// Bytes of held receive drains that a snapshot of call `applied` reflects.
    fn reflected_receive(&self, applied: u64) -> u64 {
        self.held
            .values()
            .filter_map(|(kind, bytes)| match kind {
                Kind::Receive(Some(seq)) if *seq <= applied => Some(*bytes),
                _ => None,
            })
            .sum()
    }
}

proptest! {
    /// After every input: the machine releases only leases it holds, once; it
    /// releases a receive drain only against a snapshot that reflects its
    /// shrink, and keeps reflected drains that cover the snapshot's excess; its
    /// send window matches the lease it keeps active; it asks for one growth
    /// at a time; its last `SetLimits` is its current limits; and once closed
    /// it holds nothing. Its wake ticks while the send window is above its
    /// base, never ticks an idle connection, watches snapshots while leases
    /// drain, and never matches the snapshot it just read (no busy loop).
    #[test]
    fn credit_machine_invariants(ops in proptest::collection::vec(op(), 1..160)) {
        let mut harness = Harness::new();
        let mut ledger = Ledger::default();
        let mut output_waits = false;
        for op in ops {
            let mut load_seen = None;
            let input = match op {
                Op::Add(bytes) => {
                    ledger.requests.push(bytes);
                    CreditInput::AddReceive { bytes }
                }
                Op::Finish(per_mille) => {
                    if ledger.requests.is_empty() {
                        continue;
                    }
                    let total = ledger.requests.remove(0);
                    let unspent = total * per_mille / 1000;
                    let id = harness.machine.lease_id();
                    ledger.held.insert(id, (Kind::Receive(None), unspent));
                    CreditInput::FinishReceive { total, unspent: Some((id, unspent)) }
                }
                Op::Drain { lag, excess } => {
                    let load = snapshot(harness.seq.saturating_sub(lag), excess);
                    load_seen = Some(load);
                    CreditInput::Drain(load)
                }
                Op::Tick { lag, excess, cwnd, queued, accepted } => {
                    ledger.accepted += accepted;
                    let load = CreditLoad {
                        cwnd,
                        queued_send_bytes: queued,
                        accepted_send_bytes: ledger.accepted,
                        ..snapshot(harness.seq.saturating_sub(lag), excess)
                    };
                    load_seen = Some(load);
                    CreditInput::Tick { load, per_peer_cap: PER_PEER_CAP, peer_floor: PEER_FLOOR }
                }
                Op::Grow(granted) => {
                    if !ledger.growth_asked {
                        continue;
                    }
                    ledger.growth_asked = false;
                    if granted {
                        let id = harness.machine.lease_id();
                        ledger.held.insert(id, (Kind::Growth, MIB));
                        CreditInput::SendGrown { id, bytes: MIB }
                    } else {
                        CreditInput::SendRefused
                    }
                }
                Op::Close => CreditInput::Closed,
            };
            let closing = input == CreditInput::Closed;
            let seq_before = harness.seq;
            let effects = harness.step(input);

            let mut released_receive = false;
            let mut last_limits = None;
            for effect in &effects {
                match *effect {
                    CreditEffect::Release(id) => {
                        let held = ledger.held.remove(&id);
                        prop_assert!(held.is_some(), "released {id}, which it doesn't hold");
                        if let (Some((Kind::Receive(seq), _)), Some(load)) = (held, load_seen) {
                            released_receive = true;
                            prop_assert!(
                                seq.is_some_and(|seq| seq <= load.limits_applied),
                                "released {id} against a snapshot that misses its shrink"
                            );
                        }
                    }
                    CreditEffect::MergeSend(id) => {
                        let held = ledger.held.remove(&id);
                        prop_assert!(matches!(held, Some((Kind::Growth, _))));
                        ledger.active_send += held.map_or(0, |(_, bytes)| bytes);
                    }
                    CreditEffect::DrainSend { id, bytes } => {
                        prop_assert!(bytes > 0 && bytes <= ledger.active_send);
                        ledger.active_send -= bytes;
                        ledger.held.insert(id, (Kind::SendDrain, bytes));
                    }
                    CreditEffect::ReleaseActiveSend => {
                        prop_assert!(ledger.active_send > 0);
                        ledger.active_send = 0;
                    }
                    CreditEffect::GrowSend(bytes) => {
                        prop_assert!(bytes > 0 && !ledger.growth_asked);
                        ledger.growth_asked = true;
                    }
                    CreditEffect::SetLimits(limits) => last_limits = Some(limits),
                    CreditEffect::SetPeerCap(cap) => prop_assert!(cap <= PER_PEER_CAP),
                }
            }
            // The step's set_limits call applied every shrink it made.
            if harness.seq > seq_before {
                for (kind, _) in ledger.held.values_mut() {
                    if *kind == Kind::Receive(None) {
                        *kind = Kind::Receive(Some(harness.seq));
                    }
                }
            }
            if released_receive {
                let load = load_seen.expect("only a snapshot releases a drain before close");
                prop_assert!(ledger.reflected_receive(load.limits_applied) >= load.receive_credit_excess);
            }
            if let Some(limits) = last_limits {
                prop_assert_eq!(limits, harness.machine.limits());
            }
            prop_assert_eq!(harness.machine.send_window(), BASE_SEND + ledger.active_send);
            if let Some(load) = load_seen {
                output_waits = load.queued_send_bytes > 0 || load.send_blocked_streams > 0;
            }
            let wake = harness.machine.wake();
            let drains = ledger.held.values().any(|(kind, _)| *kind != Kind::Growth);
            if harness.machine.has_drains() || ledger.active_send > 0 || output_waits {
                prop_assert_ne!(wake, CreditWake::Load { drains_seen: None });
            }
            if ledger.active_send > 0 && wake != CreditWake::Never {
                prop_assert_eq!(wake, CreditWake::Tick);
            }
            if !drains && ledger.active_send == 0 && !output_waits {
                prop_assert_ne!(wake, CreditWake::Tick, "an idle connection ticks");
            }
            if let (Some(load), CreditWake::Load { .. }) = (load_seen, wake) {
                prop_assert!(!wake.wakes_on(&load), "the wake matches the snapshot it read");
            }
            if closing {
                prop_assert_eq!(wake, CreditWake::Never);
                prop_assert!(ledger.held.values().all(|(kind, _)| *kind == Kind::Growth));
                prop_assert_eq!(ledger.active_send, 0);
                prop_assert!(!harness.machine.has_drains());
            }
        }
    }
}
